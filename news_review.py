"""Durable approval workflow for the daily controversial-issues section.

This module intentionally contains no model or WeChat integration.  Callers
must provide already verified source material and an authenticated identity.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Callable


SECTION_HEADING = "## 境外报道与争议议题"
ACTIVE_STATUSES = ("previewed", "publishing", "publish_failed")
TERMINAL_STATUSES = ("published", "cancelled", "superseded", "expired")


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    return current if current.tzinfo else current.replace(tzinfo=timezone.utc)


def identity_hash(value: str) -> str:
    if not value:
        raise ValueError("identity is required")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def body_hash(body: str) -> str:
    return hashlib.sha256(body.strip().encode("utf-8")).hexdigest()


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS news_review_drafts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            public_id TEXT UNIQUE,
            news_date TEXT NOT NULL,
            generation_date TEXT NOT NULL,
            version INTEGER NOT NULL,
            body TEXT NOT NULL,
            body_hash TEXT NOT NULL,
            sources_json TEXT NOT NULL,
            recipient_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            previewed_at TEXT,
            expires_at TEXT NOT NULL,
            publish_key TEXT NOT NULL UNIQUE,
            publication_json TEXT,
            last_error TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(news_date, version)
        );
        CREATE INDEX IF NOT EXISTS idx_news_review_pending
          ON news_review_drafts(recipient_hash,status,expires_at);
        """
    )


def _validate_date(value: str) -> None:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("invalid news date")
    datetime.strptime(value, "%Y-%m-%d")


def _validate_body(body: str) -> str:
    cleaned = body.strip()
    if not cleaned.startswith(SECTION_HEADING):
        raise ValueError("section heading is missing")
    if cleaned.startswith("---") or "<!-- news-review:" in cleaned:
        raise ValueError("reserved markdown is not allowed")
    if len(cleaned) > 20_000:
        raise ValueError("section is too long")
    return cleaned


def _validate_sources(sources: list[dict[str, Any]]) -> str:
    if not sources:
        raise ValueError("at least one source is required")
    normalized = []
    for source in sources:
        url = str(source.get("url", "")).strip()
        published = str(source.get("published", "")).strip()
        if not re.fullmatch(r"https://[^\s]+", url) or not published:
            raise ValueError("each source needs an HTTPS URL and publication date")
        normalized.append({
            "name": str(source.get("name", "")).strip()[:120],
            "url": url[:2000],
            "published": published[:80],
            "verification": str(source.get("verification", "single-source")).strip()[:80],
        })
    return json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))


def create_previewed_draft(
    conn: sqlite3.Connection,
    *,
    news_date: str,
    generation_date: str,
    body: str,
    sources: list[dict[str, Any]],
    recipient: str,
    now: datetime | None = None,
    ttl_hours: int = 48,
    preview_delivered: bool = False,
) -> dict[str, Any]:
    """Persist the exact previewed version and supersede older active versions."""
    _validate_date(news_date)
    _validate_date(generation_date)
    cleaned = _validate_body(body)
    sources_json = _validate_sources(sources)
    current = _now(now)
    recipient_digest = identity_hash(recipient)
    row = conn.execute(
        "SELECT COALESCE(MAX(version),0)+1 FROM news_review_drafts WHERE news_date=?",
        (news_date,),
    ).fetchone()
    version = int(row[0])
    digest = body_hash(cleaned)
    public_id = f"{news_date.replace('-', '')}-v{version}-{digest[:8]}"
    publish_key = f"daily-news-review:{news_date}:v{version}:{digest[:16]}"
    stamp = current.isoformat()
    expires = (current + timedelta(hours=ttl_hours)).isoformat()
    conn.execute(
        """UPDATE news_review_drafts SET status='superseded',updated_at=?
           WHERE news_date=? AND status IN ('previewed','publish_failed')""",
        (stamp, news_date),
    )
    conn.execute(
        """INSERT INTO news_review_drafts(
          public_id,news_date,generation_date,version,body,body_hash,sources_json,
          recipient_hash,status,created_at,previewed_at,expires_at,publish_key,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,'previewed',?,?,?,?,?)""",
        (public_id, news_date, generation_date, version, cleaned, digest, sources_json,
         recipient_digest, stamp, stamp if preview_delivered else None, expires, publish_key, stamp),
    )
    conn.commit()
    return {"public_id": public_id, "version": version, "body_hash": digest, "body": cleaned,
            "expires_at": expires, "status": "previewed"}


def _expire(conn: sqlite3.Connection, current: datetime) -> None:
    conn.execute(
        """UPDATE news_review_drafts SET status='expired',updated_at=?
           WHERE status IN ('previewed','publish_failed') AND expires_at<=?""",
        (current.isoformat(), current.isoformat()),
    )


def _resolve(conn: sqlite3.Connection, recipient: str, draft_id: str | None,
             current: datetime) -> sqlite3.Row:
    _expire(conn, current)
    digest = identity_hash(recipient)
    if draft_id:
        row = conn.execute(
            "SELECT * FROM news_review_drafts WHERE public_id=?", (draft_id,)
        ).fetchone()
        if not row or row["recipient_hash"] != digest:
            raise PermissionError("draft is not available for this recipient")
        return row
    rows = conn.execute(
        """SELECT * FROM news_review_drafts WHERE recipient_hash=?
           AND status IN ('previewed','publish_failed') ORDER BY created_at DESC""",
        (digest,),
    ).fetchall()
    if not rows:
        raise LookupError("no pending draft")
    if len(rows) != 1:
        raise ValueError("multiple pending drafts; specify a draft id")
    return rows[0]


def approve(
    conn: sqlite3.Connection,
    *,
    recipient: str,
    publisher: Callable[[str, dict[str, Any]], dict[str, Any]],
    draft_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Publish the exact previewed bytes, with a stable key across retries."""
    current = _now(now)
    row = _resolve(conn, recipient, draft_id, current)
    if not row["previewed_at"]:
        raise ValueError("draft preview has not been delivered")
    if row["status"] == "published":
        return {"status": "published", "duplicate": True, "public_id": row["public_id"]}
    if row["status"] not in ("previewed", "publish_failed"):
        raise ValueError(f"draft cannot be approved from {row['status']}")
    conn.execute(
        "UPDATE news_review_drafts SET status='publishing',last_error=NULL,updated_at=? WHERE id=?",
        (current.isoformat(), row["id"]),
    )
    conn.commit()
    payload = {
        "idempotencyKey": row["publish_key"],
        "slug": "daily-news-" + row["news_date"].replace("-", ""),
        "date": row["news_date"],
        "sectionHash": row["body_hash"],
        "body": row["body"],
    }
    try:
        result = publisher("daily-news-sections", payload)
        if not result.get("accepted"):
            raise RuntimeError(str(result.get("error", "publication rejected")))
    except Exception as exc:
        conn.execute(
            "UPDATE news_review_drafts SET status='publish_failed',last_error=?,updated_at=? WHERE id=?",
            (str(exc)[:500], _now().isoformat(), row["id"]),
        )
        conn.commit()
        return {"status": "publish_failed", "public_id": row["public_id"]}
    conn.execute(
        """UPDATE news_review_drafts SET status='published',publication_json=?,
           last_error=NULL,updated_at=? WHERE id=?""",
        (json.dumps(result, ensure_ascii=False), _now().isoformat(), row["id"]),
    )
    conn.commit()
    return {"status": "published", "duplicate": bool(result.get("duplicate")),
            "public_id": row["public_id"]}


def cancel(conn: sqlite3.Connection, *, recipient: str, draft_id: str | None = None,
           now: datetime | None = None) -> dict[str, Any]:
    row = _resolve(conn, recipient, draft_id, _now(now))
    if row["status"] not in ("previewed", "publish_failed"):
        raise ValueError("draft cannot be cancelled")
    conn.execute("UPDATE news_review_drafts SET status='cancelled',updated_at=? WHERE id=?",
                 (_now(now).isoformat(), row["id"]))
    conn.commit()
    return {"status": "cancelled", "public_id": row["public_id"]}


def replace_and_preview(
    conn: sqlite3.Connection, *, recipient: str, old: str, new: str,
    draft_id: str | None = None, now: datetime | None = None,
) -> dict[str, Any]:
    current = _now(now)
    row = _resolve(conn, recipient, draft_id, current)
    if row["status"] not in ("previewed", "publish_failed"):
        raise ValueError("draft cannot be modified")
    if not old or old not in row["body"] or not new:
        raise ValueError("replacement text does not match exactly")
    updated = row["body"].replace(old, new, 1)
    sources = json.loads(row["sources_json"])
    return create_previewed_draft(
        conn, news_date=row["news_date"], generation_date=current.date().isoformat(),
        body=updated, sources=sources, recipient=recipient, now=current,
    )


def pending_summary(conn: sqlite3.Connection, *, recipient: str,
                    now: datetime | None = None) -> list[dict[str, Any]]:
    _expire(conn, _now(now))
    rows = conn.execute(
        """SELECT public_id,news_date,version,status,expires_at,body_hash
           FROM news_review_drafts WHERE recipient_hash=?
           AND status IN ('previewed','publish_failed') ORDER BY created_at DESC""",
        (identity_hash(recipient),),
    ).fetchall()
    conn.commit()
    return [dict(row) for row in rows]
