#!/usr/bin/env python3
"""Persistent WeChat delivery queue and scheduled content service.

The service has three responsibilities:

* accept complete message modules through a small authenticated HTTP API;
* deliver them serially to WeChat with stable client IDs and durable retries;
* generate the daily briefing, interview exercise, and verified hot-topic digest.

It deliberately keeps message generation and delivery separate. A model or
source outage cannot lose an already accepted WeChat message.
"""

from __future__ import annotations

import asyncio
import email.utils
import hashlib
import html
import json
import os
import random
import re
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DB_PATH = os.environ.get("DELIVERY_DB_PATH", "/data/delivery.db")
LISTEN_HOST = os.environ.get("DELIVERY_LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("DELIVERY_LISTEN_PORT", "8787"))
QUEUE_TOKEN = os.environ.get("DELIVERY_QUEUE_TOKEN", "")
WEIXIN_RECEIVER = os.environ.get("WEIXIN_RECEIVER", "")
WEIXIN_TOKEN = os.environ.get("WEIXIN_TOKEN", "")
HERMES_HOME = os.environ.get("HERMES_HOME", "/home/yinsheng/.hermes")
CLIPROXY_KEY = os.environ.get("CLIPROXY_API_KEY", "")
CLIPROXY_BASE_URL = os.environ.get(
    "CLIPROXY_BASE_URL", "http://127.0.0.1:8317/v1"
).rstrip("/")
BLOG_PUBLICATION_BASE_URL = os.environ.get("BLOG_PUBLICATION_BASE_URL", "").rstrip("/")
BLOG_PUBLICATION_TOKEN = os.environ.get("BLOG_PUBLICATION_TOKEN", "")
PRIMARY_MODEL = os.environ.get("PRIMARY_MODEL", "gemini-3.7-flash-high")
FALLBACK_MODEL = os.environ.get("FALLBACK_MODEL", "gpt-5.6-luna")
MODEL_TIMEOUT_SECONDS = int(os.environ.get("MODEL_TIMEOUT_SECONDS", "60"))
MESSAGE_LIMIT = min(2000, int(os.environ.get("WEIXIN_MESSAGE_LIMIT", "1900")))
MESSAGE_GAP_SECONDS = float(os.environ.get("WEIXIN_MESSAGE_GAP_SECONDS", "10"))
try:
    TZ = ZoneInfo(os.environ.get("TZ", "Asia/Shanghai"))
except ZoneInfoNotFoundError:
    # Minimal Windows/Python installations may not ship the IANA database.
    # The production Linux image does; this keeps local tests deterministic.
    TZ = timezone(timedelta(hours=8), name="Asia/Shanghai")
HEARTBEAT_PATH = os.environ.get(
    "DELIVERY_HEARTBEAT_PATH", "/tmp/delivery_service_heartbeat"
)

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; HermesContentService/1.0)",
    "Accept": "application/rss+xml, application/xml, application/json, text/html;q=0.8",
}

DAILY_FEEDS = {
    "新华社": ("国内", "https://www.news.cn/politics/news_politics.xml"),
    "Google 新闻中文": (
        "国内/综合",
        "https://news.google.com/rss?hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
    ),
    "BBC 中文": ("国际", "https://feeds.bbci.co.uk/zhongwen/simp/rss.xml"),
    "BBC World": ("国际", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    "The Guardian": ("国际", "https://www.theguardian.com/world/rss"),
    "Google News US": (
        "国际/财经",
        "https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en",
    ),
    "36Kr": ("财经/科技", "https://rsshub.rssforever.com/36kr/newsflashes"),
    "Solidot": ("科技/开源", "https://rsshub.rssforever.com/solidot"),
    "NASA": ("科学/航天", "https://www.nasa.gov/news-release/feed/"),
    "Ars Technica": ("科技/安全", "https://feeds.arstechnica.com/arstechnica/index"),
    "ESPN": ("体育", "https://www.espn.com/espn/rss/news"),
}

TREND_GEOS = ("US", "JP", "KR", "SG", "GB", "IN")
AI_APP_REPOS = (
    "langgenius/dify",
    "infiniflow/ragflow",
    "open-webui/open-webui",
    "lobehub/lobe-chat",
    "All-Hands-AI/OpenHands",
    "Aider-AI/aider",
    "Mintplex-Labs/anything-llm",
    "FlowiseAI/Flowise",
    "microsoft/autogen",
    "crewAIInc/crewAI",
    "continuedev/continue",
    "QuivrHQ/quivr",
)

TOPICS = (
    "Java 基础与核心机制",
    "JVM 原理与性能调优",
    "Java 并发编程",
    "Spring 框架与生态",
    "MySQL 与关系型数据库",
    "Redis 与高性能缓存",
    "消息队列与异步架构",
    "分布式理论与架构",
    "系统设计与高并发架构",
    "Linux / DevOps / Kubernetes",
    "软件工程能力与架构演进",
    "AI 工程与大模型应用开发",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


@contextmanager
def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with db_connect() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                idempotency_key TEXT NOT NULL UNIQUE,
                category TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 50,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT,
                last_error TEXT
            );
            CREATE TABLE IF NOT EXISTS delivery_chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                delivery_id INTEGER NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
                seq INTEGER NOT NULL,
                body TEXT NOT NULL,
                client_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at TEXT NOT NULL,
                sent_at TEXT,
                last_error TEXT,
                UNIQUE(delivery_id, seq)
            );
            CREATE INDEX IF NOT EXISTS idx_chunks_due
                ON delivery_chunks(status, available_at, delivery_id, seq);
            CREATE TABLE IF NOT EXISTS job_runs (
                job_key TEXT NOT NULL,
                run_date TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                started_at TEXT,
                finished_at TEXT,
                last_error TEXT,
                PRIMARY KEY(job_key, run_date)
            );
            CREATE TABLE IF NOT EXISTS trend_pushes (
                fingerprint TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                pushed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS system_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        # A process restart must make an interrupted send retryable. The stable
        # client_id keeps that retry idempotent at the iLink side.
        conn.execute(
            "UPDATE delivery_chunks SET status='pending' WHERE status='sending'"
        )
        conn.execute(
            """
            UPDATE job_runs
            SET status='failed',finished_at=?,last_error='service restarted during job execution'
            WHERE status='running'
            """,
            (iso_now(),),
        )


def _state_get(key: str, default: str = "unknown") -> str:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT value FROM system_state WHERE key=?", (key,)
        ).fetchone()
    return str(row[0]) if row else default


def _state_set(key: str, value: str) -> None:
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO system_state(key,value,updated_at) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (key, value, iso_now()),
        )


def _split_hard(text: str, limit: int | None = None) -> list[str]:
    """Final safety valve for an unbreakable token or malformed model output."""
    chunk_size = limit or MESSAGE_LIMIT
    return [text[index : index + chunk_size] for index in range(0, len(text), chunk_size)]


def _split_code_block(block: str) -> list[str]:
    """Split a long fenced block into independently valid fenced snippets."""
    if len(block) <= MESSAGE_LIMIT:
        return [block]

    lines = block.splitlines()
    if len(lines) < 2 or not lines[0].lstrip().startswith("```") or not lines[-1].strip().startswith("```"):
        return _split_hard(block)

    opening = lines[0].strip()
    closing = "```"
    budget = MESSAGE_LIMIT - len(opening) - len(closing) - 2
    if budget <= 0:
        return _split_hard(block)

    body_chunks: list[str] = []
    current = ""
    for line in lines[1:-1]:
        pieces = _split_hard(line, budget) if len(line) > budget else [line]
        for piece in pieces:
            candidate = f"{current}\n{piece}" if current else piece
            if len(candidate) <= budget:
                current = candidate
            else:
                body_chunks.append(current)
                current = piece
    if current:
        body_chunks.append(current)

    return [f"{opening}\n{body}\n{closing}" for body in body_chunks] or [f"{opening}\n{closing}"]


def _split_prose(text: str) -> list[str]:
    """Split prose at paragraphs/sentences, then hard-split only as a last resort."""
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
    units: list[str] = []
    for paragraph in paragraphs:
        if len(paragraph) <= MESSAGE_LIMIT:
            units.append(paragraph)
            continue

        sentences = [
            part.strip()
            for part in re.split(r"(?<=[。！？.!?；;])\s*|\n", paragraph)
            if part.strip()
        ]
        if not sentences:
            sentences = [paragraph]
        for sentence in sentences:
            units.extend(_split_hard(sentence) if len(sentence) > MESSAGE_LIMIT else [sentence])
    return units


def _markdown_units(text: str) -> list[tuple[str, str]]:
    """Return prose and complete fenced-code units without cutting fences."""
    lines = text.splitlines(keepends=True)
    units: list[tuple[str, str]] = []
    prose: list[str] = []
    code: list[str] | None = None

    def flush_prose() -> None:
        if prose:
            units.append(("prose", "".join(prose)))
            prose.clear()

    for line in lines:
        if code is None and line.lstrip().startswith("```"):
            flush_prose()
            code = [line]
            continue
        if code is not None:
            code.append(line)
            if len(code) > 1 and line.strip().startswith("```"):
                units.append(("code", "".join(code).strip()))
                code = None
            continue
        prose.append(line)

    if code is not None:
        # A malformed fence is treated as prose so it can never block delivery.
        prose.extend(code)
    flush_prose()
    return units


def _split_oversized_text(text: str) -> list[str]:
    """Split text safely while keeping prose paragraphs and fenced code valid."""
    if len(text) <= MESSAGE_LIMIT:
        return [text]

    units: list[str] = []
    for kind, value in _markdown_units(text):
        if kind == "code":
            units.extend(_split_code_block(value))
        else:
            units.extend(_split_prose(value))

    result: list[str] = []
    current = ""
    for unit in units:
        if len(unit) > MESSAGE_LIMIT:
            if current:
                result.append(current)
                current = ""
            result.extend(_split_hard(unit))
            continue
        candidate = f"{current}\n\n{unit}" if current else unit
        if len(candidate) <= MESSAGE_LIMIT:
            current = candidate
        else:
            if current:
                result.append(current)
            current = unit
    if current:
        result.append(current)
    return result or _split_hard(text)


def semantic_split(content: str) -> list[str]:
    """Group paragraphs without cutting a paragraph, item, or code block."""
    content = content.strip()
    if not content:
        raise ValueError("message content is empty")
    marked = [
        part.strip()
        for part in re.split(r"(?m)^\s*\[\[MESSAGE\]\]\s*$", content)
        if part.strip()
    ]
    if len(marked) > 1:
        messages: list[str] = []
        for part in marked:
            messages.extend(_split_oversized_text(part))
        return messages

    atoms = [p.strip() for p in re.split(r"\n\s*\n", content) if p.strip()]
    messages = []
    current = ""
    for atom in atoms:
        atom_parts = _split_oversized_text(atom)
        for atom_part in atom_parts:
            candidate = f"{current}\n\n{atom_part}" if current else atom_part
            if len(candidate) <= MESSAGE_LIMIT:
                current = candidate
            else:
                if current:
                    messages.append(current)
                current = atom_part
    if current:
        messages.append(current)
    return messages


def enqueue_delivery(
    idempotency_key: str,
    category: str,
    messages: list[str],
    priority: int = 50,
) -> dict[str, Any]:
    key = idempotency_key.strip()
    if not key or len(key) > 160 or not re.fullmatch(r"[A-Za-z0-9_.:@/-]+", key):
        raise ValueError("invalid idempotency_key")
    clean_messages: list[str] = []
    for message in messages:
        clean_messages.extend(semantic_split(str(message)))
    if not clean_messages:
        raise ValueError("no messages supplied")
    if len(clean_messages) > 20:
        raise ValueError("too many message chunks")
    now = iso_now()
    with db_connect() as conn:
        existing = conn.execute(
            "SELECT id,status FROM deliveries WHERE idempotency_key=?", (key,)
        ).fetchone()
        if existing:
            return {
                "accepted": True,
                "duplicate": True,
                "delivery_id": existing["id"],
                "status": existing["status"],
            }
        cur = conn.execute(
            """
            INSERT INTO deliveries(idempotency_key,category,priority,status,created_at,updated_at)
            VALUES(?,?,?,'pending',?,?)
            """,
            (key, category[:80] or "general", max(0, min(100, priority)), now, now),
        )
        delivery_id = int(cur.lastrowid)
        for seq, body in enumerate(clean_messages):
            digest = hashlib.sha256(f"{key}:{seq}".encode()).hexdigest()[:40]
            conn.execute(
                """
                INSERT INTO delivery_chunks(
                    delivery_id,seq,body,client_id,status,attempts,available_at
                ) VALUES(?,?,?,?,'pending',0,?)
                """,
                (delivery_id, seq, body, f"hermes-q-{digest}", now),
            )
    return {
        "accepted": True,
        "duplicate": False,
        "delivery_id": delivery_id,
        "chunks": len(clean_messages),
    }


def submit_blog_publication(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Persist a publish command before delivering the matching WeChat content."""
    if not BLOG_PUBLICATION_BASE_URL or not BLOG_PUBLICATION_TOKEN:
        return {"accepted": False, "error": "blog publication is not configured"}
    request = urllib.request.Request(
        f"{BLOG_PUBLICATION_BASE_URL}/{path.lstrip('/')}",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {BLOG_PUBLICATION_TOKEN}",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            result = json.loads(response.read().decode("utf-8"))
            if response.status in (200, 202) and (result.get("id") or result.get("duplicate")):
                return {"accepted": True, **result}
            return {"accepted": False, "error": str(result)}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        return {"accepted": False, "error": f"blog publication HTTP {exc.code}: {detail}"}
    except Exception as exc:
        return {"accepted": False, "error": f"blog publication error: {exc}"}


def publish_scheduled_post(
    *,
    idempotency_key: str,
    slug: str,
    title: str,
    now: datetime,
    description: str,
    categories: list[str],
    tags: list[str],
    messages: list[str],
) -> dict[str, Any]:
    cleaned_messages = []
    for msg in messages:
        # Strip internal [[MESSAGE]] delimiters if present
        clean_msg = re.sub(r"\[\[MESSAGE\]\]\s*", "", msg).strip()
        if clean_msg:
            cleaned_messages.append(clean_msg)
    body = "\n\n".join(cleaned_messages).strip()
    result = submit_blog_publication("posts", {
        "idempotencyKey": idempotency_key,
        "slug": slug,
        "title": title,
        "date": now.strftime("%Y-%m-%d"),
        "description": description[:280],
        "categories": categories,
        "tags": tags,
        "body": body,
    })
    if not result.get("accepted"):
        raise RuntimeError(result.get("error", "blog publication was rejected"))
    return result


def _get_active_context_token(receiver: str) -> str | None:
    # 1. Try ContextTokenStore standard load
    try:
        from gateway.platforms.weixin import ContextTokenStore
        store = ContextTokenStore(HERMES_HOME)
        for acc in ["DEFAULT", "419076e9d700@im.bot", "default"]:
            store.restore(acc)
            tok = store.get(acc, receiver)
            if tok:
                return tok
    except Exception:
        pass
    # 2. Search direct JSON token files in ~/.hermes/weixin/accounts/
    accounts_dir = Path(HERMES_HOME) / "weixin" / "accounts"
    if accounts_dir.is_dir():
        for tf in accounts_dir.glob("*.context-tokens.json"):
            try:
                data = json.loads(tf.read_text(encoding="utf-8"))
                if receiver in data:
                    return data[receiver]
            except Exception:
                pass
    return None


def _load_weixin_token() -> str:
    if WEIXIN_TOKEN:
        return WEIXIN_TOKEN
    env_path = Path(HERMES_HOME) / ".env"
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith("WEIXIN_TOKEN="):
                return line.split("=", 1)[1].strip().strip("\"'")
    except OSError:
        pass
    return ""


async def _send_weixin_raw(body: str, client_id: str) -> tuple[bool, str]:
    """Send one complete module, preserving client_id across every retry."""
    sys.path.insert(0, "/opt/hermes-src")
    try:
        import aiohttp
        from gateway.platforms.weixin import (
            ContextTokenStore,
            ILINK_BASE_URL,
            _make_ssl_connector,
            _send_message,
        )
    except Exception as exc:
        return False, f"permanent:weixin import failed: {exc}"

    token = _load_weixin_token()
    if not token or not WEIXIN_RECEIVER:
        return False, "permanent:WeChat credentials are incomplete"
    account_id = os.environ.get("WEIXIN_ACCOUNT_ID", "DEFAULT")
    base_url = os.environ.get("WEIXIN_BASE_URL", ILINK_BASE_URL).rstrip("/")
    context_token = _get_active_context_token(WEIXIN_RECEIVER)

    try:
        async with aiohttp.ClientSession(
            trust_env=True, connector=_make_ssl_connector()
        ) as session:
            contexts = [context_token, None] if context_token else [None]
            for index, context in enumerate(contexts):
                response = await _send_message(
                    session,
                    base_url=base_url,
                    token=token,
                    to=WEIXIN_RECEIVER,
                    text=body,
                    context_token=context,
                    client_id=client_id,
                )
                ret = response.get("ret") if isinstance(response, dict) else None
                errcode = response.get("errcode") if isinstance(response, dict) else None
                if ret in (None, 0) and errcode in (None, 0):
                    return True, "sent"
                if (ret in (-2, -14) or errcode in (-2, -14)) and index == 0 and context:
                    # -2 is overloaded by iLink. Retrying tokenless once
                    # distinguishes a stale context token from real throttling.
                    continue
                message = ""
                if isinstance(response, dict):
                    message = str(response.get("errmsg") or response.get("msg") or "")
                if ret == -2 or errcode == -2:
                    return False, f"rate_limit:ret={ret},errcode={errcode}"
                if ret == -14 or errcode == -14:
                    return False, f"session:ret={ret},errcode={errcode}"
                return False, f"api:ret={ret},errcode={errcode},message={message[:120]}"
    except (asyncio.TimeoutError, TimeoutError) as exc:
        return False, f"network:timeout:{exc}"
    except Exception as exc:
        lowered = str(exc).lower()
        kind = "rate_limit" if "rate limit" in lowered else "network"
        return False, f"{kind}:{exc}"


def _retry_delay(error: str, attempts: int) -> float:
    jitter = random.uniform(0, 15)
    if error.startswith("rate_limit"):
        schedule = (75, 180, 600, 1800, 3600)
    elif error.startswith("permanent"):
        schedule = (1800, 3600, 10800, 21600)
    else:
        schedule = (30, 60, 180, 600, 1800, 3600)
    return schedule[min(max(attempts - 1, 0), len(schedule) - 1)] + jitter


def _next_due_chunk() -> sqlite3.Row | None:
    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT c.*, d.priority, d.idempotency_key
            FROM delivery_chunks c
            JOIN deliveries d ON d.id=c.delivery_id
            WHERE c.status='pending' AND c.available_at<=?
              AND NOT EXISTS (
                SELECT 1 FROM delivery_chunks previous
                WHERE previous.delivery_id=c.delivery_id
                  AND previous.seq<c.seq AND previous.status!='sent'
              )
            ORDER BY d.priority DESC, d.created_at ASC, c.seq ASC
            LIMIT 1
            """,
            (iso_now(),),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE delivery_chunks SET status='sending' WHERE id=?", (row["id"],)
            )
            conn.execute(
                "UPDATE deliveries SET status='sending',updated_at=? WHERE id=?",
                (iso_now(), row["delivery_id"]),
            )
        return row


def _finish_chunk(row: sqlite3.Row, ok: bool, error: str) -> None:
    now = utc_now()
    attempts = int(row["attempts"]) + 1
    with db_connect() as conn:
        if ok:
            conn.execute(
                """
                UPDATE delivery_chunks
                SET status='sent',attempts=?,sent_at=?,last_error=NULL
                WHERE id=?
                """,
                (attempts, now.isoformat(), row["id"]),
            )
            remaining = conn.execute(
                "SELECT COUNT(*) FROM delivery_chunks WHERE delivery_id=? AND status!='sent'",
                (row["delivery_id"],),
            ).fetchone()[0]
            if remaining == 0:
                conn.execute(
                    """
                    UPDATE deliveries SET status='sent',updated_at=?,completed_at=?,last_error=NULL
                    WHERE id=?
                    """,
                    (now.isoformat(), now.isoformat(), row["delivery_id"]),
                )
            else:
                available = (now + timedelta(seconds=MESSAGE_GAP_SECONDS)).isoformat()
                conn.execute(
                    """
                    UPDATE delivery_chunks SET available_at=?
                    WHERE delivery_id=? AND status='pending'
                    """,
                    (available, row["delivery_id"]),
                )
                conn.execute(
                    "UPDATE deliveries SET status='pending',updated_at=? WHERE id=?",
                    (now.isoformat(), row["delivery_id"]),
                )
        else:
            available = (now + timedelta(seconds=_retry_delay(error, attempts))).isoformat()
            conn.execute(
                """
                UPDATE delivery_chunks
                SET status='pending',attempts=?,available_at=?,last_error=?
                WHERE id=?
                """,
                (attempts, available, error[:500], row["id"]),
            )
            conn.execute(
                "UPDATE deliveries SET status='pending',updated_at=?,last_error=? WHERE id=?",
                (now.isoformat(), error[:500], row["delivery_id"]),
            )


def sender_loop() -> None:
    while True:
        try:
            row = _next_due_chunk()
            if not row:
                time.sleep(2)
                continue
            ok, detail = asyncio.run(_send_weixin_raw(row["body"], row["client_id"]))
            _finish_chunk(row, ok, detail)
            state = "sent" if ok else f"retry scheduled ({detail.split(':', 1)[0]})"
            print(
                f"[delivery] id={row['delivery_id']} chunk={row['seq']} {state}",
                flush=True,
            )
        except Exception as exc:
            print(f"[delivery] sender loop error: {exc}", flush=True)
            time.sleep(5)


def _http_get(url: str, timeout: int = 15) -> bytes:
    request = urllib.request.Request(url, headers=HTTP_HEADERS)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read(1_500_000)


def _strip_html(value: str) -> str:
    value = re.sub(r"<script.*?</script>|<style.*?</style>", " ", value, flags=re.I | re.S)
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def _parse_date(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None


def _parse_rss(source: str, category: str, data: bytes, limit: int = 15) -> list[dict[str, Any]]:
    root = ET.fromstring(data)
    rows: list[dict[str, Any]] = []
    elements = root.findall(".//item")
    if not elements:
        elements = root.findall(".//{http://www.w3.org/2005/Atom}entry")
    for item in elements[:limit]:
        def text_of(*names: str) -> str:
            for name in names:
                node = item.find(name)
                if node is not None and node.text:
                    return node.text.strip()
            return ""

        title = _strip_html(text_of("title", "{http://www.w3.org/2005/Atom}title"))
        if not title:
            continue
        description = _strip_html(
            text_of(
                "description",
                "summary",
                "{http://www.w3.org/2005/Atom}summary",
                "{http://purl.org/rss/1.0/modules/content/}encoded",
            )
        )[:240]
        published_raw = text_of(
            "pubDate", "published", "updated", "{http://www.w3.org/2005/Atom}updated"
        )
        published = _parse_date(published_raw)
        rows.append(
            {
                "source": source,
                "category": category,
                "title": title[:220],
                "summary": description,
                "published": published.isoformat() if published else published_raw[:80],
                "sort_ts": published.timestamp() if published else 0,
            }
        )
    return rows


def fetch_daily_candidates() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(_http_get, url): (source, category)
            for source, (category, url) in DAILY_FEEDS.items()
        }
        for future in as_completed(futures):
            source, category = futures[future]
            try:
                rows.extend(_parse_rss(source, category, future.result(), limit=12))
            except Exception as exc:
                print(f"[sources] {source} failed: {exc}", flush=True)
    cutoff = (utc_now() - timedelta(hours=42)).timestamp()
    recent = [row for row in rows if not row["sort_ts"] or row["sort_ts"] >= cutoff]
    recent.sort(key=lambda row: row["sort_ts"], reverse=True)
    # Keep source diversity before the final model selection.
    selected: list[dict[str, Any]] = []
    per_source: dict[str, int] = {}
    seen_titles: set[str] = set()
    for row in recent:
        normalized = re.sub(r"\W+", "", row["title"].lower())[:60]
        if normalized in seen_titles or per_source.get(row["source"], 0) >= 7:
            continue
        seen_titles.add(normalized)
        per_source[row["source"]] = per_source.get(row["source"], 0) + 1
        selected.append({k: v for k, v in row.items() if k != "sort_ts"})
        if len(selected) >= 55:
            break
    return selected


def _call_model(model: str, prompt: str) -> tuple[bool, str]:
    if not CLIPROXY_KEY:
        return False, "CLIPROXY_API_KEY missing"
    request = urllib.request.Request(
        f"{CLIPROXY_BASE_URL}/chat/completions",
        data=json.dumps(
            {"model": model, "messages": [{"role": "user", "content": prompt}]},
            ensure_ascii=False,
        ).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {CLIPROXY_KEY}",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=MODEL_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
        content = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
        if isinstance(content, list):
            content = "".join(
                part.get("text", "") for part in content if isinstance(part, dict)
            )
        if not isinstance(content, str) or not content.strip():
            return False, "empty response"
        return True, content.strip()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        return False, f"HTTP {exc.code}: {detail}"
    except Exception as exc:
        return False, str(exc)


def _model_alert(title: str, lines: list[str]) -> None:
    # 与实时新闻监控保持一致：微信提醒只保留一条完整状态消息。
    body = title
    fingerprint = hashlib.sha256(body.encode()).hexdigest()[:20]
    enqueue_delivery(f"content-model:{fingerprint}", "model-alert", [body], 90)


def call_model_chain(prompt: str) -> tuple[str, str]:
    previous_primary = _state_get("content_primary_health")
    previous_fallback = _state_get("content_fallback_health")
    ok, result = _call_model(PRIMARY_MODEL, prompt)
    if ok:
        _state_set("content_primary_health", "healthy")
        if previous_primary == "unhealthy":
            _model_alert(
                f"✅ 已恢复主模型｜`{PRIMARY_MODEL}`",
                [],
            )
        return result, PRIMARY_MODEL

    primary_error = result
    _state_set("content_primary_health", "unhealthy")
    ok, result = _call_model(FALLBACK_MODEL, prompt)
    if ok:
        _state_set("content_fallback_health", "healthy")
        if previous_primary != "unhealthy" or previous_fallback == "unhealthy":
            _model_alert(
                f"⚠️ 已切备用｜`{FALLBACK_MODEL}`",
                [],
            )
        return result, FALLBACK_MODEL

    _state_set("content_fallback_health", "unhealthy")
    if previous_primary != "unhealthy" or previous_fallback != "unhealthy":
        _model_alert(
            "🚨 主备均不可用｜本轮终止",
            [],
        )
    raise RuntimeError(f"both models failed: primary={primary_error[:120]}; fallback={result[:120]}")


def _require_marked_messages(result: str) -> list[str]:
    if "[[MESSAGE]]" not in result:
        raise ValueError("model output omitted [[MESSAGE]] boundaries")
    # The model's boundaries are preferred, but an oversized module is a
    # formatting issue, not a reason to discard the whole scheduled job.
    messages = semantic_split(result)
    if not messages:
        raise ValueError("model output contains no message content")
    return messages


def generate_daily_news(run_key: str) -> dict[str, Any]:
    now = datetime.now(TZ)
    candidates = fetch_daily_candidates()
    if len(candidates) < 8:
        raise RuntimeError(f"not enough fresh RSS candidates: {len(candidates)}")
    prompt = f"""你是严谨的中文新闻编辑。当前北京时间：{now:%Y-%m-%d %H:%M}。
只能从下面提供的 RSS 候选中选材，不得添加候选中没有的事实，不确定就省略。

任务：生成 8-10 条每日热点早报，尽量覆盖国内、国际、财经、科技/AI、科学/航天/健康、网络安全/开源、体育。先列 3 条今日重点，且必须来自正文。
每条新闻包含：具体标题、发生了什么、一句影响/关注点、`媒体 · 北京时间`。每条约 45-100 个中文字符。来源之间出现矛盾时不要采用。

微信消息约束：
1. 每条消息必须小于 {MESSAGE_LIMIT} 字；
2. 在每条微信消息开头单独输出一行 [[MESSAGE]]；
3. 只能在完整新闻条目之间切换消息，标题、摘要、影响和来源必须处于同一消息；
4. 不输出表格、长 URL、搜索过程、工具说明、Cron 说明和投递说明；
5. 第一条消息含早报标题和今日重点；后续消息可延续分类，但每个分类标题必须与其第一条新闻同处一条消息。

日期标题格式：📰 **每日热点早报｜{now:%m月%d日} 星期{'一二三四五六日'[now.weekday()]}**

RSS 候选 JSON：
{json.dumps(candidates, ensure_ascii=False)}
"""
    result, model = call_model_chain(prompt)
    messages = _require_marked_messages(result)
    publication = publish_scheduled_post(
        idempotency_key=f"daily-news:{now:%Y-%m-%d}",
        slug=f"daily-news-{now:%Y%m%d}",
        title=f"每日热点早报｜{now:%Y年%m月%d日}",
        now=now,
        description="每日热点早报：覆盖国内、国际、财经、科技与科学等重点新闻。",
        categories=["每日早报"],
        tags=["热点新闻", "每日早报"],
        messages=messages,
    )
    queued = enqueue_delivery(run_key, "daily-news", messages, 60)
    queued["model"] = model
    queued["publication"] = publication
    return queued


def fetch_github_project(day_of_year: int) -> dict[str, Any] | None:
    for offset in range(len(AI_APP_REPOS)):
        repo = AI_APP_REPOS[(day_of_year + offset) % len(AI_APP_REPOS)]
        try:
            payload = json.loads(_http_get(f"https://api.github.com/repos/{repo}").decode("utf-8"))
            if payload.get("archived"):
                continue
            return {
                "name": payload.get("full_name", repo),
                "url": payload.get("html_url", f"https://github.com/{repo}"),
                "description": payload.get("description", ""),
                "language": payload.get("language") or "多语言",
                "stars": payload.get("stargazers_count"),
                "updated_at": payload.get("updated_at", ""),
            }
        except Exception as exc:
            print(f"[github] {repo} metadata failed: {exc}", flush=True)
            # GitHub's anonymous REST quota is shared by the egress IP and can
            # be exhausted by unrelated jobs. The public repository page is
            # an allowed verification source and exposes the same repository
            # name, description, primary language, and star count.
            try:
                page = _http_get(f"https://github.com/{repo}").decode("utf-8", errors="replace")

                def meta(name: str) -> str:
                    patterns = (
                        rf'<meta[^>]+name="{re.escape(name)}"[^>]+content="([^"]*)"',
                        rf'<meta[^>]+content="([^"]*)"[^>]+name="{re.escape(name)}"',
                        rf'<meta[^>]+property="{re.escape(name)}"[^>]+content="([^"]*)"',
                    )
                    for pattern in patterns:
                        match = re.search(pattern, page, flags=re.I)
                        if match:
                            return html.unescape(match.group(1)).strip()
                    return ""

                stars_raw = meta("octolytics-dimension-repository_stars")
                language_match = re.search(
                    r'itemprop="programmingLanguage"[^>]*>([^<]+)<', page, flags=re.I
                )
                description = meta("og:description") or meta("description")
                if stars_raw.isdigit():
                    return {
                        "name": repo,
                        "url": f"https://github.com/{repo}",
                        "description": _strip_html(description)[:300],
                        "language": (
                            html.unescape(language_match.group(1)).strip()
                            if language_match
                            else "多语言"
                        ),
                        "stars": int(stars_raw),
                        "updated_at": "",
                        "verified_via": "GitHub repository page",
                    }
            except Exception as page_exc:
                print(f"[github] {repo} page verification failed: {page_exc}", flush=True)
    return None


def generate_interview(run_key: str) -> dict[str, Any]:
    now = datetime.now(TZ)
    day_of_year = now.timetuple().tm_yday
    topic = TOPICS[day_of_year % len(TOPICS)]
    project = fetch_github_project(day_of_year)
    project_text = json.dumps(project, ensure_ascii=False) if project else "null"
    prompt = f"""你是资深 Java/架构面试官。日期为 {now:%Y-%m-%d}，今日唯一主题是：{topic}。

只出 1 道贴近生产环境的高级问题，必须包含真实场景、约束和故障现象。先给一句话结论，再讲核心原理、关键实现、工程取舍、故障边界、监控排障和常见追问；代码只保留关键片段。当今日主题不是 Kubernetes 时，严禁把排查重点转成容器编排。

最后推荐 1 个 AI 应用层 GitHub 项目。只能使用下面已经由 GitHub API 核实的元数据；若为 null，省略项目推荐，不能编造 Star、语言或链接：
{project_text}

微信消息约束：
1. 内容放得下时合并为 1 条，否则输出 2-3 条完整消息；每条小于 {MESSAGE_LIMIT} 字，并在每条开头单独输出 [[MESSAGE]]；
2. 按完整章节拆分，例如“场景+结论”“原理+实现+取舍”“排障+追问+GitHub 项目”；
3. 代码块必须完整位于一条消息中，绝不从代码中间拆分；
4. 不输出搜索过程、工具调用、Cron 或投递说明。

首行格式：【高级面试题｜{now:%Y-%m-%d}】
第二行：【今日方向】：{topic}
"""
    result, model = call_model_chain(prompt)
    messages = _require_marked_messages(result)
    publication = publish_scheduled_post(
        idempotency_key=f"technical-daily:{now:%Y-%m-%d}",
        slug=f"technical-daily-{now:%Y%m%d}",
        title=f"技术深潜｜{now:%Y年%m月%d日}",
        now=now,
        description=f"围绕{topic}的生产级技术推演、排障思路与 AI 应用项目推荐。",
        categories=["每日技术推送"],
        tags=[topic, "Java", "系统设计", "AI 工程"],
        messages=messages,
    )
    queued = enqueue_delivery(run_key, "interview", messages, 55)
    queued["model"] = model
    queued["publication"] = publication
    return queued


def _google_trends_items(geo: str) -> list[dict[str, Any]]:
    data = _http_get(f"https://trends.google.com/trending/rss?geo={geo}")
    root = ET.fromstring(data)
    ns = {"ht": "https://trends.google.com/trending/rss"}
    rows = []
    for item in root.findall(".//item")[:10]:
        title = (item.findtext("title") or "").strip()
        traffic = (item.findtext("ht:approx_traffic", default="", namespaces=ns) or "").strip()
        sources = []
        for news in item.findall("ht:news_item", ns):
            source = (news.findtext("ht:news_item_source", default="", namespaces=ns) or "").strip()
            news_title = (news.findtext("ht:news_item_title", default="", namespaces=ns) or "").strip()
            if source and news_title:
                sources.append({"source": source, "title": news_title})
        if title:
            rows.append({"topic": title, "signal": f"Google Trends {geo}", "traffic": traffic, "sources": sources})
    return rows


def _x_trends_items() -> list[str]:
    try:
        page = _http_get("https://trends24.in/").decode("utf-8", errors="replace")
    except Exception as exc:
        print(f"[trends] Trends24 unavailable: {exc}", flush=True)
        return []
    topics = []
    pattern = r'<a[^>]+href="[^"]*twitter\.com/search[^"]*"[^>]*>(.*?)</a>'
    for match in re.finditer(pattern, page, flags=re.I | re.S):
        topic = _strip_html(match.group(1))
        if 2 <= len(topic) <= 60 and topic not in topics:
            topics.append(topic)
        if len(topics) >= 12:
            break
    return topics


def _verify_topic(topic: str) -> list[dict[str, str]]:
    query = urllib.parse.quote(f'"{topic}" when:2d')
    url = f"https://news.google.com/rss/search?q={query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
    try:
        rows = _parse_rss("Google News", "热议核验", _http_get(url), limit=8)
    except Exception:
        return []
    verified = []
    seen = set()
    for row in rows:
        # Google News titles usually end with " - publisher".
        publisher = row["title"].rsplit(" - ", 1)[-1].strip()
        key = publisher.lower()
        if key in seen:
            continue
        seen.add(key)
        verified.append({"source": publisher, "title": row["title"], "summary": row["summary"][:160]})
    return verified


def fetch_trend_candidates() -> list[dict[str, Any]]:
    trends: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_google_trends_items, geo): geo for geo in TREND_GEOS}
        for future in as_completed(futures):
            try:
                trends.extend(future.result())
            except Exception as exc:
                print(f"[trends] Google Trends {futures[future]} failed: {exc}", flush=True)

    # Google Trends is already backed by attached news items. X is strictly a
    # discovery signal and must pass a separate two-publisher news check.
    candidates: list[dict[str, Any]] = []
    seen = set()
    for item in trends:
        key = re.sub(r"\W+", "", item["topic"].lower())
        publishers = {source["source"].lower() for source in item["sources"]}
        if key and key not in seen and len(publishers) >= 2:
            seen.add(key)
            candidates.append(item)
    for topic in _x_trends_items()[:8]:
        key = re.sub(r"\W+", "", topic.lower())
        if not key or key in seen:
            continue
        verified = _verify_topic(topic)
        if len({item["source"].lower() for item in verified}) < 2:
            continue
        seen.add(key)
        candidates.append(
            {"topic": topic, "signal": "X 全球热议", "traffic": "", "sources": verified}
        )
    return candidates[:36]


def _trend_daily_count(now: datetime) -> int:
    start = now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    with db_connect() as conn:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM deliveries WHERE category='global-trends' AND created_at>=?",
                (start.isoformat(),),
            ).fetchone()[0]
        )


def generate_trend_digest(run_key: str) -> dict[str, Any]:
    now = datetime.now(TZ)
    if _trend_daily_count(now) >= 3:
        return {"skipped": True, "reason": "daily digest limit reached"}
    candidates = fetch_trend_candidates()
    with db_connect() as conn:
        recent = {
            row[0]
            for row in conn.execute(
                "SELECT fingerprint FROM trend_pushes WHERE pushed_at>=?",
                ((utc_now() - timedelta(hours=30)).isoformat(),),
            )
        }
    filtered = []
    for item in candidates:
        fp = hashlib.sha256(re.sub(r"\W+", "", item["topic"].lower()).encode()).hexdigest()[:24]
        if fp not in recent:
            item["fingerprint"] = fp
            filtered.append(item)
    if not filtered:
        return {"skipped": True, "reason": "no new verified trends"}

    prompt = f"""你是全球热点编辑。北京时间 {now:%Y-%m-%d %H:%M}。
候选话题来自 Google Trends 或 X 趋势发现，但每个候选已经附有至少两个独立新闻发布者。趋势只是发现信号，事实只能来自 sources 中的新闻标题和摘要。

从候选中最多选 4 个、最少可选 0 个真正具有广泛影响或高讨论度的话题。除了政经科技，也应关注影响力较大的社会、娱乐、体育、商业人物事件。自然灾害和普通赛事比分优先留给突发通道，除非已形成全球性讨论。拒绝来源含糊、纯粉丝刷榜、营销标签和无法解释的话题。

严格输出 JSON 数组，不要 Markdown。每个对象字段：fingerprint、title、why_hot、facts、impact、sources（2-3 个媒体名）。fingerprint 必须原样使用候选提供的值；不得补充候选中没有的事实。

候选：
{json.dumps(filtered, ensure_ascii=False)}
"""
    result, model = call_model_chain(prompt)
    clean = result[result.find("[") : result.rfind("]") + 1]
    selected = json.loads(clean)
    if not isinstance(selected, list) or not selected:
        return {"skipped": True, "reason": "model selected no significant trends", "model": model}
    allowed = {item["fingerprint"]: item for item in filtered}
    valid = []
    for item in selected[:4]:
        if not isinstance(item, dict) or item.get("fingerprint") not in allowed:
            continue
        sources = item.get("sources") or []
        if len(set(str(source).strip().lower() for source in sources)) < 2:
            continue
        valid.append(item)
    if not valid:
        return {"skipped": True, "reason": "no selection passed validation", "model": model}

    header = f"🌐 全球热议观察｜{now:%m月%d日 %H:%M}\n（趋势用于发现，事实经至少两个新闻来源交叉核验）"
    atoms = []
    for index, item in enumerate(valid, 1):
        atoms.append(
            f"🔥 {index}｜{item.get('title', '')}\n"
            f"为什么热：{item.get('why_hot', '')}\n"
            f"已核实事实：{item.get('facts', '')}\n"
            f"影响/关注：{item.get('impact', '')}\n"
            f"来源：{'、'.join(str(source) for source in item.get('sources', [])[:3])}"
        )
    messages = []
    current = header
    for atom in atoms:
        candidate = f"{current}\n\n{atom}"
        if len(candidate) <= MESSAGE_LIMIT:
            current = candidate
        else:
            messages.append(current)
            current = f"🌐 全球热议观察（续）\n\n{atom}"
    messages.append(current)
    queued = enqueue_delivery(run_key, "global-trends", messages, 45)
    
    # Also publish to Blog
    try:
        date_str = now.strftime("%Y-%m-%d")
        hour_tag = run_key.split("T")[-1] if "T" in run_key else ""
        slug_key = run_key.replace(":", "-").replace("_", "-").lower()
        publish_scheduled_post(
            idempotency_key=f"blog:{run_key}",
            slug=f"global-trends-{slug_key}",
            title=f"全球热议观察（{date_str} {hour_tag}:00）",
            now=now,
            description=f"全球热议观察与事实交叉核验简报（{date_str} {hour_tag}:00）。",
            categories=["Global Trends", "News"],
            tags=["全球热议", "趋势观察", "新闻核验"],
            messages=messages,
        )
    except Exception as exc:
        print(f"[TrendsBlogError] Failed to submit to blog outbox: {exc}")

    with db_connect() as conn:
        for item in valid:
            conn.execute(
                "INSERT OR IGNORE INTO trend_pushes(fingerprint,title,pushed_at) VALUES(?,?,?)",
                (item["fingerprint"], str(item.get("title", ""))[:200], iso_now()),
            )
    queued["model"] = model
    queued["topics"] = len(valid)
    return queued


JOBS = {
    "daily-news": generate_daily_news,
    "interview": generate_interview,
    "global-trends": generate_trend_digest,
}


def run_job(job_key: str, run_date: str, force: bool = False) -> dict[str, Any]:
    if job_key not in JOBS:
        raise ValueError("unknown job")
    with db_connect() as conn:
        row = conn.execute(
            "SELECT status,attempts,finished_at FROM job_runs WHERE job_key=? AND run_date=?",
            (job_key, run_date),
        ).fetchone()
        if row and row["status"] == "success" and not force:
            return {"skipped": True, "reason": "already completed"}
        attempts = (int(row["attempts"]) if row else 0) + 1
        conn.execute(
            """
            INSERT INTO job_runs(job_key,run_date,status,attempts,started_at,finished_at,last_error)
            VALUES(?,?,'running',?,?,NULL,NULL)
            ON CONFLICT(job_key,run_date) DO UPDATE SET
              status='running',attempts=excluded.attempts,started_at=excluded.started_at,
              finished_at=NULL,last_error=NULL
            """,
            (job_key, run_date, attempts, iso_now()),
        )
    suffix = f":manual:{int(time.time())}" if force else ""
    delivery_key = f"{job_key}:{run_date}{suffix}"
    try:
        result = JOBS[job_key](delivery_key)
        with db_connect() as conn:
            conn.execute(
                "UPDATE job_runs SET status='success',finished_at=?,last_error=NULL WHERE job_key=? AND run_date=?",
                (iso_now(), job_key, run_date),
            )
        return result
    except Exception as exc:
        with db_connect() as conn:
            conn.execute(
                "UPDATE job_runs SET status='failed',finished_at=?,last_error=? WHERE job_key=? AND run_date=?",
                (iso_now(), str(exc)[:500], job_key, run_date),
            )
        raise


def _job_should_retry(job_key: str, run_date: str) -> bool:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT status,attempts,finished_at FROM job_runs WHERE job_key=? AND run_date=?",
            (job_key, run_date),
        ).fetchone()
    if not row:
        return True
    if row["status"] in ("success", "running") or int(row["attempts"]) >= 4:
        return False
    finished = datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else utc_now()
    return (utc_now() - finished).total_seconds() >= 900


def scheduler_loop() -> None:
    while True:
        now = datetime.now(TZ)
        date_key = now.strftime("%Y-%m-%d")
        due: list[tuple[str, str]] = []
        if (now.hour, now.minute) >= (7, 0) and now.hour < 12:
            due.append(("daily-news", date_key))
        if (now.hour, now.minute) >= (7, 30) and now.hour < 12:
            due.append(("interview", date_key))
        if 8 <= now.hour <= 22 and now.hour % 2 == 0 and now.minute < 10:
            due.append(("global-trends", f"{date_key}T{now.hour:02d}"))
        for job_key, run_date in due:
            if not _job_should_retry(job_key, run_date):
                continue
            try:
                print(f"[scheduler] running {job_key} for {run_date}", flush=True)
                result = run_job(job_key, run_date)
                print(f"[scheduler] {job_key} result={result}", flush=True)
            except Exception as exc:
                print(f"[scheduler] {job_key} failed: {exc}", flush=True)
        try:
            Path(HEARTBEAT_PATH).write_text(iso_now(), encoding="utf-8")
        except OSError:
            pass
        time.sleep(30)


def queue_stats() -> dict[str, Any]:
    with db_connect() as conn:
        delivery_rows = conn.execute(
            "SELECT status,COUNT(*) AS count FROM deliveries GROUP BY status"
        ).fetchall()
        chunk_rows = conn.execute(
            "SELECT status,COUNT(*) AS count FROM delivery_chunks GROUP BY status"
        ).fetchall()
        recent_errors = [
            dict(row)
            for row in conn.execute(
                """
                SELECT id,category,status,last_error,updated_at
                FROM deliveries WHERE last_error IS NOT NULL
                ORDER BY updated_at DESC LIMIT 5
                """
            )
        ]
    return {
        "deliveries": {row["status"]: row["count"] for row in delivery_rows},
        "chunks": {row["status"]: row["count"] for row in chunk_rows},
        "recent_errors": recent_errors,
    }


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "HermesDelivery/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[api] {self.address_string()} {fmt % args}", flush=True)

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        if not QUEUE_TOKEN:
            return False
        supplied = self.headers.get("Authorization", "")
        expected = f"Bearer {QUEUE_TOKEN}"
        return hashlib.sha256(supplied.encode()).digest() == hashlib.sha256(expected.encode()).digest()

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self._json(200, {"ok": True, "time": iso_now()})
            return
        if self.path == "/v1/stats" and self._authorized():
            self._json(200, queue_stats())
            return
        self._json(401 if not self._authorized() else 404, {"error": "unauthorized or not found"})

    def do_POST(self) -> None:  # noqa: N802
        if not self._authorized():
            self._json(401, {"error": "unauthorized"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 1_000_000:
                raise ValueError("invalid request size")
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if self.path == "/v1/deliveries":
                messages = payload.get("messages")
                if not isinstance(messages, list):
                    content = payload.get("content")
                    messages = [content] if isinstance(content, str) else []
                result = enqueue_delivery(
                    str(payload.get("idempotency_key", "")),
                    str(payload.get("category", "general")),
                    messages,
                    int(payload.get("priority", 50)),
                )
                self._json(202, result)
                return
            match = re.fullmatch(r"/v1/jobs/([a-z-]+)/run", self.path)
            if match:
                job_key = match.group(1)
                run_date = str(payload.get("run_date") or datetime.now(TZ).strftime("%Y-%m-%d"))
                result = run_job(job_key, run_date, force=bool(payload.get("force", True)))
                self._json(200, result)
                return
            self._json(404, {"error": "not found"})
        except ValueError as exc:
            self._json(400, {"error": str(exc)})
        except Exception as exc:
            print(f"[api] request failed: {exc}", flush=True)
            self._json(500, {"error": str(exc)[:300]})


def main() -> None:
    if not QUEUE_TOKEN:
        raise SystemExit("DELIVERY_QUEUE_TOKEN is required")
    init_db()
    threading.Thread(target=sender_loop, name="weixin-sender", daemon=True).start()
    threading.Thread(target=scheduler_loop, name="content-scheduler", daemon=True).start()
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), ApiHandler)
    print(
        f"Hermes delivery service listening on {LISTEN_HOST}:{LISTEN_PORT}; "
        f"models={PRIMARY_MODEL}->{FALLBACK_MODEL}",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
