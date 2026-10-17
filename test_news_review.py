import sqlite3
import unittest
from datetime import datetime, timedelta, timezone

import news_review


BODY = "## 境外报道与争议议题\n\n### 示例议题\n事实摘要与争议。"
SOURCES = [{"name": "Example", "url": "https://example.com/report",
            "published": "2026-10-17T01:00:00+00:00", "verification": "single-source"}]
NOW = datetime(2026, 10, 17, 2, tzinfo=timezone.utc)


class NewsReviewTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        news_review.init_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def draft(self, recipient="owner", body=BODY, date="2026-10-17", now=NOW):
        return news_review.create_previewed_draft(
            self.conn, news_date=date, generation_date="2026-10-17", body=body,
            sources=SOURCES, recipient=recipient, now=now,
        )

    def test_unapproved_draft_never_calls_publisher(self):
        self.draft()
        self.assertEqual(self.conn.execute(
            "SELECT status FROM news_review_drafts").fetchone()[0], "previewed")

    def test_cross_recipient_is_rejected(self):
        draft = self.draft()
        with self.assertRaises(PermissionError):
            news_review.approve(self.conn, recipient="attacker", draft_id=draft["public_id"],
                                publisher=lambda *_: {"accepted": True})

    def test_expired_draft_is_not_publishable(self):
        draft = self.draft()
        with self.assertRaises(ValueError):
            news_review.approve(
                self.conn, recipient="owner", draft_id=draft["public_id"],
                now=NOW + timedelta(hours=49), publisher=lambda *_: {"accepted": True})

    def test_new_version_supersedes_old_preview(self):
        first = self.draft()
        second = self.draft(body=BODY + "\n\n已知回应。", now=NOW + timedelta(minutes=1))
        old = self.conn.execute("SELECT status FROM news_review_drafts WHERE public_id=?",
                                (first["public_id"],)).fetchone()[0]
        self.assertEqual(old, "superseded")
        self.assertNotEqual(first["body_hash"], second["body_hash"])

    def test_multiple_dates_require_explicit_id(self):
        self.draft()
        self.draft(date="2026-10-16", now=NOW + timedelta(minutes=1))
        with self.assertRaisesRegex(ValueError, "multiple"):
            news_review.approve(self.conn, recipient="owner",
                                publisher=lambda *_: {"accepted": True})

    def test_retry_reuses_exact_body_and_idempotency_key(self):
        draft = self.draft()
        calls = []
        def publisher(path, payload):
            calls.append((path, payload.copy()))
            return ({"accepted": False, "error": "timeout"} if len(calls) == 1
                    else {"accepted": True, "id": 7})
        failed = news_review.approve(self.conn, recipient="owner", draft_id=draft["public_id"],
                                     publisher=publisher)
        done = news_review.approve(self.conn, recipient="owner", draft_id=draft["public_id"],
                                   publisher=publisher)
        self.assertEqual(failed["status"], "publish_failed")
        self.assertEqual(done["status"], "published")
        self.assertEqual(calls[0][1], calls[1][1])

    def test_duplicate_confirmation_does_not_publish_again(self):
        draft = self.draft()
        calls = []
        publisher = lambda path, payload: calls.append(payload) or {"accepted": True, "id": 1}
        news_review.approve(self.conn, recipient="owner", draft_id=draft["public_id"],
                            publisher=publisher)
        again = news_review.approve(self.conn, recipient="owner", draft_id=draft["public_id"],
                                    publisher=publisher)
        self.assertTrue(again["duplicate"])
        self.assertEqual(len(calls), 1)

    def test_cancel_blocks_approval(self):
        draft = self.draft()
        news_review.cancel(self.conn, recipient="owner", draft_id=draft["public_id"])
        with self.assertRaises(ValueError):
            news_review.approve(self.conn, recipient="owner", draft_id=draft["public_id"],
                                publisher=lambda *_: {"accepted": True})

    def test_replace_creates_new_preview_and_requires_new_confirmation(self):
        draft = self.draft()
        changed = news_review.replace_and_preview(
            self.conn, recipient="owner", draft_id=draft["public_id"],
            old="事实摘要", new="经核实的事实摘要", now=NOW + timedelta(minutes=1))
        self.assertEqual(changed["version"], 2)
        rows = self.conn.execute("SELECT version,status,body FROM news_review_drafts ORDER BY version").fetchall()
        self.assertEqual(rows[0]["status"], "superseded")
        self.assertEqual(rows[1]["status"], "previewed")
        self.assertIn("经核实", rows[1]["body"])

    def test_source_and_body_validation_fail_closed(self):
        with self.assertRaises(ValueError):
            news_review.create_previewed_draft(
                self.conn, news_date="2026-10-17", generation_date="2026-10-17",
                body="没有固定标题", sources=SOURCES, recipient="owner", now=NOW)
        with self.assertRaises(ValueError):
            news_review.create_previewed_draft(
                self.conn, news_date="2026-10-17", generation_date="2026-10-17",
                body=BODY, sources=[{"url": "http://unsafe.test", "published": "today"}],
                recipient="owner", now=NOW)


if __name__ == "__main__":
    unittest.main()
