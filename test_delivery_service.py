import importlib
import os
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone


class DeliveryServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.TemporaryDirectory()
        os.environ["DELIVERY_DB_PATH"] = os.path.join(cls.tempdir.name, "delivery.db")
        os.environ["WEIXIN_MESSAGE_LIMIT"] = "120"
        os.environ["ENABLE_WEIXIN_DELIVERY"] = "true"
        cls.service = importlib.import_module("delivery_service")
        cls.service.init_db()

    @classmethod
    def tearDownClass(cls):
        cls.tempdir.cleanup()

    def test_marked_modules_stay_intact(self):
        messages = self.service.semantic_split(
            "[[MESSAGE]]\n第一条完整消息\n\n完整段落\n[[MESSAGE]]\n第二条完整消息"
        )
        self.assertEqual(messages, ["第一条完整消息\n\n完整段落", "第二条完整消息"])

    def test_preview_receipt_and_modified_version_gate_publication(self):
        import news_review
        body = "## 境外报道与争议议题\n\n隔离测试事实。"
        with self.service.db_connect() as conn:
            draft = news_review.create_previewed_draft(
                conn, news_date="2026-10-17", generation_date="2026-10-17", body=body,
                sources=[{"name": "Example", "url": "https://example.com/report",
                          "published": "2026-10-17T01:00:00Z"}], recipient="test-owner")
        self.service.queue_news_review_preview(draft)
        command = {"identity": "test-owner", "command": "approve", "draft_id": draft["public_id"]}
        with patch.object(self.service, "submit_blog_publication", return_value={"accepted": True}) as publisher:
            with self.assertRaisesRegex(ValueError, "preview"):
                self.service.handle_news_review_command(command)
            publisher.assert_not_called()
            with self.service.db_connect() as conn:
                conn.execute("UPDATE deliveries SET status='sent' WHERE idempotency_key=?",
                             ("news-review-preview:" + draft["public_id"],))
            modified = self.service.handle_news_review_command({
                **command, "command": "replace", "old": "事实", "new": "新事实"})
            command["draft_id"] = modified["public_id"]
            with self.assertRaisesRegex(ValueError, "preview"):
                self.service.handle_news_review_command(command)
            with self.service.db_connect() as conn:
                chunks = conn.execute("""SELECT body FROM delivery_chunks WHERE delivery_id=? ORDER BY seq""",
                                      (modified["delivery"]["delivery_id"],)).fetchall()
                self.assertIn("新事实", "".join(row[0] for row in chunks))
                conn.execute("UPDATE deliveries SET status='sent' WHERE idempotency_key=?",
                             ("news-review-preview:" + modified["public_id"],))
            self.assertEqual(self.service.handle_news_review_command(command)["status"], "published")
            self.service.handle_news_review_command(command)
            self.assertEqual(publisher.call_count, 1)
            self.assertEqual(publisher.call_args.args[1]["body"], modified["body"])

    def test_enqueue_is_idempotent_and_client_id_is_stable(self):
        first = self.service.enqueue_delivery(
            "test:stable-id", "test", ["完整消息"], 10
        )
        duplicate = self.service.enqueue_delivery(
            "test:stable-id", "test", ["不应覆盖原消息"], 10
        )
        self.assertFalse(first["duplicate"])
        self.assertTrue(duplicate["duplicate"])
        with self.service.db_connect() as conn:
            rows = conn.execute(
                "SELECT body,client_id FROM delivery_chunks WHERE delivery_id=?",
                (first["delivery_id"],),
            ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["body"], "完整消息")
        self.assertTrue(rows[0]["client_id"].startswith("hermes-q-"))

    def test_oversized_marked_module_is_split_instead_of_failing(self):
        oversized = "[[MESSAGE]]\n" + ("这是一个完整段落。" * 30)
        messages = self.service.semantic_split(oversized)
        self.assertGreater(len(messages), 1)
        self.assertTrue(all(len(message) <= 120 for message in messages))

    def test_oversized_code_block_is_split_into_valid_fences(self):
        oversized = "```java\n" + ("a\n" * 100) + "```"
        messages = self.service.semantic_split(oversized)
        self.assertGreater(len(messages), 1)
        self.assertTrue(all(len(message) <= 120 for message in messages))
        self.assertTrue(all(message.startswith("```java\n") and message.endswith("\n```") for message in messages))

    def test_long_code_line_is_split_within_the_fence_budget(self):
        oversized = "```text\n" + ("x" * 200) + "\n```"
        messages = self.service.semantic_split(oversized)
        self.assertTrue(all(len(message) <= 120 for message in messages))
        self.assertTrue(all(message.startswith("```text\n") and message.endswith("\n```") for message in messages))

    def test_rss_parser_keeps_real_link_and_publication_time(self):
        rows = self.service._parse_rss("Source", "国际", b"""
          <rss><channel><item><title>Report</title>
          <link>https://example.com/report</link>
          <pubDate>Sat, 17 Oct 2026 01:00:00 GMT</pubDate>
          <description>Facts only</description></item></channel></rss>""")
        self.assertEqual(rows[0]["url"], "https://example.com/report")
        self.assertTrue(rows[0]["published"].startswith("2026-10-17T01:00:00"))

    def test_review_omits_stale_or_undated_candidates_without_model_call(self):
        original_receiver = self.service.WEIXIN_RECEIVER
        original_model = self.service.call_model_chain
        self.service.WEIXIN_RECEIVER = "configured-recipient"
        self.service.call_model_chain = lambda _: self.fail("model must not be called")
        try:
            result = self.service.prepare_news_review([
                {"url": "https://example.com/old", "published": "2026-10-01T00:00:00Z"},
                {"url": "https://example.com/unknown", "published": ""},
            ], datetime(2026, 10, 17, 7, tzinfo=timezone.utc))
        finally:
            self.service.WEIXIN_RECEIVER = original_receiver
            self.service.call_model_chain = original_model
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
