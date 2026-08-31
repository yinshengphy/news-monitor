import importlib
import os
import tempfile
import unittest


class DeliveryServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.TemporaryDirectory()
        os.environ["DELIVERY_DB_PATH"] = os.path.join(cls.tempdir.name, "delivery.db")
        os.environ["WEIXIN_MESSAGE_LIMIT"] = "120"
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


if __name__ == "__main__":
    unittest.main()
