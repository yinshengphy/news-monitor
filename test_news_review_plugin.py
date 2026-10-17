import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


PLUGIN = Path(__file__).parent / "hermes_plugins/news-review-direct/__init__.py"
spec = importlib.util.spec_from_file_location("news_review_direct", PLUGIN)
plugin = importlib.util.module_from_spec(spec)
assert spec.loader
spec.loader.exec_module(plugin)


class NewsReviewPluginTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        plugin.IDENTITY_FILE = Path(self.tempdir.name) / "identity.json"
        plugin.IDENTITY_FILE.write_text(json.dumps({
            "chat_id_sha256": hashlib.sha256(b"owner-chat").hexdigest(),
            "user_id_sha256": hashlib.sha256(b"owner-user").hexdigest(),
        }))

    def tearDown(self):
        self.tempdir.cleanup()

    def source(self, platform="weixin", chat_type="dm", chat="owner-chat", user="owner-user"):
        return SimpleNamespace(platform=platform, chat_type=chat_type, chat_id=chat, user_id=user)

    def test_requires_weixin_private_chat_and_both_identity_fields(self):
        self.assertTrue(plugin._authorized(self.source()))
        self.assertFalse(plugin._authorized(self.source(chat_type="group")))
        self.assertFalse(plugin._authorized(self.source(platform="telegram")))
        self.assertFalse(plugin._authorized(self.source(chat="other")))
        self.assertFalse(plugin._authorized(self.source(user="other")))

    def test_commands_are_unambiguous_and_do_not_capture_essay_commands(self):
        self.assertEqual(plugin._parse("同意发布"), {"command": "approve", "draft_id": ""})
        self.assertEqual(plugin._parse("不发布 20261017-v2-deadbeef")["draft_id"],
                         "20261017-v2-deadbeef")
        self.assertEqual(plugin._parse("待审新闻"), {"command": "list"})
        self.assertEqual(plugin._parse("修改新闻：旧内容改成新内容")["command"], "replace")
        self.assertIsNone(plugin._parse("确认"))
        self.assertIsNone(plugin._parse("修改：随笔旧内容改成新内容"))
        self.assertIsNone(plugin._parse("同意发布吧"))


if __name__ == "__main__":
    unittest.main()
