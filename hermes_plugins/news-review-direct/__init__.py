"""Zero-token, fail-closed Hermes hook for news review commands."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)
SECRET_DIR = Path.home() / ".hermes" / "secrets"
TOKEN_FILE = SECRET_DIR / "news-review-api-token"
IDENTITY_FILE = SECRET_DIR / "news-review-identity.json"
API_URL = "http://127.0.0.1:8787/v1/news-review/command"

APPROVE_RE = re.compile(r"^同意发布(?:\s+([0-9]{8}-v\d+-[0-9a-f]{8}))?$")
CANCEL_RE = re.compile(r"^不发布(?:\s+([0-9]{8}-v\d+-[0-9a-f]{8}))?$")
REPLACE_RE = re.compile(
    r"^修改新闻[：:]\s*(?:([0-9]{8}-v\d+-[0-9a-f]{8})\s+)?(.+?)改成(.+?)$"
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _platform(source: Any) -> str:
    value = getattr(source, "platform", "")
    return str(getattr(value, "value", value)).lower()


def _authorized(source: Any) -> bool:
    """Require WeChat DM and both configured chat/user hashes."""
    if _platform(source) != "weixin" or getattr(source, "chat_type", "") != "dm":
        return False
    chat_id = str(getattr(source, "chat_id", "") or "")
    user_id = str(getattr(source, "user_id", "") or "")
    if not chat_id or not user_id:
        return False
    try:
        allowed = json.loads(IDENTITY_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return (
        _sha(chat_id) == allowed.get("chat_id_sha256")
        and _sha(user_id) == allowed.get("user_id_sha256")
    )


def _parse(text: str) -> dict[str, str] | None:
    trimmed = text.strip()
    if trimmed == "待审新闻":
        return {"command": "list"}
    match = APPROVE_RE.fullmatch(trimmed)
    if match:
        return {"command": "approve", "draft_id": match.group(1) or ""}
    match = CANCEL_RE.fullmatch(trimmed)
    if match:
        return {"command": "cancel", "draft_id": match.group(1) or ""}
    match = REPLACE_RE.fullmatch(trimmed)
    if match:
        return {"command": "replace", "draft_id": match.group(1) or "",
                "old": match.group(2).strip(), "new": match.group(3).strip()}
    return None


def _call(source: Any, command: dict[str, str]) -> dict[str, Any]:
    token = TOKEN_FILE.read_text(encoding="utf-8").strip()
    if not token:
        raise RuntimeError("审批服务凭据未配置")
    # Raw identifiers never leave localhost and are never logged. The service
    # stores only a SHA-256 identity digest.
    command["identity"] = f"weixin:{getattr(source, 'chat_id')}"
    request = urllib.request.Request(
        API_URL, method="POST",
        data=json.dumps(command, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=130) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read().decode("utf-8")).get("error", "请求失败")
        except Exception:
            detail = f"HTTP {exc.code}"
        raise RuntimeError(detail) from exc


def _reply(result: dict[str, Any]) -> str:
    status = result.get("status")
    if status == "published":
        return "✅ 已按你预览的确切版本提交发布。" + ("（重复确认未重复写入）" if result.get("duplicate") else "")
    if status == "publish_failed":
        return "⚠️ 发布提交失败，草稿保持不变；可稍后用同一命令安全重试。"
    if status == "cancelled":
        return "🗑️ 待审稿已取消，不会发布。"
    if status == "previewed":
        return f"📝 修改后的新版本已保存：{result.get('public_id')}。请先查看新预览，再回复同意发布。"
    if status == "ok":
        drafts = result.get("drafts", [])
        if not drafts:
            return "当前没有待审批的新闻稿。"
        lines = ["待审批新闻稿："]
        lines.extend(f"• {item['public_id']}（{item['news_date']}，{item['status']}）" for item in drafts)
        return "\n".join(lines)
    return "审批命令已处理。"


def _background(command: dict[str, str], event: Any, gateway: Any,
                loop: asyncio.AbstractEventLoop) -> None:
    try:
        reply = _reply(_call(event.source, command))
    except Exception as exc:
        reply = f"❌ 新闻审批处理失败：{exc}"
    adapter = gateway.adapters.get(event.source.platform)
    if adapter:
        asyncio.run_coroutine_threadsafe(
            adapter.send(str(event.source.chat_id), reply), loop,
        )


def pre_gateway_dispatch(event: Any, gateway: Any, session_store: Any,
                         **kwargs: Any) -> dict[str, Any] | None:
    text = getattr(event, "text", None)
    command = _parse(text) if isinstance(text, str) else None
    if not command:
        return None
    # Hermes invokes all hooks even when an earlier hook returns skip.
    # The essay hook must explicitly yield recognized news commands.
    setattr(event, "_news_review_command", True)
    # Recognized commands from groups/other identities are consumed and
    # rejected without revealing whether a draft exists.
    if not _authorized(event.source):
        return {"action": "skip", "reason": "news-review-unauthorized"}
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return {"action": "skip", "reason": "news-review-no-loop"}
    threading.Thread(target=_background, args=(command, event, gateway, loop),
                     daemon=True, name="news-review-direct").start()
    return {"action": "skip", "reason": "news-review-direct"}


def register(ctx: Any) -> None:
    ctx.register_hook("pre_gateway_dispatch", pre_gateway_dispatch)
    logger.info("news-review-direct plugin loaded")
