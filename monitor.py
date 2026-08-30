#!/usr/bin/env python3
"""
Global Breaking News Monitoring Service (Production Grade v2)
- Concurrent multi-source RSS & GeoJSON ingestion
- Outbox pattern for reliable WeChat delivery with exponential backoff
- Enhanced topic deduplication: geographic entity normalization + LLM active incident context
- Strict LLM evaluation fallback chain through host CLIProxyAPI (Gemini -> Codex Luna)
- Clean WeChat card typography without unrendered Markdown and raw URLs
- Watchdog heartbeat monitoring
"""

import os
import sys
import time
import json
import sqlite3
import hashlib
import re
import urllib.request
import urllib.error
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ----------------- Configuration -----------------
DB_PATH = os.environ.get("NEWS_DB_PATH", "/data/news.db")
HERMES_HOME = os.environ.get("HERMES_HOME", "/home/yinsheng/.hermes")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "180"))
HTTP_PROXY = os.environ.get("HTTP_PROXY", "http://127.0.0.1:7890")
CLIPROXY_KEY = os.environ.get("CLIPROXY_API_KEY", "")
CLIPROXY_BASE_URL = os.environ.get("CLIPROXY_BASE_URL", "http://127.0.0.1:8317/v1").rstrip("/")
PRIMARY_MODEL = os.environ.get("PRIMARY_MODEL", "gemini-3.7-flash-high")
FALLBACK_MODEL = os.environ.get("FALLBACK_MODEL", "gpt-5.6-luna")
MODEL_TIMEOUT_SECONDS = int(os.environ.get("MODEL_TIMEOUT_SECONDS", "25"))
TARGET_WEIXIN = os.environ.get("WEIXIN_RECEIVER", "o9cq80-itgP5_UnL3cyqo9gv9JBg@im.wechat")
HEARTBEAT_PATH = os.environ.get("HEARTBEAT_PATH", "/tmp/news_monitor_heartbeat")

# News Sources Definition
SOURCES = {
    # 1. Global Seismic & Natural Disaster Feeds
    "USGS Earthquakes": {
        "url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson",
        "type": "geojson_usgs",
        "weight": 95,
        "is_official": True
    },
    # 2. International Top News & Breaking RSS
    "AP News Top Headlines": {
        "url": "https://rsshub.rssforever.com/apnews/topics/apf-topnews",
        "type": "rss",
        "weight": 90,
        "is_official": False
    },
    "AP News World": {
        "url": "https://rsshub.rssforever.com/apnews/topics/world-news",
        "type": "rss",
        "weight": 90,
        "is_official": False
    },
    "BBC World News": {
        "url": "https://feeds.bbci.co.uk/news/world/rss.xml",
        "type": "rss",
        "weight": 90,
        "is_official": False
    },
    "BBC Chinese (中文要闻)": {
        "url": "https://feeds.bbci.co.uk/zhongwen/simp/rss.xml",
        "type": "rss",
        "weight": 88,
        "is_official": False
    },
    "DW Chinese (德国之声)": {
        "url": "https://rss.dw.com/xml/rss-chi-all",
        "type": "rss",
        "weight": 85,
        "is_official": False
    },
    "The Guardian World": {
        "url": "https://www.theguardian.com/world/rss",
        "type": "rss",
        "weight": 85,
        "is_official": False
    },
    "CNN World News": {
        "url": "http://rss.cnn.com/rss/edition_world.rss",
        "type": "rss",
        "weight": 85,
        "is_official": False
    },
    "Al Jazeera English": {
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
        "type": "rss",
        "weight": 85,
        "is_official": False
    },
    "RFI 法国国际广播": {
        "url": "https://www.rfi.fr/cn/%E4%B8%AD%E5%9B%BD/rss",
        "type": "rss",
        "weight": 80,
        "is_official": False
    },
    "CNA 中央社": {
        "url": "https://feeds.feedburner.com/cnaFirstNews",
        "type": "rss",
        "weight": 80,
        "is_official": False
    },
    # 3. Aggregators & Google News Radar
    "Google News China Breaking": {
        "url": "https://news.google.com/rss/search?q=%E4%B8%AD%E5%9B%BD%20when:1d&hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
        "type": "rss",
        "weight": 80,
        "is_official": False
    },
    "Google News Top": {
        "url": "https://news.google.com/rss?hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
        "type": "rss",
        "weight": 80,
        "is_official": False
    },
    # 4. Domestic Fast News & Flash Feeds
    "RSSHub 36Kr 快讯": {
        "url": "https://rsshub.rssforever.com/36kr/newsflashes",
        "type": "rss",
        "weight": 70,
        "is_official": False
    },
    "RSSHub Solidot 科技要闻": {
        "url": "https://rsshub.rssforever.com/solidot",
        "type": "rss",
        "weight": 65,
        "is_official": False
    }
}

# ----------------- Database Setup & Migrations -----------------
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # 1. Base table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS processed_items (
        hash TEXT PRIMARY KEY,
        source TEXT,
        title TEXT,
        link TEXT,
        published_at TEXT,
        discovered_at TEXT,
        score INTEGER,
        is_pushed INTEGER DEFAULT 0
    )
    """)
    
    # Migrations
    cur.execute("PRAGMA table_info(processed_items)")
    columns = [col[1] for col in cur.fetchall()]
    
    if "push_status" not in columns:
        cur.execute("ALTER TABLE processed_items ADD COLUMN push_status INTEGER DEFAULT 0")
    if "retry_count" not in columns:
        cur.execute("ALTER TABLE processed_items ADD COLUMN retry_count INTEGER DEFAULT 0")
    if "last_retry_at" not in columns:
        cur.execute("ALTER TABLE processed_items ADD COLUMN last_retry_at TEXT")
    if "incident_key" not in columns:
        cur.execute("ALTER TABLE processed_items ADD COLUMN incident_key TEXT")
    if "wechat_msg" not in columns:
        cur.execute("ALTER TABLE processed_items ADD COLUMN wechat_msg TEXT")

    # 2. Active incidents table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS active_incidents (
        incident_key TEXT PRIMARY KEY,
        location TEXT,
        level TEXT,
        first_discovered_at TEXT,
        last_pushed_at TEXT,
        highest_score INTEGER,
        first_title TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS system_state (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT
    )
    """)
    conn.commit()
    conn.close()

def is_item_processed(item_hash: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM processed_items WHERE hash = ?", (item_hash,))
    row = cur.fetchone()
    conn.close()
    return row is not None

def record_item(item_hash: str, source: str, title: str, link: str, published_at: str, 
                score: int, is_pushed: int, push_status: int = 0, incident_key: str = None, wechat_msg: str = None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    now_str = datetime.now(timezone.utc).isoformat()
    cur.execute("""
    INSERT INTO processed_items (hash, source, title, link, published_at, discovered_at, score, is_pushed, push_status, retry_count, last_retry_at, incident_key, wechat_msg)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, ?)
    ON CONFLICT(hash) DO UPDATE SET
        score=excluded.score,
        push_status=excluded.push_status,
        incident_key=excluded.incident_key,
        wechat_msg=excluded.wechat_msg
    """, (item_hash, source, title, link, published_at, now_str, score, is_pushed, push_status, incident_key, wechat_msg))
    conn.commit()
    conn.close()

# ----------------- Topic Deduplication & Entity Normalization -----------------
ADMIN_SUFFIXES = ["中国", "自治区", "特别行政区", "壮族", "维吾尔", "回族", "藏族", "蒙古", "省", "市", "地区", "州", "盟", "县", "区", "镇", "乡", "口岸", "海域", "附近"]

def extract_geo_tokens(location: str, title: str) -> set[str]:
    """
    Extract normalized geographic entities (e.g. {'西藏', '日喀则', '吉隆'})
    stripping common administrative decorations.
    """
    text = f"{location} {title}"
    # Extract 2-4 character Chinese tokens
    words = re.findall(r"[\u4e00-\u9fa5]{2,4}", text)
    clean_tokens = set()
    for w in words:
        cleaned = w
        for suffix in ADMIN_SUFFIXES:
            if len(cleaned) > 2 and cleaned.endswith(suffix):
                cleaned = cleaned[:-len(suffix)]
            elif cleaned == suffix:
                cleaned = ""
        if len(cleaned) >= 2 and cleaned not in ["事故", "灾害", "发生", "启动", "响应", "救援", "突发", "国家", "最新", "现场"]:
            clean_tokens.add(cleaned)
    return clean_tokens

def extract_event_type(level: str, title: str) -> str:
    types = ["泥石流", "地震", "山洪", "暴雨", "滑坡", "海啸", "爆炸", "坠毁", "空袭", "袭击", "交火", "政变", "大火", "坍塌", "台风", "飓风"]
    combined = f"{level} {title}"
    for t in types:
        if t in combined:
            return t
    return "突发事件"

def get_active_incidents_summary() -> list[dict]:
    """
    Fetch active incidents from the last 4 hours for LLM context injection.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    SELECT incident_key, location, level, highest_score, first_title, last_pushed_at
    FROM active_incidents
    ORDER BY last_pushed_at DESC
    LIMIT 6
    """)
    rows = cur.fetchall()
    conn.close()
    
    summary = []
    now_utc = datetime.now(timezone.utc)
    for r in rows:
        try:
            last_dt = datetime.fromisoformat(r[5])
            mins = int((now_utc - last_dt).total_seconds() / 60)
            summary.append({
                "key": r[0],
                "location": r[1],
                "level": r[2],
                "score": r[3],
                "title": r[4],
                "elapsed_mins": mins
            })
        except Exception:
            pass
    return summary

def check_incident_cooldown_enhanced(location: str, level: str, title: str, score: int, is_followup_llm: bool) -> tuple[bool, str, str]:
    """
    Enhanced deduplication:
    1. Checks if LLM flagged it as followup
    2. Checks fuzzy token overlap against active_incidents within 4 hours
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT incident_key, location, level, last_pushed_at, highest_score, first_title FROM active_incidents")
    rows = cur.fetchall()
    now_utc = datetime.now(timezone.utc)
    now_str = now_utc.isoformat()

    new_tokens = extract_geo_tokens(location, title)
    new_type = extract_event_type(level, title)
    canonical_key = f"{''.join(sorted(list(new_tokens))[:3])}_{new_type}" if new_tokens else f"{title[:10]}_{new_type}"

    for row in rows:
        inc_key, exist_loc, exist_level, last_pushed_str, highest_score, first_title = row
        try:
            last_pushed_dt = datetime.fromisoformat(last_pushed_str)
            elapsed_seconds = (now_utc - last_pushed_dt).total_seconds()
        except Exception:
            elapsed_seconds = 999999

        # Active within 4 hours (14400s)
        if elapsed_seconds < 14400:
            exist_tokens = extract_geo_tokens(exist_loc, first_title)
            exist_type = extract_event_type(exist_level, first_title)
            
            # Check overlap
            common_tokens = new_tokens & exist_tokens
            type_match = (new_type == exist_type) or ("灾害" in new_type and "灾害" in exist_type)
            
            if (len(common_tokens) >= 1 and type_match) or is_followup_llm or (canonical_key == inc_key):
                # Matched active incident!
                if score >= highest_score + 15:
                    cur.execute("""
                    UPDATE active_incidents
                    SET last_pushed_at = ?, highest_score = ?
                    WHERE incident_key = ?
                    """, (now_str, max(highest_score, score), inc_key))
                    conn.commit()
                    conn.close()
                    return True, f"Major escalation (+{score - highest_score} pts)", inc_key
                
                conn.close()
                return False, f"Cooldown active ({int(elapsed_seconds/60)}m ago: {first_title[:25]})", inc_key

    # New incident
    cur.execute("""
    INSERT INTO active_incidents (incident_key, location, level, first_discovered_at, last_pushed_at, highest_score, first_title)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(incident_key) DO UPDATE SET
        last_pushed_at=excluded.last_pushed_at,
        highest_score=excluded.highest_score
    """, (canonical_key, location, level, now_str, now_str, score, title))
    conn.commit()
    conn.close()
    return True, "New incident registered", canonical_key

# ----------------- Model Inference Pipeline -----------------
def call_cliproxy(model_name: str, prompt: str) -> tuple[bool, str]:
    """Call either Gemini or Codex through the host-local OpenAI-compatible gateway."""
    if not CLIPROXY_KEY:
        return False, "CLIPROXY_API_KEY missing"
    url = f"{CLIPROXY_BASE_URL}/chat/completions"
    body = json.dumps({
        "model": model_name,
        "messages": [{"role": "user", "content": prompt}]
    }).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={
        "Authorization": f"Bearer {CLIPROXY_KEY}",
        "Content-Type": "application/json"
    })
    try:
        with urllib.request.urlopen(req, timeout=MODEL_TIMEOUT_SECONDS) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if isinstance(content, list):
                content = "".join(
                    part.get("text", "") for part in content if isinstance(part, dict)
                )
            if not isinstance(content, str) or not content.strip():
                return False, "empty model response"
            return True, content.strip()
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            detail = str(e.reason)
        return False, f"HTTP {e.code}: {detail}"
    except urllib.error.URLError as e:
        return False, f"network error: {e.reason}"
    except Exception as e:
        return False, f"request error: {e}"

# ----------------- Model Interface State Alerts -----------------
PRIMARY_HEALTH_KEY = "model_health_primary"
FALLBACK_HEALTH_KEY = "model_health_fallback"
MODEL_ALERT_PENDING_KEY = "model_alert_pending"


def get_system_state(key: str, default: str = "") -> str:
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("SELECT value FROM system_state WHERE key = ?", (key,)).fetchone()
        conn.close()
        return row[0] if row else default
    except Exception as e:
        print(f"[State Warning] Failed to read {key}: {e}", flush=True)
        return default


def _upsert_system_state(cur, key: str, value: str):
    cur.execute("""
    INSERT INTO system_state (key, value, updated_at)
    VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET
        value=excluded.value,
        updated_at=excluded.updated_at
    """, (key, value, datetime.now(timezone.utc).isoformat()))


def _short_model_error(error: str) -> str:
    compact = re.sub(r"\s+", " ", str(error or "unknown error")).strip()
    status_match = re.search(r"HTTP\s+(\d+)", compact, re.IGNORECASE)
    message_match = re.search(r'"message"\s*:\s*"([^"]+)"', compact)
    if status_match and message_match:
        return f"HTTP {status_match.group(1)}：{message_match.group(1)}"[:220]
    return compact[:220]


def _build_model_alert(title: str, details: list[str]) -> str:
    bj_time = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    detail_text = "\n".join(f"• {line}" for line in details)
    return f"""{title}
───────────────────────
时间：{bj_time}
{detail_text}

调用顺序：{PRIMARY_MODEL} → {FALLBACK_MODEL}
说明：仅在模型接口状态发生变化时提醒，不会重复发送相同故障。"""


def _save_model_health_and_alert(primary_state: str, fallback_state: str, alert_message: str = ""):
    """Persist health transitions and queue any alert atomically."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        _upsert_system_state(cur, PRIMARY_HEALTH_KEY, primary_state)
        _upsert_system_state(cur, FALLBACK_HEALTH_KEY, fallback_state)
        if alert_message:
            row = cur.execute(
                "SELECT value FROM system_state WHERE key = ?", (MODEL_ALERT_PENDING_KEY,)
            ).fetchone()
            pending = row[0] if row else ""
            combined = f"{pending}\n\n{alert_message}".strip() if pending else alert_message
            _upsert_system_state(cur, MODEL_ALERT_PENDING_KEY, combined[-3500:])
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[State Error] Failed to persist model health: {e}", flush=True)


def flush_pending_model_alert():
    message = get_system_state(MODEL_ALERT_PENDING_KEY)
    if not message:
        return
    print("[MODEL ALERT] Sending queued model interface status notification...", flush=True)
    ok, err = send_weixin_direct_call(message)
    if not ok:
        print(f"[MODEL ALERT Error] Delivery failed and will retry: {err}", flush=True)
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM system_state WHERE key = ?", (MODEL_ALERT_PENDING_KEY,))
        conn.commit()
        conn.close()
        print("[MODEL ALERT] Notification delivered successfully.", flush=True)
    except Exception as e:
        print(f"[State Warning] Alert sent but pending state cleanup failed: {e}", flush=True)


def record_model_outcome(
    primary_ok: bool,
    primary_error: str = "",
    fallback_attempted: bool = False,
    fallback_ok: bool | None = None,
    fallback_error: str = "",
):
    """Send transition-only alerts for primary/fallback failure and recovery."""
    previous_primary = get_system_state(PRIMARY_HEALTH_KEY, "unknown")
    previous_fallback = get_system_state(FALLBACK_HEALTH_KEY, "unknown")
    new_primary = "healthy" if primary_ok else "unhealthy"
    new_fallback = previous_fallback
    alert_message = ""

    if primary_ok:
        if previous_primary == "unhealthy":
            alert_message = _build_model_alert(
                "✅ 模型接口恢复提醒｜主模型已恢复",
                [
                    f"主模型 {PRIMARY_MODEL} 已恢复正常，后续研判已自动切回主模型。",
                    f"备用模型 {FALLBACK_MODEL} 保持待命。",
                ],
            )
    elif fallback_attempted and fallback_ok is True:
        new_fallback = "healthy"
        details = []
        if previous_primary != "unhealthy":
            details.extend([
                f"主模型 {PRIMARY_MODEL} 调用失败：{_short_model_error(primary_error)}",
                f"已切换至备用模型 {FALLBACK_MODEL}，当前接管成功。",
            ])
        if previous_fallback == "unhealthy":
            details.append(f"备用模型 {FALLBACK_MODEL} 已恢复正常。")
        if details:
            title = (
                "✅ 模型接口恢复提醒｜备用模型已恢复并接管"
                if previous_primary == "unhealthy" and previous_fallback == "unhealthy"
                else "⚠️ 模型接口切换提醒｜主模型已切换至备用"
            )
            alert_message = _build_model_alert(title, details)
    elif fallback_attempted and fallback_ok is False:
        new_fallback = "unhealthy"
        if previous_primary != "unhealthy" or previous_fallback != "unhealthy":
            details = []
            if previous_primary != "unhealthy":
                details.append(f"主模型 {PRIMARY_MODEL} 调用失败：{_short_model_error(primary_error)}")
            details.append(f"备用模型 {FALLBACK_MODEL} 调用失败：{_short_model_error(fallback_error)}")
            details.append("主、备用模型当前均不可用，本轮新闻研判已安全终止，不会推送未研判内容。")
            title = (
                "🚨 模型接口故障提醒｜主备模型均失败"
                if previous_primary != "unhealthy"
                else "🚨 模型接口故障提醒｜备用模型也已失败"
            )
            alert_message = _build_model_alert(title, details)

    _save_model_health_and_alert(new_primary, new_fallback, alert_message)
    flush_pending_model_alert()

def evaluate_and_summarize(title: str, snippet: str, source: str) -> dict:
    """
    Evaluates news significance (0-100) with active incident awareness.
    """
    recent_incidents = get_active_incidents_summary()
    recent_context = ""
    if recent_incidents:
        recent_context = "【近期已推送的重大事件（4小时内，请防止重复）】：\n"
        for inc in recent_incidents:
            recent_context += f"- [{inc['elapsed_mins']}分钟前] {inc['location']} | {inc['level']} | 标题: {inc['title']} (得分:{inc['score']})\n"
    
    prompt = f"""你是一个顶尖全球重大新闻与突发事件研判专家。
请根据以下新闻线索进行分析评估：
【来源】：{source}
【标题】：{title}
【内容/摘要】：{snippet}

{recent_context}

研判标准：
1. 重大突发标准（得分 80-100 分）：
   - 特大自然灾害（特大地震、泥石流、山洪、破坏性海啸、重大台风飓风）
   - 严重人员伤亡事故（死伤失联人数较多、重大坍塌/特大交通事故）
   - 重大国家安全/战争冲突（开火、突发空袭、政变、军事突发行动）
   - 顶级政经与科技震荡（全球核心市场剧变、关键断供、关键领导人突发变故）
2. 普通新闻、日常言论、娱乐、体育、常规财报：得分低于 75 分。
3. 【关键防重复规则】：若该新闻仅仅是上述【近期已推送事件】的后续工作进展、例行会议、保险理赔、一般性增援且**未发生伤亡人数剧变**，必须设置 "is_followup_of_recent": true，且 score 不得高于 75 分！

请严格输出 JSON 格式（不要输出 markdown 代码块标记以外的多余文字）：
{{
  "score": 85,
  "is_breaking": true,
  "is_followup_of_recent": false,
  "level": "特大自然灾害",
  "verified_status": "海外主流媒体首发·待官方通报 / 官方权威已确认 / 多源已证实",
  "location": "西藏日喀则吉隆县",
  "time_str": "发生时间（若已知）",
  "core_facts": "核心事实一句话说明",
  "impact": "人员伤亡、交通/基础设施或区域影响",
  "summary": "100字左右的高信息密度中文精炼摘要"
}}
"""
    model_used = None
    fallback_used = False
    result_text = None
    # Step 1: Primary - Gemini 3.7 Flash High through host CLIProxyAPI
    ok, res = call_cliproxy(PRIMARY_MODEL, prompt)
    if ok:
        record_model_outcome(primary_ok=True)
        model_used = "Gemini 3.7 Flash High"
        result_text = res
    else:
        print(f"[Fallback] Gemini 3.7 Flash failed: {res}")
        primary_error = res
        # Step 2: First and only fallback - Codex GPT Luna through the same gateway
        ok, res = call_cliproxy(FALLBACK_MODEL, prompt)
        if ok:
            record_model_outcome(
                primary_ok=False,
                primary_error=primary_error,
                fallback_attempted=True,
                fallback_ok=True,
            )
            model_used = "GPT-5.6 Luna"
            fallback_used = True
            result_text = res
        else:
            record_model_outcome(
                primary_ok=False,
                primary_error=primary_error,
                fallback_attempted=True,
                fallback_ok=False,
                fallback_error=res,
            )
            print(f"[Error] Both configured models failed! GPT Luna error: {res}")
            return {"score": 0, "error": "all_models_failed"}

    # Extract JSON
    try:
        clean_json = result_text.strip()
        if "{" in clean_json and "}" in clean_json:
            clean_json = clean_json[clean_json.find("{"):clean_json.rfind("}")+1]
        data = json.loads(clean_json.strip())
        data["model_used"] = model_used
        data["fallback_used"] = fallback_used
        return data
    except Exception as e:
        print(f"[Parse Error] Failed to parse model output: {e}\nRaw output: {result_text}")
        return {"score": 0, "error": f"json_parse_error: {e}"}

# ----------------- WeChat Formatter & Outbox Dispatcher -----------------
def format_wechat_card(news_meta: dict, analysis: dict) -> str:
    title = news_meta.get("title", "")
    source = news_meta.get("source", "未知信源")
    
    score = analysis.get("score", 0)
    level = analysis.get("level", "重大突发")
    status = analysis.get("verified_status", "待核实")
    location = analysis.get("location", "未知")
    time_str = analysis.get("time_str", "")
    core_facts = analysis.get("core_facts", title)
    impact = analysis.get("impact", "尚在评估中")
    summary = analysis.get("summary", "")
    
    now = datetime.now()
    if not time_str or "已知" in time_str or time_str == "刚刚":
        time_display = now.strftime("%Y-%m-%d %H:%M")
        event_time = time_str if time_str else now.strftime("%Y-%m-%d %H:%M")
    else:
        event_time = time_str
        time_display = time_str

    msg = f"""🚨 全球重大突发快讯｜{level}
───────────────────────
【事件地点】{location}
【发生时间】{event_time}
【评估等级】{score} / 100（重大突发）
【核实状态】{status}

📌 核心事实：
{core_facts}

⚠️ 影响与伤亡：
{impact}

📝 精炼综述：
{summary}

来源：{source}（{time_display}）"""

    return msg

def send_weixin_direct_call(message: str) -> tuple[bool, str]:
    try:
        import asyncio
        sys.path.insert(0, "/opt/hermes-src")
        
        token = os.environ.get("WEIXIN_TOKEN", "")
        env_path = "/home/yinsheng/.hermes/.env"
        if not token and os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    if line.strip().startswith("WEIXIN_TOKEN="):
                        token = line.strip().split("=", 1)[1].strip("\"'")
                        
        extra = {"account_id": "DEFAULT"}
        from gateway.platforms.weixin import send_weixin_direct
        
        res = asyncio.run(send_weixin_direct(
            extra=extra,
            token=token,
            chat_id=TARGET_WEIXIN,
            message=message
        ))
        if res.get("success"):
            return True, "success"
        return False, str(res.get("error", "unknown_error"))
    except Exception as e:
        return False, str(e)

def flush_outbox():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    SELECT hash, source, title, wechat_msg, retry_count, last_retry_at
    FROM processed_items
    WHERE push_status = 1
    ORDER BY discovered_at ASC
    LIMIT 5
    """)
    pending = cur.fetchall()
    conn.close()

    if not pending:
        return

    now_utc = datetime.now(timezone.utc)
    for row in pending:
        item_hash, source, title, msg, retry_cnt, last_retry_str = row
        if not msg:
            continue
            
        if last_retry_str:
            try:
                last_retry_dt = datetime.fromisoformat(last_retry_str)
                delay_needed = min(600, 30 * (2 ** min(retry_cnt, 5)))
                if (now_utc - last_retry_dt).total_seconds() < delay_needed:
                    continue
            except Exception:
                pass

        print(f"\n[PUSH QUEUE] Attempting delivery (retry #{retry_cnt}): {title[:40]}...", flush=True)
        ok, err = send_weixin_direct_call(msg)
        
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        now_str = datetime.now(timezone.utc).isoformat()
        if ok:
            print(f"[PUSH SUCCESS] Alert delivered successfully: {title[:40]}", flush=True)
            cur.execute("""
            UPDATE processed_items
            SET push_status = 2, is_pushed = 1, last_retry_at = ?
            WHERE hash = ?
            """, (now_str, item_hash))
        else:
            new_cnt = retry_cnt + 1
            print(f"[PUSH ERROR] Send failed (attempt #{new_cnt}): {err}", flush=True)
            if new_cnt >= 5:
                print(f"[PUSH ABORT] Max retries (5) exceeded for: {title[:40]}", flush=True)
                cur.execute("""
                UPDATE processed_items
                SET push_status = 3, retry_count = ?, last_retry_at = ?
                WHERE hash = ?
                """, (new_cnt, now_str, item_hash))
            else:
                cur.execute("""
                UPDATE processed_items
                SET retry_count = ?, last_retry_at = ?
                WHERE hash = ?
                """, (new_cnt, now_str, item_hash))
        conn.commit()
        conn.close()
        time.sleep(1)

# ----------------- Concurrent Fetchers -----------------
def fetch_rss_single(name: str, cfg: dict) -> list[dict]:
    url = cfg["url"]
    items = []
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read()
            root = ET.fromstring(data)
            for el in root.findall(".//item")[:15]:
                t = el.find("title")
                l = el.find("link")
                d = el.find("description")
                p = el.find("pubDate")
                title = t.text.strip() if t is not None and t.text else ""
                link = l.text.strip() if l is not None and l.text else ""
                snippet = d.text.strip() if d is not None and d.text else ""
                pub = p.text.strip() if p is not None and p.text else ""
                if title:
                    h = hashlib.sha256(f"{name}:{title}".encode("utf-8")).hexdigest()[:16]
                    items.append({
                        "hash": h,
                        "source": name,
                        "title": title,
                        "link": link,
                        "snippet": snippet[:300],
                        "published_at": pub
                    })
    except Exception as e:
        print(f"[Fetch Error] RSS {name} failed: {e}", flush=True)
    return items

def fetch_usgs_single(name: str, cfg: dict) -> list[dict]:
    url = cfg["url"]
    items = []
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            for f in data.get("features", [])[:10]:
                props = f.get("properties", {})
                mag = props.get("mag", 0)
                place = props.get("place", "Unknown")
                time_ms = props.get("time", 0)
                url_quake = props.get("url", "")
                
                coords = f.get("geometry", {}).get("coordinates", [0, 0])
                lon, lat = coords[0], coords[1]
                is_near_china = (70 <= lon <= 135) and (15 <= lat <= 55)
                
                if mag < 6.0 and not is_near_china:
                    continue
                
                title = f"USGS 全球地震速报: M {mag} 级地震 - {place}"
                snippet = f"震级: M {mag}, 震源地点: {place}, 发生时间: {datetime.fromtimestamp(time_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
                h = hashlib.sha256(f"usgs:{f.get('id')}:{mag}".encode("utf-8")).hexdigest()[:16]
                items.append({
                    "hash": h,
                    "source": "USGS 地质调查局",
                    "title": title,
                    "link": url_quake,
                    "snippet": snippet,
                    "published_at": str(time_ms)
                })
    except Exception as e:
        print(f"[Fetch Error] USGS failed: {e}", flush=True)
    return items

# Fast keyword heuristic before calling LLM
BREAKING_KEYWORDS = [
    "泥石流", "特大", "地震", "山洪", "暴雨红色预警", "伤亡", "失联", "坍塌", "遇难",
    "爆炸", "坠毁", "交火", "袭击", "宣战", "空袭", "政变", "紧急状态", "重大事故",
    "海啸", "山火", "枪击", "决堤", "沉没",
    "earthquake", "landslide", "explosion", "tsunami", "casualt", "fatalit",
    "airstrike", "missile", "evacuat", "emergency declared", "wildfire", "shooting", "plane crash"
]

def quick_filter(title: str, snippet: str) -> bool:
    content = (title + " " + snippet).lower()
    for kw in BREAKING_KEYWORDS:
        if kw.lower() in content:
            return True
    return False

def fetch_all_sources_concurrently() -> list[dict]:
    all_items = []
    start_t = time.time()
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_name = {}
        for name, cfg in SOURCES.items():
            if cfg["type"] == "geojson_usgs":
                f = executor.submit(fetch_usgs_single, name, cfg)
            else:
                f = executor.submit(fetch_rss_single, name, cfg)
            future_to_name[f] = name
            
        for future in as_completed(future_to_name):
            try:
                res = future.result()
                all_items.extend(res)
            except Exception as e:
                name = future_to_name[future]
                print(f"[Fetch Exception] Source {name} raised: {e}", flush=True)
                
    elapsed = time.time() - start_t
    print(f"[*] Fetched {len(all_items)} raw items across {len(SOURCES)} sources in {elapsed:.2f}s.", flush=True)
    return all_items

# ----------------- Maintenance & Housekeeping -----------------
def cleanup_old_records(retention_days: int = 14, max_records: int = 50000):
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""
        DELETE FROM processed_items
        WHERE is_pushed = 0 
        AND julianday('now') - julianday(discovered_at) > ?
        """, (retention_days,))
        
        cur.execute("""
        DELETE FROM active_incidents
        WHERE julianday('now') - julianday(last_pushed_at) > 3
        """)

        cur.execute("SELECT COUNT(*) FROM processed_items")
        total = cur.fetchone()[0]
        if total > max_records:
            excess = total - max_records
            cur.execute("""
            DELETE FROM processed_items
            WHERE hash IN (
                SELECT hash FROM processed_items
                WHERE is_pushed = 0
                ORDER BY discovered_at ASC
                LIMIT ?
            )
            """, (excess,))
        
        conn.commit()
        cur.execute("PRAGMA optimize;")
        conn.close()
    except Exception as e:
        print(f"[Cleanup Warning] Error during DB cleanup: {e}", flush=True)

def update_heartbeat():
    try:
        with open(HEARTBEAT_PATH, "w") as f:
            f.write(datetime.now(timezone.utc).isoformat())
    except Exception as e:
        print(f"[Heartbeat Error] Failed to write heartbeat: {e}", flush=True)

# ----------------- Main Poll Loop -----------------
def poll_all():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting news polling cycle across {len(SOURCES)} sources...", flush=True)
    
    # 1. Concurrently fetch all feeds
    raw_items = fetch_all_sources_concurrently()
    new_candidates = [it for it in raw_items if not is_item_processed(it["hash"])]
    print(f"[*] Found {len(new_candidates)} new unprocessed items.", flush=True)
    
    # 2. Process new candidate items
    for item in new_candidates:
        title = item["title"]
        snippet = item["snippet"]
        source = item["source"]
        
        is_candidate = quick_filter(title, snippet)
        if not is_candidate:
            record_item(item["hash"], source, title, item["link"], item["published_at"], 
                        score=0, is_pushed=0, push_status=0)
            continue
        
        # LLM Evaluation Chain
        print(f"\n[!] Candidate detected: [{source}] {title[:60]}", flush=True)
        analysis = evaluate_and_summarize(title, snippet, source)
        score = analysis.get("score", 0)
        is_breaking = analysis.get("is_breaking", False)
        is_followup = analysis.get("is_followup_of_recent", False)
        location = analysis.get("location", "未知")
        level = analysis.get("level", "重大突发")
        
        print(f" -> Score: {score} | Breaking: {is_breaking} | Followup: {is_followup} | Model: {analysis.get('model_used')}", flush=True)
        
        if (score >= 80 or is_breaking) and not is_followup:
            # 3. Enhanced Topic Cooldown & Fuzzy Anti-Spam Check
            should_push, reason, incident_key = check_incident_cooldown_enhanced(location, level, title, score, is_followup)
            
            if should_push:
                print(f" -> [PUSH ACCEPTED] Incident '{incident_key}': {reason}", flush=True)
                msg_card = format_wechat_card(item, analysis)
                record_item(item["hash"], source, title, item["link"], item["published_at"],
                            score=score, is_pushed=0, push_status=1, incident_key=incident_key, wechat_msg=msg_card)
            else:
                print(f" -> [PUSH SUPPRESSED] Incident '{incident_key}': {reason}", flush=True)
                record_item(item["hash"], source, title, item["link"], item["published_at"],
                            score=score, is_pushed=0, push_status=0, incident_key=incident_key)
        else:
            record_item(item["hash"], source, title, item["link"], item["published_at"],
                        score=score, is_pushed=0, push_status=0)
            
    # 4. Flush Outbox Queue (Send pending & retry failed alerts)
    flush_outbox()
    
    # 5. Refresh Watchdog Heartbeat
    update_heartbeat()

def main():
    init_db()
    print("=== Global Breaking News Monitoring Service (Production v2) Started ===")
    print(f"Database: {DB_PATH}")
    print(f"Poll Interval: {POLL_INTERVAL_SECONDS}s")
    print(f"Heartbeat Path: {HEARTBEAT_PATH}")
    print(f"Model Chain: {PRIMARY_MODEL} -> {FALLBACK_MODEL} (via {CLIPROXY_BASE_URL})")
    cycle_count = 0
    while True:
        try:
            poll_all()
            cycle_count += 1
            if cycle_count % 20 == 0:
                cleanup_old_records(retention_days=14, max_records=50000)
        except Exception as e:
            print(f"[Loop Exception] Unhandled error: {e}", flush=True)
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
