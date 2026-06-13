#!/usr/bin/env python3
"""
BTC-only sandbox runner driven by periodic multi-model directional calls.
"""

import csv
import hashlib
import json
import os
import re
import signal
import socket
import threading
import time
import textwrap
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_IMPORT_ERROR = ""
except Exception as exc:  # pragma: no cover
    Image = None
    ImageDraw = None
    ImageFont = None
    PIL_IMPORT_ERROR = str(exc)

try:
    import websocket
    WEBSOCKET_IMPORT_ERROR = ""
except Exception as exc:  # pragma: no cover
    websocket = None
    WEBSOCKET_IMPORT_ERROR = str(exc)

try:
    from rich import box
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.rule import Rule
    from rich.table import Table
    from rich.text import Text
except Exception:  # pragma: no cover
    class _SimpleBox:
        SIMPLE_HEAD = None

    box = _SimpleBox()

    class Text:
        def __init__(self, text="", style=None):
            self.text = str(text)
            self.style = style

        def __str__(self):
            return self.text

    class Table:
        def __init__(self, *args, **kwargs):
            self.rows = []
            self.columns = []

        @classmethod
        def grid(cls, *args, **kwargs):
            return cls()

        def add_column(self, *args, **kwargs):
            self.columns.append(args[0] if args else "")

        def add_row(self, *values):
            self.rows.append([str(value) for value in values])

        def __str__(self):
            lines = []
            if self.columns:
                lines.append(" | ".join(str(col) for col in self.columns))
            for row in self.rows:
                lines.append(" | ".join(row))
            return "\n".join(lines)

    class Rule:
        def __init__(self, title="", style=None):
            self.title = title

        def __str__(self):
            return f"--- {self.title} ---"

    class Group:
        def __init__(self, *items):
            self.items = items

        def __str__(self):
            return "\n".join(str(item) for item in self.items)

    class Panel:
        def __init__(self, renderable, title="", border_style=None, box=None, expand=True):
            self.renderable = renderable
            self.title = title

        def __str__(self):
            title = f"[{self.title}]\n" if self.title else ""
            return f"{title}{self.renderable}"

    class Console:
        def print(self, *args, **kwargs):
            print(*args)

    class Live:
        def __init__(self, renderable, console=None, refresh_per_second=1, screen=False):
            self.renderable = renderable

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def update(self, renderable):
            self.renderable = renderable


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SETUP_DIR = os.path.join(BASE_DIR, "setup")
HISTORY_DIR = os.path.join(BASE_DIR, "history")
LEGACY_SETTINGS_PATH = os.path.join(BASE_DIR, "settings.txt")
LEGACY_STATE_PATH = os.path.join(BASE_DIR, "state.txt")
SETTINGS_PATH = os.path.join(SETUP_DIR, "settings.txt")
TRADES_CSV_PATH = os.path.join(HISTORY_DIR, "trades.csv")
AI_RESPONSES_CSV_PATH = os.path.join(HISTORY_DIR, "ai_responses.csv")
STRATEGY_RETURNS_CSV_PATH = os.path.join(HISTORY_DIR, "strategy_returns.csv")
STATE_PATH = os.path.join(SETUP_DIR, "state.txt")
SNAPSHOT_DIR = r"G:\My Drive\+tradebot"
PANEL_BORDER_STYLE = "rgb(237,125,175)"
HEADING_STYLE = "bold cyan"
BODY_STYLE = "white"
BTC_PERP = "BTC"
HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
FEAR_GREED_URL = "https://api.alternative.me/fng/?limit=1&format=json"
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
CLAUDE_MESSAGES_URL = "https://api.anthropic.com/v1/messages"
PERPLEXITY_SONAR_URL = "https://api.perplexity.ai/v1/sonar"
GROK_RESPONSES_URL = "https://api.x.ai/v1/responses"
OPENAI_MODEL_DEFAULT = "gpt-4.1"
GEMINI_MODEL_DEFAULT = "gemini-2.5-flash-lite"
CLAUDE_MODEL_DEFAULT = "claude-3-5-haiku-latest"
PERPLEXITY_MODEL_DEFAULT = "sonar"
GROK_MODEL_DEFAULT = "grok-4-fast"
STARTING_BALANCE_USDC = 10000.0
STACK_FRACTION = 0.975
LEVERAGE = 10.0
TAKER_FEE_PCT = 0.015
STOP_LOSS_USDC = 250.0
SIGNAL_INTERVAL_SECONDS = 15 * 60
DISPLAY_COLUMNS = 15
DISPLAY_MINUTE_SECONDS = 60
PRICE_HISTORY_SECONDS = (DISPLAY_COLUMNS + 2) * DISPLAY_MINUTE_SECONDS
FEED_STALE_AFTER_SECONDS = 20.0
LIVE_REFRESH_HZ = 0.5
LIVE_SCREEN = False
OPENAI_TIMEOUT_SECONDS = 90
OPENAI_MAX_ATTEMPTS = 3
OPENAI_RETRY_DELAY_SECONDS = 3
SIGNAL_RETRY_DELAY_SECONDS = 30
STARTUP_SIGNAL_RETRY_SECONDS = 5
STATE_SAVE_INTERVAL_SECONDS = 3
SNAPSHOT_LEAD_SECONDS = 20
LATEST_CHANGE_SUMMARY = "Tape uses time stamps; final column shows live"
FEAR_GREED_LONG_THRESHOLD = 30
FEAR_GREED_SHORT_THRESHOLD = 70
PROMPT = """I am trading on the Hyperliquid BTC Perpetual market using taker orders and my fees are 0.015% and 0.015% each way, so I need to clear 0.03% on any trade to make profit.

Decide the single best directional trade for the next 15 minutes using only the BTC market snapshot and sentiment metrics included in this prompt. Treat the supplied data as the full evidence set.

Priority order:
1. Immediate BTC market structure and momentum.
2. Order book pressure, spread, and short-term range positioning.
3. Background sentiment metrics explicitly included in the prompt, such as Fear & Greed or StockGeist if present.

Hard rules:
- Do not introduce ETF flows, Federal Reserve decisions, macro commentary, external news, or any other information unless it is explicitly included in this prompt.
- Do not rely on outside knowledge, assumed headlines, or guessed context.
- If the supplied data does not support a measurable edge after fees, return NO_TRADE.
- Do not default to NO_TRADE just because confidence is imperfect. If one side has the clearest edge from the supplied data, choose it.
- In the why field, cite the actual supplied metrics by name and value wherever possible.

Return valid JSON only with this exact shape:
{"signal":"LONG|SHORT|NO_TRADE","why":"1-3 short sentences using the supplied metrics only","sources":["up to 3 short source strings drawn only from the supplied prompt context"]}"""
VALID_SIGNALS = {"LONG", "SHORT", "NO_TRADE"}
AI_PROVIDER_ORDER = ("gemini", "openai", "claude", "perplexity", "grok")
console = Console()


def now_ts():
    return time.time()


def format_ts(ts):
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")


def format_minute_stamp(ts):
    if not ts:
        return "--:--"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M")


def iso_utc(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def pct_change(from_price, to_price):
    if from_price <= 0:
        return 0.0
    return ((to_price - from_price) / from_price) * 100.0


def apply_fee(notional, fee_pct):
    return float(notional) * (float(fee_pct) / 100.0)


def post_json(url, payload, timeout, headers=None, return_raw=False):
    body = json.dumps(payload).encode("utf-8")
    request_headers = {"Content-Type": "application/json"}
    if headers:
        request_headers.update(headers)
    req = urllib_request.Request(url, data=body, headers=request_headers, method="POST")
    try:
        with urllib_request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except (socket.timeout, TimeoutError) as exc:
        raise RuntimeError(f"Timeout after {timeout}s") from exc
    except urllib_error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {raw}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Network error: {exc}") from exc
    parsed = json.loads(raw)
    if return_raw:
        return parsed, raw
    return parsed


def read_json_response(url, payload=None, headers=None, timeout=90, return_raw=False):
    request_headers = {"Content-Type": "application/json"}
    if headers:
        request_headers.update(headers)
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib_request.Request(
        url,
        data=body,
        headers=request_headers,
        method="POST" if body is not None else "GET",
    )
    try:
        with urllib_request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except (socket.timeout, TimeoutError) as exc:
        raise RuntimeError(f"Timeout after {timeout}s") from exc
    except urllib_error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {raw}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Network error: {exc}") from exc
    parsed = json.loads(raw)
    if return_raw:
        return parsed, raw
    return parsed


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def round_or_none(value, digits=4):
    if value is None:
        return None
    return round(float(value), digits)


def clamp(value, low, high):
    return max(low, min(high, value))


def compute_window_metrics(candles, count):
    if not candles:
        return {"ret_pct": 0.0, "range_pct": 0.0, "high": 0.0, "low": 0.0}
    window = candles[-count:] if len(candles) >= count else candles[:]
    if not window:
        return {"ret_pct": 0.0, "range_pct": 0.0, "high": 0.0, "low": 0.0}
    first_close = safe_float(window[0]["c"])
    last_close = safe_float(window[-1]["c"])
    high = max(safe_float(item["h"]) for item in window)
    low = min(safe_float(item["l"]) for item in window)
    range_pct = pct_change(low, high) if low > 0 else 0.0
    return {
        "ret_pct": pct_change(first_close, last_close),
        "range_pct": range_pct,
        "high": high,
        "low": low,
    }


def price_position(current_price, low, high):
    if high <= low:
        return 0.5
    return clamp((current_price - low) / (high - low), 0.0, 1.0)


def fetch_hyperliquid_snapshot():
    now_ms = int(time.time() * 1000)
    mids = read_json_response(
        HYPERLIQUID_INFO_URL,
        payload={"type": "allMids"},
        timeout=15,
    )
    book = read_json_response(
        HYPERLIQUID_INFO_URL,
        payload={"type": "l2Book", "coin": "BTC"},
        timeout=15,
    )
    candles_1m = read_json_response(
        HYPERLIQUID_INFO_URL,
        payload={
            "type": "candleSnapshot",
            "req": {
                "coin": "BTC",
                "interval": "1m",
                "startTime": now_ms - (65 * 60 * 1000),
                "endTime": now_ms,
            },
        },
        timeout=20,
    )
    candles_1h = read_json_response(
        HYPERLIQUID_INFO_URL,
        payload={
            "type": "candleSnapshot",
            "req": {
                "coin": "BTC",
                "interval": "1h",
                "startTime": now_ms - (26 * 60 * 60 * 1000),
                "endTime": now_ms,
            },
        },
        timeout=20,
    )
    levels = book.get("levels") or [[], []]
    bids = levels[0] if len(levels) > 0 else []
    asks = levels[1] if len(levels) > 1 else []
    best_bid = safe_float(bids[0]["px"]) if bids else 0.0
    best_ask = safe_float(asks[0]["px"]) if asks else 0.0
    mid = safe_float(mids.get("BTC")) if isinstance(mids, dict) else 0.0
    if mid <= 0 and best_bid > 0 and best_ask > 0:
        mid = (best_bid + best_ask) / 2.0
    spread_bps = (((best_ask - best_bid) / mid) * 10000.0) if mid > 0 and best_ask >= best_bid else 0.0
    bid5 = sum(safe_float(level.get("sz")) for level in bids[:5])
    ask5 = sum(safe_float(level.get("sz")) for level in asks[:5])
    imbalance = (bid5 / (bid5 + ask5)) if (bid5 + ask5) > 0 else 0.5

    metrics_1m = compute_window_metrics(candles_1m, 1)
    metrics_5m = compute_window_metrics(candles_1m, 5)
    metrics_15m = compute_window_metrics(candles_1m, 15)
    metrics_1h = compute_window_metrics(candles_1m, 60)
    metrics_4h = compute_window_metrics(candles_1h, 4)
    metrics_day = compute_window_metrics(candles_1h, 24)

    return {
        "ts_utc": iso_utc(now_ts()),
        "source": "Hyperliquid BTC perp",
        "px": {
            "mid": round_or_none(mid, 2),
            "bid": round_or_none(best_bid, 2),
            "ask": round_or_none(best_ask, 2),
            "spr_bps": round_or_none(spread_bps, 3),
        },
        "ret_pct": {
            "1m": round_or_none(metrics_1m["ret_pct"]),
            "5m": round_or_none(metrics_5m["ret_pct"]),
            "15m": round_or_none(metrics_15m["ret_pct"]),
            "1h": round_or_none(metrics_1h["ret_pct"]),
            "4h": round_or_none(metrics_4h["ret_pct"]),
        },
        "rng_pct": {
            "1m": round_or_none(metrics_1m["range_pct"]),
            "5m": round_or_none(metrics_5m["range_pct"]),
            "15m": round_or_none(metrics_15m["range_pct"]),
            "1h": round_or_none(metrics_1h["range_pct"]),
            "4h": round_or_none(metrics_4h["range_pct"]),
        },
        "pos": {
            "1h": round_or_none(price_position(mid, metrics_1h["low"], metrics_1h["high"]), 3),
            "4h": round_or_none(price_position(mid, metrics_4h["low"], metrics_4h["high"]), 3),
            "1d": round_or_none(price_position(mid, metrics_day["low"], metrics_day["high"]), 3),
        },
        "book": {
            "bid5": round_or_none(bid5, 4),
            "ask5": round_or_none(ask5, 4),
            "imb": round_or_none(imbalance, 3),
        },
        "levels": {
            "h1_high": round_or_none(metrics_1h["high"], 2),
            "h1_low": round_or_none(metrics_1h["low"], 2),
            "d1_high": round_or_none(metrics_day["high"], 2),
            "d1_low": round_or_none(metrics_day["low"], 2),
        },
    }


def fetch_btc_price_near_ts(target_ts, window_seconds=120):
    target_ts = float(target_ts or 0.0)
    if target_ts <= 0:
        return 0.0
    target_ms = int(target_ts * 1000)
    window_ms = max(60_000, int(window_seconds * 1000))
    candles = read_json_response(
        HYPERLIQUID_INFO_URL,
        payload={
            "type": "candleSnapshot",
            "req": {
                "coin": BTC_PERP,
                "interval": "1m",
                "startTime": target_ms - window_ms,
                "endTime": target_ms + window_ms,
            },
        },
        timeout=15,
    )
    best_price = 0.0
    best_delta = None
    for candle in candles or []:
        candle_ts = safe_float(candle.get("t"), 0.0) / 1000.0
        close_price = safe_float(candle.get("c"), 0.0)
        if candle_ts <= 0 or close_price <= 0:
            continue
        delta = abs(candle_ts - target_ts)
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_price = close_price
    return best_price


def format_commit_ts(iso_text):
    if not iso_text:
        return "unknown"
    try:
        dt = datetime.fromisoformat(str(iso_text).replace("Z", "+00:00"))
    except Exception:
        return str(iso_text)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def get_local_build_info():
    file_path = os.path.abspath(__file__)
    try:
        raw = Path(file_path).read_bytes()
        digest = hashlib.sha256(raw).hexdigest()[:8]
        modified_at = format_commit_ts(
            datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc).isoformat()
        )
        return {
            "label": digest,
            "modified_at": modified_at,
            "file_path": file_path,
        }
    except Exception as exc:
        return {
            "label": "unknown",
            "modified_at": f"unavailable ({exc})",
            "file_path": file_path,
        }


def set_terminal_title(title):
    clean_title = str(title).replace("\n", " ").replace("\r", " ")
    try:
        if os.name == "nt":
            os.system(f"title {clean_title}")
        else:
            print(f"\33]0;{clean_title}\a", end="", flush=True)
    except Exception:
        pass


def dashboard_title_text():
    return f"muzz.world | Latest changes: {LATEST_CHANGE_SUMMARY}"


def render_dashboard_text(trader, market, width=160):
    renderable = build_dashboard(trader, market)
    try:
        capture_console = Console(record=True, width=width)
        capture_console.print(renderable)
        export_text = getattr(capture_console, "export_text", None)
        if callable(export_text):
            return export_text(styles=False)
    except Exception:
        pass
    return str(renderable)


def write_text_image(text, output_path):
    if Image is None or ImageDraw is None or ImageFont is None:
        raise RuntimeError(f"Pillow unavailable: {PIL_IMPORT_ERROR or 'not installed'}")
    font = ImageFont.load_default()
    lines = text.splitlines() or [""]
    dummy_image = Image.new("RGB", (16, 16), color=(0, 0, 0))
    drawer = ImageDraw.Draw(dummy_image)
    line_height = 18
    max_width = 0
    for line in lines:
        bbox = drawer.textbbox((0, 0), line, font=font)
        max_width = max(max_width, bbox[2] - bbox[0])
        line_height = max(line_height, (bbox[3] - bbox[1]) + 4)
    padding = 16
    width = max(800, max_width + (padding * 2))
    height = max(200, (line_height * len(lines)) + (padding * 2))
    image = Image.new("RGB", (width, height), color=(0, 0, 0))
    draw = ImageDraw.Draw(image)
    y = padding
    for line in lines:
        draw.text((padding, y), line, font=font, fill=(235, 235, 235))
        y += line_height
    image.save(output_path, format="PNG")


def _collect_string_candidates(value, candidates):
    if isinstance(value, str):
        candidates.append(value)
        return
    if isinstance(value, dict):
        for nested in value.values():
            _collect_string_candidates(nested, candidates)
        return
    if isinstance(value, list):
        for nested in value:
            _collect_string_candidates(nested, candidates)


def _extract_json_object(text):
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z0-9_]*\n?", "", stripped)
        stripped = re.sub(r"\n?```$", "", stripped)
        stripped = stripped.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped
    match = re.search(r"\{[\s\S]*\}", stripped)
    if match:
        return match.group(0)
    return ""


def extract_signal_response(payload):
    candidates = []
    _collect_string_candidates(payload, candidates)

    for text in candidates:
        blob = _extract_json_object(text)
        if not blob:
            continue
        try:
            parsed = json.loads(blob)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        signal_value = str(parsed.get("signal", "")).strip().upper()
        if signal_value not in VALID_SIGNALS:
            continue
        why = str(parsed.get("why", "")).strip()
        raw_sources = parsed.get("sources", [])
        if isinstance(raw_sources, list):
            sources = [str(item).strip() for item in raw_sources if str(item).strip()]
        elif isinstance(raw_sources, str) and raw_sources.strip():
            sources = [raw_sources.strip()]
        else:
            sources = []
        return {
            "signal": signal_value,
            "why": why,
            "sources": sources[:3],
        }

    for text in candidates:
        upper = text.upper().replace("`", " ")
        match = re.search(r"\b(NO_TRADE|LONG|SHORT)\b", upper)
        if match:
            return {
                "signal": match.group(1),
                "why": "",
                "sources": [],
            }
    return {
        "signal": "",
        "why": "",
        "sources": [],
    }


def load_settings(path):
    settings = {}
    if not os.path.exists(path):
        return settings
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        settings[key.strip()] = value.strip()
    return settings


def resolve_settings_path():
    if os.path.exists(SETTINGS_PATH):
        return SETTINGS_PATH
    if os.path.exists(LEGACY_SETTINGS_PATH):
        return LEGACY_SETTINGS_PATH
    return SETTINGS_PATH


def fear_greed_to_signal(value_text):
    try:
        value = int(str(value_text).strip())
    except Exception:
        return ""
    if value <= FEAR_GREED_LONG_THRESHOLD:
        return "LONG"
    if value >= FEAR_GREED_SHORT_THRESHOLD:
        return "SHORT"
    return "NO_TRADE"


def fetch_fear_greed():
    try:
        data = read_json_response(FEAR_GREED_URL, None, timeout=15)
    except Exception:
        return {"value": "", "classification": "", "signal": ""}
    rows = data.get("data") or []
    if not rows:
        return {"value": "", "classification": "", "signal": ""}
    item = rows[0] or {}
    value = str(item.get("value", "")).strip()
    return {
        "value": value,
        "classification": str(item.get("value_classification", "")).strip(),
        "signal": fear_greed_to_signal(value),
    }


def build_ai_prompt(snapshot, fear_greed):
    sentiment_context = {
        "fear_greed_value": fear_greed.get("value", ""),
        "fear_greed_classification": fear_greed.get("classification", ""),
        "fear_greed_background_signal": fear_greed.get("signal", ""),
        "note": "Treat Fear & Greed as background BTC sentiment context, not a standalone trading command.",
    }
    return (
        PROMPT
        + "\n\nFresh market snapshot:\n"
        + json.dumps(snapshot, separators=(",", ":"), ensure_ascii=True)
        + "\n\nBackground sentiment metric:\n"
        + json.dumps(sentiment_context, separators=(",", ":"), ensure_ascii=True)
    )


def sanitize_request_url(url):
    if "key=" in url:
        prefix, _, _ = url.partition("key=")
        return prefix + "key=***redacted***"
    return url


def sanitize_request_headers(headers):
    sanitized = dict(headers or {})
    for key in list(sanitized):
        if key.lower() in {"authorization", "x-api-key"}:
            sanitized[key] = "***redacted***"
    return sanitized


def format_ai_response_cell(signal, why, error_text=""):
    signal_text = str(signal or "").strip().upper() or "NO_RESPONSE"
    detail = str(why or error_text or "").strip()
    if not detail:
        return signal_text
    return f"{signal_text} | {detail}"


def wrap_panel_text(value, width=26):
    text = " ".join(str(value or "").strip().split())
    if not text:
        return ""
    return textwrap.fill(text, width=max(16, int(width)))


def market_feed_status(market_state):
    last_message_at = float(market_state.get("last_message_at") or 0.0)
    last_open_at = float(market_state.get("last_open_at") or 0.0)
    last_error = str(market_state.get("last_error") or "").strip()
    if last_message_at > 0:
        feed_age = max(0.0, now_ts() - last_message_at)
        if feed_age <= FEED_STALE_AFTER_SECONDS and not last_error:
            return {
                "label": "HL Feed: LIVE",
                "detail": f"last tick {feed_age:.1f}s ago",
                "style": "bold green",
            }
        return {
            "label": "HL Feed: STALE",
            "detail": f"last tick {feed_age:.1f}s ago",
            "style": "bold yellow",
        }
    if last_open_at > 0 and not last_error:
        return {
            "label": "HL Feed: CONNECTING",
            "detail": f"ws open {format_ts(last_open_at) or 'n/a'}",
            "style": "bold yellow",
        }
    return {
        "label": "HL Feed: DISCONNECTED",
        "detail": last_error or "no market data yet",
        "style": "bold red",
    }


def extract_openai_output_text(response_data):
    top_level = response_data.get("output_text")
    if isinstance(top_level, str) and top_level.strip():
        return top_level.strip()
    parts = []
    for item in response_data.get("output", []) or []:
        for content in item.get("content", []) or []:
            content_type = content.get("type")
            if content_type in {"output_text", "text"}:
                text = content.get("text", "")
                if text:
                    parts.append(str(text))
    return "\n".join(parts).strip()


def extract_gemini_output_text(response_data):
    parts = []
    for candidate in response_data.get("candidates", []) or []:
        content = candidate.get("content") or {}
        for part in content.get("parts", []) or []:
            text = part.get("text", "")
            if text:
                parts.append(str(text))
    return "\n".join(parts).strip()


def extract_claude_output_text(response_data):
    parts = []
    for item in response_data.get("content", []) or []:
        if item.get("type") == "text":
            text = item.get("text", "")
            if text:
                parts.append(str(text))
    return "\n".join(parts).strip()


def extract_chat_completion_text(response_data):
    choices = response_data.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    return str(message.get("content", "") or "").strip()


class BtcMarket:
    def __init__(self, stop_event):
        if websocket is None:
            raise RuntimeError(f"websocket-client unavailable: {WEBSOCKET_IMPORT_ERROR}")
        self.stop_event = stop_event
        self.lock = threading.Lock()
        self.history = deque()
        self.current_mid = 0.0
        self.first_message_at = 0.0
        self.last_message_at = 0.0
        self.last_error = ""
        self.last_open_at = 0.0
        self.ws_app = None
        self.ws_thread = None
        self.loop_thread = None

    def start(self):
        if self.loop_thread and self.loop_thread.is_alive():
            return
        self.loop_thread = threading.Thread(target=self._loop, daemon=True, name="btc-market-loop")
        self.loop_thread.start()

    def stop(self):
        ws_app = self.ws_app
        self.ws_app = None
        if ws_app:
            try:
                ws_app.close()
            except Exception:
                pass
        if self.ws_thread and self.ws_thread.is_alive():
            self.ws_thread.join(timeout=2)

    def _prune_locked(self, ts):
        cutoff = ts - PRICE_HISTORY_SECONDS
        while self.history and self.history[0]["ts"] < cutoff:
            self.history.popleft()

    def _record_mid(self, raw_mid):
        ts = now_ts()
        try:
            mid = float(raw_mid)
        except Exception:
            return
        with self.lock:
            if not self.first_message_at:
                self.first_message_at = ts
            self.current_mid = mid
            self.history.append({"ts": ts, "mid": mid})
            self._prune_locked(ts)
            self.last_message_at = ts
            self.last_error = ""

    def _on_open(self, ws_app):
        with self.lock:
            self.last_open_at = now_ts()
            self.last_error = ""
        ws_app.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))

    def _on_message(self, _ws_app, raw_message):
        try:
            if raw_message == "Websocket connection established.":
                return
            message = json.loads(raw_message)
        except Exception:
            return
        if message.get("channel") != "allMids":
            return
        data = message.get("data") or {}
        mids = data.get("mids") if isinstance(data, dict) and "mids" in data else data
        if not isinstance(mids, dict):
            return
        if BTC_PERP in mids:
            self._record_mid(mids[BTC_PERP])

    def _on_error(self, _ws_app, error):
        with self.lock:
            self.last_error = f"Websocket error: {error}"

    def _on_close(self, _ws_app, status_code, close_msg):
        with self.lock:
            if not self.last_error:
                self.last_error = f"Websocket closed ({status_code}): {close_msg or 'no message'}"

    def _connect(self):
        self.stop()
        self.ws_app = websocket.WebSocketApp(
            HYPERLIQUID_WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self.ws_thread = threading.Thread(
            target=lambda: self.ws_app.run_forever(ping_interval=20, ping_timeout=10),
            daemon=True,
            name="btc-allmids-websocket",
        )
        self.ws_thread.start()

    def _loop(self):
        while not self.stop_event.is_set():
            try:
                self._connect()
                while not self.stop_event.is_set():
                    time.sleep(1)
                    with self.lock:
                        last_message_at = self.last_message_at
                        ws_thread_alive = self.ws_thread.is_alive() if self.ws_thread else False
                    if not ws_thread_alive:
                        raise RuntimeError("Websocket thread exited unexpectedly.")
                    if last_message_at and (now_ts() - last_message_at) > FEED_STALE_AFTER_SECONDS:
                        raise RuntimeError(f"Market data stale ({now_ts() - last_message_at:.1f}s).")
            except Exception as exc:
                with self.lock:
                    self.last_error = str(exc)
            finally:
                self.stop()
            time.sleep(2)

    def get_state(self):
        with self.lock:
            history = list(self.history)
            return {
                "history": history,
                "mid": float(self.current_mid or 0.0),
                "first_message_at": self.first_message_at,
                "last_message_at": self.last_message_at,
                "last_error": self.last_error,
                "last_open_at": self.last_open_at,
            }

    def nearest_price(self, seconds_ago):
        state = self.get_state()
        history = state["history"]
        if not history:
            return None
        target_ts = now_ts() - seconds_ago
        candidate = None
        for item in history:
            if item["ts"] <= target_ts:
                candidate = item
            else:
                break
        if candidate is None:
            return None
        return float(candidate["mid"])

    def nearest_price_to_ts(self, target_ts, max_gap_seconds=None):
        state = self.get_state()
        history = state["history"]
        if not history:
            return None
        target_ts = float(target_ts or 0.0)
        best_item = None
        best_gap = None
        for item in history:
            gap = abs(float(item["ts"]) - target_ts)
            if best_gap is None or gap < best_gap:
                best_item = item
                best_gap = gap
        if best_item is None:
            return None
        if max_gap_seconds is not None and best_gap is not None and best_gap > float(max_gap_seconds):
            return None
        return float(best_item["mid"])

    def get_minute_prices(self):
        prices = []
        for minute in range(DISPLAY_COLUMNS, 0, -1):
            prices.append(self.nearest_price(minute * DISPLAY_MINUTE_SECONDS))
        return prices

    def fetch_book_snapshot(self):
        book = post_json(
            HYPERLIQUID_INFO_URL,
            {"type": "l2Book", "coin": BTC_PERP},
            timeout=3,
        )
        levels = book.get("levels") or [[], []]
        bids = levels[0] if len(levels) > 0 else []
        asks = levels[1] if len(levels) > 1 else []
        if not bids or not asks:
            raise RuntimeError("Order book snapshot missing bids or asks.")
        best_bid = float(bids[0]["px"])
        best_ask = float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2.0
        bid_depth = sum(float(level["sz"]) for level in bids[:5])
        ask_depth = sum(float(level["sz"]) for level in asks[:5])
        spread_pct = ((best_ask - best_bid) / mid) * 100.0 if mid > 0 else 0.0
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid": mid,
            "bid_depth_top5": bid_depth,
            "ask_depth_top5": ask_depth,
            "spread_pct": spread_pct,
            "ts": now_ts(),
        }


class SandboxTrader:
    def __init__(self, market, settings):
        self.market = market
        self.settings = settings
        self.start_time = now_ts()
        self.build_info = get_local_build_info()
        self.available = STARTING_BALANCE_USDC
        self.position = None
        self.trades = deque(maxlen=12)
        self.logs = deque(maxlen=12)
        self.last_signal = "PENDING"
        self.last_signal_why = ""
        self.last_signal_sources = []
        self.last_signal_at = 0.0
        self.next_signal_at = self.start_time
        self.last_signal_error = ""
        self.last_provider_results = {}
        self.last_provider_errors = {}
        self.state_path = STATE_PATH
        self.snapshot_dir = SNAPSHOT_DIR
        self.last_snapshot_key = ""
        self.snapshot_thread = None
        self.last_state_save_at = 0.0
        self.resume_note = ""
        self.lock = threading.Lock()
        os.makedirs(SETUP_DIR, exist_ok=True)
        os.makedirs(HISTORY_DIR, exist_ok=True)
        self.legacy_state_path = LEGACY_STATE_PATH
        self.trades_csv_path = TRADES_CSV_PATH
        self.ai_responses_csv_path = AI_RESPONSES_CSV_PATH
        self.strategy_returns_csv_path = STRATEGY_RETURNS_CSV_PATH
        self.pending_strategy_evaluations = []
        self._ensure_trades_csv()
        self._ensure_strategy_returns_csv()
        self._reset_ai_responses_csv()
        set_terminal_title(dashboard_title_text())
        restored = self._restore_state()
        self.log("BTC sandbox runner started.")
        self.log(
            f"Local build -> {self.build_info['label']} | {self.build_info['modified_at']}"
        )
        self.log(f"Running file -> {self.build_info['file_path']}")
        if restored:
            self.log("Recovered state from state.txt.")
            if self.resume_note:
                self.log(self.resume_note)
        self.persist_state(force=True)

    def _reset_ai_responses_csv(self):
        with open(self.ai_responses_csv_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "timestamp_utc",
                    "epoch_ts",
                    "prompt_sent",
                    "fear_greed",
                    "gemini",
                    "openai",
                    "claude",
                    "perplexity",
                    "grok",
                    "consensus",
                ]
            )

    def _ensure_trades_csv(self):
        if os.path.exists(self.trades_csv_path):
            return
        with open(self.trades_csv_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "timestamp_utc",
                    "epoch_ts",
                    "coin",
                    "side",
                    "reason",
                    "entry_price",
                    "exit_price",
                    "size",
                    "entry_usdc",
                    "exit_usdc",
                    "gross_pnl",
                    "net_pnl",
                    "fees_paid",
                    "seconds_open",
                    "available_after",
                    "equity_after",
                ]
            )

    def _append_trade_csv(self, trade):
        with open(self.trades_csv_path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    iso_utc(float(trade["timestamp"])),
                    f"{float(trade['timestamp']):.6f}",
                    BTC_PERP,
                    trade["side"],
                    trade["reason"],
                    f"{float(trade['entry_price']):.8f}",
                    f"{float(trade['exit_price']):.8f}",
                    f"{float(trade['size']):.10f}",
                    f"{float(trade['entry_usdc']):.4f}",
                    f"{float(trade['exit_usdc']):.4f}",
                    f"{float(trade['gross_pnl']):.4f}",
                    f"{float(trade['net_pnl']):.4f}",
                    f"{float(trade['fees_paid']):.4f}",
                    f"{float(trade['seconds_open']):.2f}",
                    f"{float(trade['available_after']):.4f}",
                    f"{float(trade['equity_after']):.4f}",
                ]
            )

    def _append_ai_responses_csv(self, ts, prompt_text, fear_greed, provider_results, provider_errors, consensus):
        provider_result_map = {item["provider"]: item for item in provider_results}
        provider_error_map = dict(provider_errors or {})
        fear_greed_cell = (
            f"{fear_greed.get('value', '')} {fear_greed.get('classification', '')}".strip()
            or fear_greed.get("signal", "")
            or ""
        )
        row = [iso_utc(ts), f"{ts:.6f}", prompt_text, fear_greed_cell]
        for provider in AI_PROVIDER_ORDER:
            result = provider_result_map.get(provider)
            if result:
                row.append(format_ai_response_cell(result["signal"], result["why"]))
            else:
                row.append(format_ai_response_cell("", "", provider_error_map.get(provider, "")))
        row.append(format_ai_response_cell(consensus["signal"], consensus["why"]))
        with open(self.ai_responses_csv_path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(row)

    def _ensure_strategy_returns_csv(self):
        if os.path.exists(self.strategy_returns_csv_path):
            return
        headers = [
            "period_start_utc",
            "period_start_ts",
            "period_end_utc",
            "period_end_ts",
            "entry_price",
            "exit_price",
            "btc_move_pct",
            "round_trip_fee_pct",
        ]
        for provider in AI_PROVIDER_ORDER:
            headers.extend([f"{provider}_signal", f"{provider}_return_pct"])
        headers.extend(["consensus_signal", "consensus_return_pct"])
        with open(self.strategy_returns_csv_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers)

    def _signal_return_pct(self, signal_value, entry_price, exit_price):
        signal_text = str(signal_value or "").strip().upper()
        if signal_text == "NO_TRADE":
            return 0.0
        if signal_text not in VALID_SIGNALS or entry_price <= 0 or exit_price <= 0:
            return None
        move_pct = pct_change(entry_price, exit_price)
        directional_move_pct = move_pct if signal_text == "LONG" else -move_pct
        return directional_move_pct - (TAKER_FEE_PCT * 2.0)

    def _queue_strategy_evaluation(self, ts, entry_price, provider_results, consensus):
        entry_price = safe_float(entry_price, 0.0)
        if entry_price <= 0:
            self.log("Strategy return log skipped: entry price unavailable.")
            return
        signal_map = {
            provider: ""
            for provider in AI_PROVIDER_ORDER
        }
        for item in provider_results:
            provider = str(item.get("provider", "")).strip()
            if provider in signal_map:
                signal_map[provider] = str(item.get("signal", "")).strip().upper()
        signal_map["consensus"] = str(consensus.get("signal", "")).strip().upper()
        self.pending_strategy_evaluations.append(
            {
                "start_ts": float(ts),
                "end_ts": float(ts) + SIGNAL_INTERVAL_SECONDS,
                "entry_price": entry_price,
                "signals": signal_map,
            }
        )

    def _append_strategy_returns_csv(self, evaluation, exit_price):
        start_ts = float(evaluation["start_ts"])
        end_ts = float(evaluation["end_ts"])
        entry_price = float(evaluation["entry_price"])
        exit_price = float(exit_price)
        btc_move_pct = pct_change(entry_price, exit_price)
        row = [
            iso_utc(start_ts),
            f"{start_ts:.6f}",
            iso_utc(end_ts),
            f"{end_ts:.6f}",
            f"{entry_price:.8f}",
            f"{exit_price:.8f}",
            f"{btc_move_pct:.6f}",
            f"{(TAKER_FEE_PCT * 2.0):.6f}",
        ]
        signals = evaluation.get("signals", {})
        for provider in AI_PROVIDER_ORDER:
            signal_value = str(signals.get(provider, "")).strip().upper()
            result_pct = self._signal_return_pct(signal_value, entry_price, exit_price)
            row.extend(
                [
                    signal_value,
                    "" if result_pct is None else f"{result_pct:.6f}",
                ]
            )
        consensus_signal = str(signals.get("consensus", "")).strip().upper()
        consensus_return_pct = self._signal_return_pct(consensus_signal, entry_price, exit_price)
        row.extend(
            [
                consensus_signal,
                "" if consensus_return_pct is None else f"{consensus_return_pct:.6f}",
            ]
        )
        with open(self.strategy_returns_csv_path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(row)

    def _resolve_strategy_exit_price(self, target_ts):
        price = self.market.nearest_price_to_ts(target_ts, max_gap_seconds=45)
        if price:
            return price
        return fetch_btc_price_near_ts(target_ts)

    def maybe_finalize_strategy_returns(self):
        if not self.pending_strategy_evaluations:
            return
        now = now_ts()
        remaining = []
        completed = 0
        for evaluation in self.pending_strategy_evaluations:
            end_ts = float(evaluation.get("end_ts", 0.0) or 0.0)
            if end_ts <= 0 or now < end_ts:
                remaining.append(evaluation)
                continue
            exit_price = safe_float(self._resolve_strategy_exit_price(end_ts), 0.0)
            if exit_price <= 0:
                remaining.append(evaluation)
                continue
            self._append_strategy_returns_csv(evaluation, exit_price)
            completed += 1
        self.pending_strategy_evaluations = remaining
        if completed:
            self.log(f"Logged {completed} strategy return window(s).")

    def log(self, message):
        ts = now_ts()
        self.logs.appendleft(f"{format_ts(ts)} {message}")

    def _state_payload(self):
        return {
            "saved_at": now_ts(),
            "available": self.available,
            "position": self.position,
            "trades": list(self.trades),
            "logs": list(self.logs),
            "last_signal": self.last_signal,
            "last_signal_why": self.last_signal_why,
            "last_provider_results": self.last_provider_results,
            "last_provider_errors": self.last_provider_errors,
            "last_signal_at": self.last_signal_at,
            "next_signal_at": self.next_signal_at,
            "last_snapshot_key": self.last_snapshot_key,
            "pending_strategy_evaluations": self.pending_strategy_evaluations,
        }

    def persist_state(self, force=False):
        ts = now_ts()
        if not force and (ts - self.last_state_save_at) < STATE_SAVE_INTERVAL_SECONDS:
            return
        payload = self._state_payload()
        temp_path = f"{self.state_path}.tmp"
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=True, indent=2)
        os.replace(temp_path, self.state_path)
        self.last_state_save_at = ts

    def _restore_state(self):
        state_path = self.state_path if os.path.exists(self.state_path) else self.legacy_state_path
        if not os.path.exists(state_path):
            return False
        try:
            raw = Path(state_path).read_text(encoding="utf-8")
            payload = json.loads(raw)
        except Exception:
            return False
        if not isinstance(payload, dict):
            return False
        try:
            saved_at = float(payload.get("saved_at", 0.0) or 0.0)
            self.available = float(payload.get("available", self.available))
            position = payload.get("position")
            self.position = position if isinstance(position, dict) else None
            trades = payload.get("trades", [])
            self.trades = deque(trades if isinstance(trades, list) else [], maxlen=12)
            logs = payload.get("logs", [])
            self.logs = deque(logs if isinstance(logs, list) else [], maxlen=12)
            self.last_signal = str(payload.get("last_signal", self.last_signal))
            self.last_signal_why = str(payload.get("last_signal_why", self.last_signal_why))
            self.last_signal_sources = []
            raw_provider_results = payload.get("last_provider_results", {})
            self.last_provider_results = raw_provider_results if isinstance(raw_provider_results, dict) else {}
            raw_provider_errors = payload.get("last_provider_errors", {})
            self.last_provider_errors = raw_provider_errors if isinstance(raw_provider_errors, dict) else {}
            self.last_signal_at = float(payload.get("last_signal_at", self.last_signal_at) or 0.0)
            self.next_signal_at = float(payload.get("next_signal_at", self.next_signal_at) or self.start_time)
            self.last_snapshot_key = str(payload.get("last_snapshot_key", self.last_snapshot_key))
            pending = payload.get("pending_strategy_evaluations", [])
            self.pending_strategy_evaluations = pending if isinstance(pending, list) else []
        except Exception:
            return False
        now = now_ts()
        if self.next_signal_at <= now:
            missed_by = max(0.0, now - self.next_signal_at)
            missed_checks = max(1, int(missed_by // SIGNAL_INTERVAL_SECONDS) + 1)
            self.next_signal_at = now
            self.last_snapshot_key = ""
            if saved_at > 0:
                offline_for = max(0.0, now - saved_at)
                self.resume_note = (
                    f"Missed {missed_checks} scheduled check(s) while offline "
                    f"({offline_for:.0f}s away, overdue by {missed_by:.0f}s). "
                    "Catching up as soon as market data is ready."
                )
            else:
                self.resume_note = "Missed scheduled check while offline. Catching up as soon as market data is ready."
        return True

    def _signal_ready_state(self):
        state = self.market.get_state()
        current_mid = float(state["mid"] or 0.0)
        last_message_at = float(state["last_message_at"] or 0.0)
        if current_mid <= 0 or last_message_at <= 0:
            reason = state["last_error"] or "Market feed not ready yet."
            return False, reason
        feed_age = max(0.0, now_ts() - last_message_at)
        if feed_age > FEED_STALE_AFTER_SECONDS:
            return False, f"Market feed stale ({feed_age:.1f}s)."
        return True, ""

    def _defer_signal(self, reason, delay_seconds):
        retry_delay = max(1, int(round(float(delay_seconds))))
        self.next_signal_at = now_ts() + retry_delay
        self.last_signal_error = reason
        self.log(f"{reason} Retrying in {retry_delay}s.")
        self.persist_state(force=True)

    def maybe_save_dashboard_snapshot(self, market):
        if self.next_signal_at <= 0:
            return
        remaining = self.next_signal_at - now_ts()
        if remaining < 0 or remaining > SNAPSHOT_LEAD_SECONDS:
            return
        snapshot_key = str(int(self.next_signal_at))
        if snapshot_key == self.last_snapshot_key:
            return
        if self.snapshot_thread and self.snapshot_thread.is_alive():
            return
        os.makedirs(self.snapshot_dir, exist_ok=True)
        stamp = datetime.fromtimestamp(self.next_signal_at, tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        png_path = os.path.join(self.snapshot_dir, f"tradebot_{stamp}.png")
        text_path = os.path.join(self.snapshot_dir, f"tradebot_{stamp}.txt")
        self.last_snapshot_key = snapshot_key

        def save_snapshot():
            dashboard_text = render_dashboard_text(self, market)
            try:
                write_text_image(dashboard_text, png_path)
                self.log(f"Saved dashboard snapshot -> {png_path}")
                return
            except Exception as exc:
                Path(text_path).write_text(dashboard_text, encoding="utf-8")
                self.log(f"Saved text snapshot -> {text_path} | png unavailable: {exc}")

        self.snapshot_thread = threading.Thread(
            target=save_snapshot,
            daemon=True,
            name="dashboard-snapshot-writer",
        )
        self.snapshot_thread.start()

    def live_pnl(self):
        if not self.position:
            return 0.0
        state = self.market.get_state()
        current_mid = float(state["mid"] or 0.0)
        if current_mid <= 0:
            return 0.0
        direction = 1.0 if self.position["side"] == "LONG" else -1.0
        return ((current_mid - float(self.position["entry_price"])) * float(self.position["size"])) * direction

    def equity(self):
        reserved_margin = float(self.position["initial_margin"]) if self.position else 0.0
        return self.available + reserved_margin + self.live_pnl()

    def maybe_stop_loss(self):
        if not self.position:
            return
        live_pnl = self.live_pnl()
        if live_pnl > -STOP_LOSS_USDC:
            return
        self.log(f"Stop loss triggered at {live_pnl:.2f} USDC.")
        self.close_position(f"STOP_LOSS_{STOP_LOSS_USDC:.2f}")

    def _call_with_retries(self, provider, model, request_url, payload, headers, parse_text):
        last_exc = None
        data = None
        for attempt in range(1, OPENAI_MAX_ATTEMPTS + 1):
            try:
                data, _response_text = post_json(
                    request_url,
                    payload,
                    timeout=OPENAI_TIMEOUT_SECONDS,
                    headers=headers,
                    return_raw=True,
                )
                break
            except Exception as exc:
                last_exc = exc
                retryable = "timeout" in str(exc).lower() or "network error" in str(exc).lower()
                if retryable and attempt < OPENAI_MAX_ATTEMPTS:
                    self.log(
                        f"{provider} request issue on attempt {attempt}/{OPENAI_MAX_ATTEMPTS}: {exc}. Retrying..."
                    )
                    time.sleep(OPENAI_RETRY_DELAY_SECONDS)
                    continue
                raise
        if data is None:
            raise last_exc or RuntimeError(f"{provider} request failed")

        parsed_response = extract_signal_response({"text": parse_text(data), "raw": data})
        text = parsed_response["signal"].strip().upper()
        why = parsed_response["why"]
        sources = parsed_response["sources"]
        if text not in VALID_SIGNALS:
            raw_preview = json.dumps(data, ensure_ascii=True)[:240]
            raise RuntimeError(f"Unexpected {provider} response: {text or 'EMPTY'} | raw={raw_preview}")
        return {
            "provider": provider,
            "model": model,
            "signal": text,
            "why": why,
            "sources": sources,
        }

    def _query_openai_signal(self, prompt_text):
        api_key = self.settings.get("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY missing from settings.txt")
        model = self.settings.get("OPENAI_MODEL", OPENAI_MODEL_DEFAULT).strip() or OPENAI_MODEL_DEFAULT
        payload = {
            "model": model,
            "input": prompt_text,
            "max_output_tokens": 10000,
        }
        return self._call_with_retries(
            "openai",
            model,
            OPENAI_RESPONSES_URL,
            payload,
            {"Authorization": f"Bearer {api_key}"},
            extract_openai_output_text,
        )

    def _query_gemini_signal(self, prompt_text):
        api_key = self.settings.get("GEMINI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY missing from settings.txt")
        model = self.settings.get("GEMINI_MODEL", GEMINI_MODEL_DEFAULT).strip() or GEMINI_MODEL_DEFAULT
        request_url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            + urllib_parse.quote(model, safe="")
            + ":generateContent?key="
            + urllib_parse.quote(api_key, safe="")
        )
        payload = {
            "contents": [{"parts": [{"text": prompt_text}]}],
            "generationConfig": {"maxOutputTokens": 10000},
        }
        return self._call_with_retries(
            "gemini",
            model,
            request_url,
            payload,
            {"Content-Type": "application/json"},
            extract_gemini_output_text,
        )

    def _query_claude_signal(self, prompt_text):
        api_key = self.settings.get("CLAUDE_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("CLAUDE_API_KEY missing from settings.txt")
        model = self.settings.get("CLAUDE_MODEL", CLAUDE_MODEL_DEFAULT).strip() or CLAUDE_MODEL_DEFAULT
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        }

        def run_for_model(model_name):
            payload = {
                "model": model_name,
                "max_tokens": 1000,
                "messages": [{"role": "user", "content": prompt_text}],
            }
            return self._call_with_retries(
                "claude",
                model_name,
                CLAUDE_MESSAGES_URL,
                payload,
                headers,
                extract_claude_output_text,
            )

        try:
            return run_for_model(model)
        except Exception as exc:
            fallback_model = ""
            if model.endswith("-latest"):
                fallback_model = model[: -len("-latest")]
            if not fallback_model or fallback_model == model:
                raise
            self.log(f"claude retrying with fallback model -> {fallback_model}")
            try:
                return run_for_model(fallback_model)
            except Exception as fallback_exc:
                raise RuntimeError(
                    f"{exc} | fallback {fallback_model} failed: {fallback_exc}"
                ) from fallback_exc

    def _query_perplexity_signal(self, prompt_text):
        api_key = self.settings.get("PERPLEXITY_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("PERPLEXITY_API_KEY missing from settings.txt")
        model = self.settings.get("PERPLEXITY_MODEL", PERPLEXITY_MODEL_DEFAULT).strip() or PERPLEXITY_MODEL_DEFAULT
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt_text}],
            "max_tokens": 1000,
            "web_search_options": {"search_context_size": "low"},
        }
        return self._call_with_retries(
            "perplexity",
            model,
            PERPLEXITY_SONAR_URL,
            payload,
            {
                "Authorization": f"Bearer {api_key}",
            },
            extract_chat_completion_text,
        )

    def _query_grok_signal(self, prompt_text):
        api_key = self.settings.get("GROK_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("GROK_API_KEY missing from settings.txt")
        model = self.settings.get("GROK_MODEL", GROK_MODEL_DEFAULT).strip() or GROK_MODEL_DEFAULT
        payload = {
            "model": model,
            "input": prompt_text,
            "max_output_tokens": 10000,
        }
        return self._call_with_retries(
            "grok",
            model,
            GROK_RESPONSES_URL,
            payload,
            {
                "Authorization": f"Bearer {api_key}",
            },
            extract_openai_output_text,
        )

    def _summarize_consensus(self, provider_results, fear_greed):
        counts = {signal: 0 for signal in VALID_SIGNALS}
        for result in provider_results:
            counts[result["signal"]] += 1
        top_votes = max(counts.values()) if counts else 0
        winners = [signal for signal, count in counts.items() if count == top_votes and count > 0]
        final_signal = winners[0] if len(winners) == 1 else "NO_TRADE"
        vote_summary = ", ".join(
            f"{result['provider']}={result['signal']}" for result in provider_results
        )
        fear_summary = fear_greed.get("value", "")
        fear_classification = fear_greed.get("classification", "")
        why_parts = [
            f"Consensus {top_votes}/{len(provider_results)} -> {final_signal}. Votes: {vote_summary}.",
        ]
        if fear_summary or fear_classification:
            why_parts.append(
                f"Fear & Greed background metric: {fear_summary or 'n/a'} {fear_classification}".strip() + "."
            )
        sources = []
        for result in provider_results:
            for source in result["sources"]:
                tagged = f"{result['provider']}: {source}"
                if tagged not in sources:
                    sources.append(tagged)
            if len(sources) >= 3:
                break
        return {
            "signal": final_signal,
            "why": " ".join(why_parts),
            "sources": sources[:3],
        }

    def query_signal(self):
        snapshot = fetch_hyperliquid_snapshot()
        fear_greed = fetch_fear_greed()
        prompt_text = build_ai_prompt(snapshot, fear_greed)
        provider_results = []
        provider_errors = {}
        provider_methods = {
            "gemini": self._query_gemini_signal,
            "openai": self._query_openai_signal,
            "claude": self._query_claude_signal,
            "perplexity": self._query_perplexity_signal,
            "grok": self._query_grok_signal,
        }
        for provider in AI_PROVIDER_ORDER:
            try:
                provider_results.append(provider_methods[provider](prompt_text))
            except Exception as exc:
                provider_errors[provider] = str(exc)
                self.log(f"{provider} signal error -> {exc}")
        if not provider_results:
            raise RuntimeError(
                "All AI providers failed: "
                + " | ".join(f"{provider}: {error_text}" for provider, error_text in provider_errors.items())
            )
        consensus = self._summarize_consensus(provider_results, fear_greed)
        consensus["provider_results"] = provider_results
        consensus["provider_errors"] = provider_errors
        consensus["fear_greed"] = fear_greed
        consensus["snapshot"] = snapshot
        consensus["prompt_text"] = prompt_text
        return consensus

    def maybe_run_signal(self):
        if now_ts() < self.next_signal_at:
            return
        ready, readiness_reason = self._signal_ready_state()
        if not ready:
            self._defer_signal(readiness_reason, STARTUP_SIGNAL_RETRY_SECONDS)
            return
        signal_time = now_ts()
        try:
            signal_result = self.query_signal()
            signal_value = signal_result["signal"]
            self.last_signal_why = signal_result["why"]
            self.last_signal_sources = list(signal_result["sources"])
            self.last_signal_error = ""
            self.last_provider_results = {
                item["provider"]: dict(item) for item in signal_result.get("provider_results", [])
            }
            self.last_provider_errors = dict(signal_result.get("provider_errors", {}))
            fear_greed = signal_result.get("fear_greed", {})
            self._append_ai_responses_csv(
                signal_time,
                signal_result.get("prompt_text", ""),
                fear_greed,
                signal_result.get("provider_results", []),
                signal_result.get("provider_errors", {}),
                signal_result,
            )
            self._queue_strategy_evaluation(
                signal_time,
                signal_result.get("snapshot", {}).get("px", {}).get("mid"),
                signal_result.get("provider_results", []),
                signal_result,
            )
            if fear_greed.get("value") or fear_greed.get("classification"):
                self.log(
                    "Fear & Greed -> "
                    f"{fear_greed.get('value', 'n/a')} {fear_greed.get('classification', '')}".strip()
                )
            for provider_result in signal_result.get("provider_results", []):
                self.log(
                    f"{provider_result['provider']} signal -> {provider_result['signal']} | "
                    f"{provider_result['model']}"
                )
                if provider_result["why"]:
                    self.log(f"{provider_result['provider']} why -> {provider_result['why']}")
            self.log(f"AI consensus signal -> {signal_value}")
            if self.last_signal_why:
                self.log(f"AI consensus why -> {self.last_signal_why}")
            if self.last_signal_sources:
                self.log(f"AI consensus sources -> {' | '.join(self.last_signal_sources)}")
        except Exception as exc:
            self.last_signal_why = ""
            self.last_signal_sources = []
            self.last_provider_errors = {}
            self._defer_signal(f"AI signal error -> {exc}", SIGNAL_RETRY_DELAY_SECONDS)
            return
        self.last_signal_at = signal_time
        self.next_signal_at = signal_time + SIGNAL_INTERVAL_SECONDS
        self.last_signal = signal_value
        try:
            self.process_signal(signal_value)
        except Exception as exc:
            self._defer_signal(f"Signal execution error: {exc}", SIGNAL_RETRY_DELAY_SECONDS)
            return
        self.persist_state(force=True)

    def process_signal(self, signal_value):
        current_side = self.position["side"] if self.position else None
        if signal_value == "NO_TRADE":
            if self.position:
                self.close_position("NO_TRADE")
            else:
                self.log("No trade signal while flat.")
            return
        if current_side == signal_value:
            self.log(f"Signal {signal_value}; already in position, holding.")
            return
        if self.position and current_side != signal_value:
            self.close_position(f"FLIP_TO_{signal_value}")
        self.open_position(signal_value)

    def open_position(self, side):
        snapshot = self.market.fetch_book_snapshot()
        target_margin = max(0.0, self.equity()) * STACK_FRACTION
        target_margin = min(target_margin, self.available)
        if target_margin <= 0:
            self.log(f"Entry skipped for {side}: no available balance.")
            return
        notional = target_margin * LEVERAGE
        entry_price = float(snapshot["best_ask"] if side == "LONG" else snapshot["best_bid"])
        entry_fee = apply_fee(notional, TAKER_FEE_PCT)
        if target_margin + entry_fee > self.available:
            target_margin = self.available / (1.0 + (LEVERAGE * TAKER_FEE_PCT / 100.0))
            notional = target_margin * LEVERAGE
            entry_fee = apply_fee(notional, TAKER_FEE_PCT)
        size = notional / entry_price if entry_price > 0 else 0.0
        if size <= 0:
            self.log(f"Entry skipped for {side}: invalid size.")
            return
        self.available -= target_margin + entry_fee
        self.position = {
            "coin": BTC_PERP,
            "side": side,
            "entry_price": entry_price,
            "size": size,
            "notional": notional,
            "initial_margin": target_margin,
            "entry_fee": entry_fee,
            "entry_time": now_ts(),
        }
        self.log(
            f"{side} entry BTC at {entry_price:,.2f}. Margin {target_margin:,.2f} USDC | "
            f"Notional {notional:,.2f} | Fee {entry_fee:.4f}"
        )
        self.persist_state(force=True)

    def close_position(self, reason):
        if not self.position:
            return
        snapshot = self.market.fetch_book_snapshot()
        side = self.position["side"]
        close_price = float(snapshot["best_bid"] if side == "LONG" else snapshot["best_ask"])
        size = float(self.position["size"])
        direction = 1.0 if side == "LONG" else -1.0
        gross_pnl = ((close_price - float(self.position["entry_price"])) * size) * direction
        exit_notional = size * close_price
        exit_fee = apply_fee(exit_notional, TAKER_FEE_PCT)
        entry_fee = float(self.position["entry_fee"])
        net_pnl = gross_pnl - entry_fee - exit_fee
        self.available += float(self.position["initial_margin"]) + gross_pnl - exit_fee
        trade = {
            "timestamp": now_ts(),
            "side": side,
            "entry_price": float(self.position["entry_price"]),
            "exit_price": close_price,
            "size": size,
            "entry_usdc": float(self.position["initial_margin"]),
            "exit_usdc": float(self.position["initial_margin"]) + net_pnl,
            "gross_pnl": gross_pnl,
            "net_pnl": net_pnl,
            "fees_paid": entry_fee + exit_fee,
            "reason": reason,
            "seconds_open": now_ts() - float(self.position["entry_time"]),
            "available_after": self.available,
            "equity_after": self.available,
        }
        self.trades.appendleft(trade)
        self.position = None
        self._append_trade_csv(trade)
        self.log(
            f"{side} exit {reason} at {close_price:,.2f}. Gross {gross_pnl:.4f} | "
            f"Net {net_pnl:.4f} USDC | Fees {(entry_fee + exit_fee):.4f}"
        )
        self.persist_state(force=True)


def style_pct(value):
    if value > 0:
        return "bold bright_green"
    if value < 0:
        return "bold bright_red"
    return "bold white"


def render_price_cell(price, previous_price):
    if price is None:
        return Text("-", style="dim")
    style = "white"
    if previous_price is not None:
        if price > previous_price:
            style = "green"
        elif price < previous_price:
            style = "red"
    return Text(f"{price:,.2f}", style=style)


def label_value(label, value, value_style=BODY_STYLE, label_style=HEADING_STYLE):
    return Text.assemble((f"{label}\n", label_style), (str(value), value_style))


def build_countdown_panel(trader):
    cycle_seconds = max(1, int(SIGNAL_INTERVAL_SECONDS))
    remaining = max(0.0, trader.next_signal_at - now_ts())
    remaining = max(0.0, min(cycle_seconds, remaining))
    minutes = int(remaining // 60)
    seconds = int(remaining % 60)
    countdown_text = f"{minutes:02d}:{seconds:02d}"
    return Text(f"Next 15m Check {countdown_text}", style=HEADING_STYLE)


def build_section(title, content):
    return Group(
        Text(""),
        Rule(str(title).title(), style=PANEL_BORDER_STYLE),
        Text(""),
        content,
    )


def build_summary_table(trader, market_state):
    table = Table.grid(expand=True)
    for _ in range(8):
        table.add_column(justify="left")
    position_side = trader.position["side"] if trader.position else "FLAT"
    next_signal_in = max(0, int(round(trader.next_signal_at - now_ts())))
    feed_age = max(0.0, now_ts() - market_state["last_message_at"]) if market_state["last_message_at"] else 9999.0
    live_pnl = trader.live_pnl()
    live_pnl_style = style_pct(live_pnl)
    table.add_row(
        label_value("Available", f"{trader.available:,.2f} USDC"),
        label_value("Equity", f"{trader.equity():,.2f} USDC"),
        label_value("Live PnL", f"{live_pnl:,.2f} USDC", value_style=live_pnl_style),
        label_value("BTC Mid", f"{market_state['mid']:,.2f}" if market_state["mid"] else "-"),
        label_value("Signal", trader.last_signal),
        label_value("Next Ask", f"{next_signal_in}s"),
        label_value("Position", position_side),
        label_value("Feed Age", f"{feed_age:.1f}s"),
    )
    return table


def build_price_table(market_state):
    table = Table(expand=True, padding=(0, 0), pad_edge=False, collapse_padding=True, box=None)
    table.add_column("Perp", header_style=HEADING_STYLE, style=BODY_STYLE, no_wrap=True)
    reference_ts = float(market_state["last_message_at"] or now_ts())
    labels = [
        format_minute_stamp(reference_ts - (minute * DISPLAY_MINUTE_SECONDS))
        for minute in range(DISPLAY_COLUMNS, 0, -1)
    ]
    labels.append("Live")
    for label in labels:
        table.add_column(label, justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    prices = list(market_state["minute_prices"])
    live_mid = float(market_state["mid"] or 0.0)
    prices.append(live_mid if live_mid > 0 else None)
    cells = []
    previous = None
    for price in prices:
        cells.append(render_price_cell(price, previous))
        previous = price if price is not None else previous
    table.add_row(Text(BTC_PERP, style=BODY_STYLE), *cells)
    return table


def build_position_table(trader, market_state):
    table = Table(expand=True, padding=(0, 0), pad_edge=False, collapse_padding=True, box=None)
    table.add_column("Perp", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Side", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Entry", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Current", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("PnL", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Gain %", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Opened", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    if not trader.position:
        table.add_row("-", "No active position", "-", "-", "-", "-", "-")
        return table
    current_mid = float(market_state["mid"] or trader.position["entry_price"])
    direction = 1.0 if trader.position["side"] == "LONG" else -1.0
    current_pnl = trader.live_pnl()
    pnl_pct = pct_change(float(trader.position["entry_price"]), current_mid) * direction
    current_pnl_text = Text(f"{current_pnl:,.2f} USDC", style=style_pct(current_pnl))
    pnl_text = Text(f"{pnl_pct:,.4f}%", style=style_pct(pnl_pct))
    table.add_row(
        BTC_PERP,
        trader.position["side"],
        f"{float(trader.position['entry_price']):,.2f}",
        f"{current_mid:,.2f}",
        current_pnl_text,
        pnl_text,
        format_ts(float(trader.position["entry_time"])),
    )
    return table


def build_trades_table(trader):
    table = Table(expand=True, padding=(0, 0), pad_edge=False, collapse_padding=True, box=None)
    table.add_column("Time", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Side", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Reason", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Entry Px", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Exit Px", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Entry USD", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Exit USD", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Raw PnL", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Fees", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Final PnL", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    if not trader.trades:
        table.add_row("-", "No trades yet", "-", "-", "-", "-", "-", "-", "-", "-")
        return table
    for trade in list(trader.trades)[:8]:
        gross_pnl = float(trade["gross_pnl"])
        net_pnl = float(trade["net_pnl"])
        fees_paid = float(trade["fees_paid"])
        gross_style = style_pct(gross_pnl)
        net_style = style_pct(net_pnl)
        table.add_row(
            format_ts(float(trade["timestamp"])),
            trade["side"],
            trade["reason"],
            f"{float(trade['entry_price']):,.2f}",
            f"{float(trade['exit_price']):,.2f}",
            f"{float(trade['entry_usdc']):,.2f}",
            f"{float(trade['exit_usdc']):,.2f}",
            Text(f"{gross_pnl:,.4f}", style=gross_style),
            Text(f"{fees_paid:,.4f}", style="bold yellow"),
            Text(f"{net_pnl:,.4f}", style=net_style),
        )
    return table


def build_logs_panel(trader):
    lines = list(trader.logs)[:10]
    if not lines:
        return Text("No logs yet.", style=BODY_STYLE)
    rendered = []
    for line in lines:
        match = re.match(r"^(\d{2}:\d{2}:\d{2})(\s+)(.*)$", str(line))
        if match:
            rendered.append(
                Text.assemble(
                    (match.group(1), HEADING_STYLE),
                    (match.group(2), BODY_STYLE),
                    (match.group(3), BODY_STYLE),
                )
            )
        else:
            rendered.append(Text(str(line), style=BODY_STYLE))
    return Group(*rendered)


def build_signal_rationale_panel(trader):
    summary_lines = [Text.assemble(("Signal: ", HEADING_STYLE), (trader.last_signal, BODY_STYLE))]
    if trader.last_signal_why:
        summary_lines.append(Text.assemble(("Why: ", HEADING_STYLE), (trader.last_signal_why, BODY_STYLE)))

    provider_table = Table(expand=True, padding=(0, 1), pad_edge=False, collapse_padding=False, box=box.SIMPLE_HEAD)
    for provider in AI_PROVIDER_ORDER:
        provider_table.add_column(provider.title(), header_style=HEADING_STYLE, style=BODY_STYLE)

    row = []
    has_provider_output = False
    for provider in AI_PROVIDER_ORDER:
        result = trader.last_provider_results.get(provider)
        error_text = trader.last_provider_errors.get(provider, "")
        if result:
            has_provider_output = True
            detail_parts = [wrap_panel_text(result.get("why", ""), width=24) or "No rationale returned."]
            cell_text = f"{result.get('signal', 'NO_RESPONSE')}\n\n" + "\n\n".join(
                part for part in detail_parts if part
            )
            row.append(cell_text)
            continue
        if error_text:
            has_provider_output = True
            row.append(f"NO_RESPONSE\n\n{wrap_panel_text(error_text, width=24)}")
            continue
        row.append("Waiting for next signal.")

    if has_provider_output:
        provider_table.add_row(*row)
        return Group(*summary_lines, Text(""), provider_table)

    if len(summary_lines) == 1 and trader.last_signal == "PENDING":
        summary_lines.append(Text("No model rationale yet.", style=BODY_STYLE))
    return Group(*summary_lines)


def build_dashboard(trader, market):
    state = market.get_state()
    market_state = {
        **state,
        "minute_prices": market.get_minute_prices(),
    }
    feed_status = market_feed_status(market_state)
    status_text = trader.last_signal_error or (state["last_error"] if state["last_error"] else ("Managing position." if trader.position else "Waiting for next signal."))
    header_table = Table.grid(expand=True)
    header_table.add_column(ratio=1)
    header_table.add_column(justify="right", no_wrap=True)
    header_table.add_row(
        Text(dashboard_title_text(), style=BODY_STYLE),
        Text(feed_status["label"], style=feed_status["style"]),
    )
    header_table.add_row(
        Text(
            f"BTC only | Runtime {int(now_ts() - trader.start_time)}s | WS open {format_ts(state['last_open_at']) or 'n/a'}",
            style=HEADING_STYLE,
        ),
        Text(feed_status["detail"], style=HEADING_STYLE),
    )
    header = [
        header_table,
        Text(status_text, style="bold yellow" if (trader.last_signal_error or state["last_error"]) else "bold green"),
    ]
    sections = [
        build_countdown_panel(trader),
        Text(""),
        *header,
        build_section("Account", build_summary_table(trader, market_state)),
        build_section("BTC 1m Tape", build_price_table(market_state)),
        build_section("Open Position", build_position_table(trader, market_state)),
        build_section("Recent Trades", build_trades_table(trader)),
        build_section("Signal Rationale", build_signal_rationale_panel(trader)),
        build_section("Action Log", build_logs_panel(trader)),
    ]
    return Panel(Group(*sections), border_style=PANEL_BORDER_STYLE, box=box.ROUNDED)


def main():
    stop_event = threading.Event()
    market = BtcMarket(stop_event)
    settings = load_settings(resolve_settings_path())
    trader = SandboxTrader(market, settings)
    market.start()

    def handle_stop(_signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    def trader_loop():
        while not stop_event.is_set():
            try:
                trader.maybe_finalize_strategy_returns()
                trader.maybe_stop_loss()
                trader.maybe_run_signal()
                trader.maybe_save_dashboard_snapshot(market)
                trader.persist_state()
            except Exception as exc:
                trader.log(f"Main loop error: {exc}")
            time.sleep(1)

    worker = threading.Thread(target=trader_loop, daemon=True, name="btc-signal-loop")
    worker.start()

    try:
        with Live(
            build_dashboard(trader, market),
            console=console,
            refresh_per_second=LIVE_REFRESH_HZ,
            screen=LIVE_SCREEN,
            auto_refresh=False,
            vertical_overflow="crop",
        ) as live:
            while not stop_event.is_set():
                live.update(build_dashboard(trader, market), refresh=True)
                time.sleep(1.0 / LIVE_REFRESH_HZ)
    finally:
        stop_event.set()
        try:
            trader.persist_state(force=True)
        except Exception:
            pass
        market.stop()
        console.print("\nStopped BTC sandbox runner.")


if __name__ == "__main__":
    main()
