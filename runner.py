#!/usr/bin/env python3
"""
BTC sandbox runner with a local web dashboard.

Run this file directly and it will:
1. start the market feed and sandbox engine,
2. serve the browser UI locally,
3. open the default browser automatically.
"""

from __future__ import annotations

import csv
import hashlib
import json
import mimetypes
import os
import re
import signal
import socket
import threading
import time
import webbrowser
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SETUP_DIR = os.path.join(BASE_DIR, "setup")
HISTORY_DIR = os.path.join(BASE_DIR, "history")
LEGACY_SETTINGS_PATH = os.path.join(BASE_DIR, "settings.txt")
LEGACY_STATE_PATH = os.path.join(BASE_DIR, "state.txt")
SETTINGS_PATH = os.path.join(SETUP_DIR, "settings.txt")
STATE_PATH = os.path.join(SETUP_DIR, "state.txt")
TRADES_CSV_PATH = os.path.join(HISTORY_DIR, "trades.csv")
AI_RESPONSES_CSV_PATH = os.path.join(HISTORY_DIR, "ai_responses.csv")
STRATEGY_RETURNS_CSV_PATH = os.path.join(HISTORY_DIR, "strategy_returns.csv")
WEB_DIST_DIR = os.path.join(BASE_DIR, "webui", "dist")

BTC_PERP = "BTC"
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
STATE_SAVE_INTERVAL_SECONDS = 3
SIGNAL_RETRY_DELAY_SECONDS = 30
STARTUP_SIGNAL_RETRY_SECONDS = 5
MARKET_POLL_INTERVAL_SECONDS = 1.0
DEFAULT_HTTP_PORT = 8000
HTTP_PORT_SEARCH_ATTEMPTS = 20
OPENAI_TIMEOUT_SECONDS = 45
OPENAI_MAX_ATTEMPTS = 2
OPENAI_RETRY_DELAY_SECONDS = 2
LATEST_CHANGE_SUMMARY = "Browser dashboard replaces the terminal renderer"
FEAR_GREED_LONG_THRESHOLD = 30
FEAR_GREED_SHORT_THRESHOLD = 70

VALID_SIGNALS = {"LONG", "SHORT", "NO_TRADE"}
AI_PROVIDER_ORDER = ("gemini", "openai", "claude", "perplexity", "grok")
SIGNAL_SCORE_MAP = {"SHORT": 1, "NO_TRADE": 0, "LONG": -1}
CONSENSUS_SCORE_THRESHOLD = 2

PROMPT = """I am trading on the Hyperliquid BTC Perpetual market using taker orders and my fees are 0.015% and 0.015% each way, so I need to clear 0.03% on any trade to make profit.

Decide the single best directional trade for the next 15 minutes using only the BTC market snapshot and sentiment metrics included in this prompt. Treat the supplied data as the full evidence set.

Priority order:
1. Immediate BTC market structure and momentum.
2. Order book pressure, spread, and short-term range positioning.
3. Background sentiment metrics explicitly included in this prompt, such as Fear & Greed if present.

Hard rules:
- Do not introduce ETF flows, Federal Reserve decisions, macro commentary, external news, or any other information unless it is explicitly included in this prompt.
- Do not rely on outside knowledge, assumed headlines, or guessed context.
- If the supplied data does not support a measurable edge after fees, return NO_TRADE.
- Do not default to NO_TRADE just because confidence is imperfect. If one side has the clearest edge from the supplied data, choose it.
- In the why field, cite the actual supplied metrics by name and value wherever possible.

Return valid JSON only with this exact shape:
{"signal":"LONG|SHORT|NO_TRADE","why":"1-3 short sentences using the supplied metrics only","sources":["up to 3 short source strings drawn only from the supplied prompt context"]}"""


def now_ts() -> float:
    return time.time()


def iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def format_ts(ts: float) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")


def format_commit_ts(iso_text: str) -> str:
    if not iso_text:
        return "unknown"
    try:
        dt = datetime.fromisoformat(str(iso_text).replace("Z", "+00:00"))
    except Exception:
        return str(iso_text)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def safe_float(value, default=0.0) -> float:
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


def pct_change(from_price: float, to_price: float) -> float:
    if from_price <= 0:
        return 0.0
    return ((to_price - from_price) / from_price) * 100.0


def apply_fee(notional: float, fee_pct: float) -> float:
    return float(notional) * (float(fee_pct) / 100.0)


def load_settings(path: str) -> dict:
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


def resolve_settings_path() -> str:
    if os.path.exists(SETTINGS_PATH):
        return SETTINGS_PATH
    if os.path.exists(LEGACY_SETTINGS_PATH):
        return LEGACY_SETTINGS_PATH
    return SETTINGS_PATH


def post_json(url: str, payload: dict, timeout: int, headers=None, return_raw=False):
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


def read_json_response(url: str, payload=None, headers=None, timeout=90, return_raw=False):
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
    return {
        "ret_pct": pct_change(first_close, last_close),
        "range_pct": pct_change(low, high) if low > 0 else 0.0,
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
        payload={"type": "l2Book", "coin": BTC_PERP},
        timeout=15,
    )
    candles_1m = read_json_response(
        HYPERLIQUID_INFO_URL,
        payload={
            "type": "candleSnapshot",
            "req": {
                "coin": BTC_PERP,
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
                "coin": BTC_PERP,
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
    mid = safe_float(mids.get(BTC_PERP)) if isinstance(mids, dict) else 0.0
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
            return {"signal": match.group(1), "why": "", "sources": []}
    return {"signal": "", "why": "", "sources": []}


def extract_openai_output_text(response_data):
    top_level = response_data.get("output_text")
    if isinstance(top_level, str) and top_level.strip():
        return top_level.strip()
    parts = []
    for item in response_data.get("output", []) or []:
        for content in item.get("content", []) or []:
            if content.get("type") in {"output_text", "text"}:
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


def get_local_build_info():
    file_path = os.path.abspath(__file__)
    try:
        raw = Path(file_path).read_bytes()
        digest = hashlib.sha256(raw).hexdigest()[:8]
        modified_at = format_commit_ts(
            datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc).isoformat()
        )
        return {"label": digest, "modified_at": modified_at, "file_path": file_path}
    except Exception as exc:
        return {
            "label": "unknown",
            "modified_at": f"unavailable ({exc})",
            "file_path": file_path,
        }


def signal_return_pct(signal_value, entry_price, exit_price):
    signal_text = str(signal_value or "").strip().upper()
    if signal_text == "NO_TRADE":
        return 0.0
    if signal_text not in VALID_SIGNALS or entry_price <= 0 or exit_price <= 0:
        return None
    move_pct = pct_change(entry_price, exit_price)
    directional_move_pct = move_pct if signal_text == "LONG" else -move_pct
    return directional_move_pct - (TAKER_FEE_PCT * 2.0)


def compute_consensus(provider_results, fear_greed):
    counts = {signal: 0 for signal in VALID_SIGNALS}
    total_score = 0
    vote_summary = []
    sources = []
    for result in provider_results:
        signal_value = result["signal"]
        counts[signal_value] += 1
        total_score += SIGNAL_SCORE_MAP[signal_value]
        vote_summary.append(f"{result['provider']}={signal_value}")
        for source in result.get("sources", []):
            tagged = f"{result['provider']}: {source}"
            if tagged not in sources:
                sources.append(tagged)
            if len(sources) >= 3:
                break
    if total_score >= CONSENSUS_SCORE_THRESHOLD:
        final_signal = "SHORT"
    elif total_score <= -CONSENSUS_SCORE_THRESHOLD:
        final_signal = "LONG"
    else:
        final_signal = "NO_TRADE"
    why_parts = [
        (
            f"Consensus score {total_score:+d} using SHORT=+1, NO_TRADE=0, LONG=-1. "
            f"Trade threshold is +/-{CONSENSUS_SCORE_THRESHOLD}. Votes: {', '.join(vote_summary)}."
        )
    ]
    fear_summary = fear_greed.get("value", "")
    fear_classification = fear_greed.get("classification", "")
    if fear_summary or fear_classification:
        why_parts.append(
            f"Fear & Greed background metric: {fear_summary or 'n/a'} {fear_classification}".strip() + "."
        )
    return {
        "signal": final_signal,
        "why": " ".join(why_parts),
        "sources": sources[:3],
        "score": total_score,
        "counts": counts,
    }


def market_feed_status(market_state):
    last_message_at = float(market_state.get("last_message_at") or 0.0)
    last_error = str(market_state.get("last_error") or "").strip()
    if last_message_at > 0:
        feed_age = max(0.0, now_ts() - last_message_at)
        if feed_age <= FEED_STALE_AFTER_SECONDS and not last_error:
            return {"label": "HL Feed: LIVE", "detail": f"last poll {feed_age:.1f}s ago", "style": "green"}
        return {"label": "HL Feed: STALE", "detail": f"last poll {feed_age:.1f}s ago", "style": "yellow"}
    return {"label": "HL Feed: DISCONNECTED", "detail": last_error or "no market data yet", "style": "red"}


class MarketFeed:
    def __init__(self, stop_event):
        self.stop_event = stop_event
        self.lock = threading.Lock()
        self.history = deque()
        self.current_bid = 0.0
        self.current_ask = 0.0
        self.current_mid = 0.0
        self.current_spread_bps = 0.0
        self.first_message_at = 0.0
        self.last_message_at = 0.0
        self.last_error = ""
        self.thread = None

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._loop, daemon=True, name="btc-market-feed")
        self.thread.start()

    def stop(self):
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)

    def _prune_locked(self, ts):
        cutoff = ts - PRICE_HISTORY_SECONDS
        while self.history and self.history[0]["ts"] < cutoff:
            self.history.popleft()

    def _record_quote(self, snapshot):
        ts = now_ts()
        bid = float(snapshot["best_bid"])
        ask = float(snapshot["best_ask"])
        mid = float(snapshot["mid"])
        spread_bps = (((ask - bid) / mid) * 10000.0) if mid > 0 and ask >= bid else 0.0
        with self.lock:
            if not self.first_message_at:
                self.first_message_at = ts
            self.current_bid = bid
            self.current_ask = ask
            self.current_mid = mid
            self.current_spread_bps = spread_bps
            self.history.append({"ts": ts, "mid": mid})
            self._prune_locked(ts)
            self.last_message_at = ts
            self.last_error = ""

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

    def _loop(self):
        while not self.stop_event.is_set():
            try:
                self._record_quote(self.fetch_book_snapshot())
            except Exception as exc:
                with self.lock:
                    self.last_error = str(exc)
            self.stop_event.wait(MARKET_POLL_INTERVAL_SECONDS)

    def get_state(self):
        with self.lock:
            return {
                "history": list(self.history),
                "bid": float(self.current_bid or 0.0),
                "ask": float(self.current_ask or 0.0),
                "mid": float(self.current_mid or 0.0),
                "spread_bps": float(self.current_spread_bps or 0.0),
                "first_message_at": self.first_message_at,
                "last_message_at": self.last_message_at,
                "last_error": self.last_error,
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


class SandboxTrader:
    def __init__(self, market, settings):
        self.market = market
        self.settings = settings
        self.start_time = now_ts()
        self.build_info = get_local_build_info()
        self.available = STARTING_BALANCE_USDC
        self.position = None
        self.trades = deque(maxlen=20)
        self.logs = deque(maxlen=80)
        self.last_signal = "PENDING"
        self.last_signal_why = ""
        self.last_signal_sources = []
        self.last_signal_score = 0
        self.last_signal_at = 0.0
        self.next_signal_at = self.start_time
        self.last_signal_error = ""
        self.last_provider_results = {}
        self.last_provider_errors = {}
        self.pending_strategy_evaluations = []
        self.state_path = STATE_PATH
        self.legacy_state_path = LEGACY_STATE_PATH
        self.trades_csv_path = TRADES_CSV_PATH
        self.ai_responses_csv_path = AI_RESPONSES_CSV_PATH
        self.strategy_returns_csv_path = STRATEGY_RETURNS_CSV_PATH
        self.last_state_save_at = 0.0
        self.resume_note = ""
        self.lock = threading.Lock()
        os.makedirs(SETUP_DIR, exist_ok=True)
        os.makedirs(HISTORY_DIR, exist_ok=True)
        self._ensure_trades_csv()
        self._ensure_ai_responses_csv()
        self._ensure_strategy_returns_csv()
        restored = self._restore_state()
        self.log("BTC sandbox runner started.")
        self.log(f"Local build -> {self.build_info['label']} | {self.build_info['modified_at']}")
        self.log(f"Running file -> {self.build_info['file_path']}")
        self.log(f"Web dashboard build -> {WEB_DIST_DIR}")
        if restored:
            self.log("Recovered state from state file.")
            if self.resume_note:
                self.log(self.resume_note)
        self.persist_state(force=True)

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

    def _ensure_ai_responses_csv(self):
        if os.path.exists(self.ai_responses_csv_path):
            return
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
                detail = result["signal"]
                if result.get("why"):
                    detail += f" | {result['why']}"
                row.append(detail)
            else:
                row.append(provider_error_map.get(provider, "NO_RESPONSE"))
        consensus_cell = consensus["signal"]
        if consensus.get("why"):
            consensus_cell += f" | {consensus['why']}"
        row.append(consensus_cell)
        with open(self.ai_responses_csv_path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(row)

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
            result_pct = signal_return_pct(signal_value, entry_price, exit_price)
            row.extend([signal_value, "" if result_pct is None else f"{result_pct:.6f}"])
        consensus_signal = str(signals.get("consensus", "")).strip().upper()
        consensus_return_pct = signal_return_pct(consensus_signal, entry_price, exit_price)
        row.extend([consensus_signal, "" if consensus_return_pct is None else f"{consensus_return_pct:.6f}"])
        with open(self.strategy_returns_csv_path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(row)

    def log(self, message):
        ts = now_ts()
        with self.lock:
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
            "last_signal_sources": self.last_signal_sources,
            "last_signal_score": self.last_signal_score,
            "last_provider_results": self.last_provider_results,
            "last_provider_errors": self.last_provider_errors,
            "last_signal_at": self.last_signal_at,
            "next_signal_at": self.next_signal_at,
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
            payload = json.loads(Path(state_path).read_text(encoding="utf-8"))
        except Exception:
            return False
        if not isinstance(payload, dict):
            return False
        try:
            saved_at = float(payload.get("saved_at", 0.0) or 0.0)
            self.available = float(payload.get("available", self.available))
            position = payload.get("position")
            self.position = position if isinstance(position, dict) else None
            self.trades = deque(payload.get("trades", []), maxlen=20)
            self.logs = deque(payload.get("logs", []), maxlen=80)
            self.last_signal = str(payload.get("last_signal", self.last_signal))
            self.last_signal_why = str(payload.get("last_signal_why", self.last_signal_why))
            self.last_signal_sources = list(payload.get("last_signal_sources", []))
            self.last_signal_score = int(payload.get("last_signal_score", self.last_signal_score) or 0)
            raw_provider_results = payload.get("last_provider_results", {})
            self.last_provider_results = raw_provider_results if isinstance(raw_provider_results, dict) else {}
            raw_provider_errors = payload.get("last_provider_errors", {})
            self.last_provider_errors = raw_provider_errors if isinstance(raw_provider_errors, dict) else {}
            self.last_signal_at = float(payload.get("last_signal_at", self.last_signal_at) or 0.0)
            self.next_signal_at = float(payload.get("next_signal_at", self.next_signal_at) or self.start_time)
            pending = payload.get("pending_strategy_evaluations", [])
            self.pending_strategy_evaluations = pending if isinstance(pending, list) else []
        except Exception:
            return False
        now = now_ts()
        if self.next_signal_at <= now:
            missed_by = max(0.0, now - self.next_signal_at)
            missed_checks = max(1, int(missed_by // SIGNAL_INTERVAL_SECONDS) + 1)
            self.next_signal_at = now
            if saved_at > 0:
                offline_for = max(0.0, now - saved_at)
                self.resume_note = (
                    f"Missed {missed_checks} scheduled check(s) while offline "
                    f"({offline_for:.0f}s away, overdue by {missed_by:.0f}s). "
                    "Catching up as soon as market data is ready."
                )
        return True

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

    def _signal_ready_state(self):
        state = self.market.get_state()
        current_mid = float(state["mid"] or 0.0)
        last_message_at = float(state["last_message_at"] or 0.0)
        if current_mid <= 0 or last_message_at <= 0:
            return False, state["last_error"] or "Market feed not ready yet."
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
                data, _raw = post_json(
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
                    time.sleep(OPENAI_RETRY_DELAY_SECONDS)
                    continue
                raise
        if data is None:
            raise last_exc or RuntimeError(f"{provider} request failed")
        parsed = extract_signal_response({"text": parse_text(data), "raw": data})
        signal_value = parsed["signal"].strip().upper()
        if signal_value not in VALID_SIGNALS:
            raw_preview = json.dumps(data, ensure_ascii=True)[:240]
            raise RuntimeError(f"Unexpected {provider} response: {signal_value or 'EMPTY'} | raw={raw_preview}")
        return {
            "provider": provider,
            "model": model,
            "signal": signal_value,
            "why": parsed["why"],
            "sources": parsed["sources"],
        }

    def _query_openai_signal(self, prompt_text):
        api_key = self.settings.get("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY missing from settings.txt")
        model = self.settings.get("OPENAI_MODEL", OPENAI_MODEL_DEFAULT).strip() or OPENAI_MODEL_DEFAULT
        started = time.perf_counter()
        result = self._call_with_retries(
            "openai",
            model,
            OPENAI_RESPONSES_URL,
            {"model": model, "input": prompt_text, "max_output_tokens": 1200},
            {"Authorization": f"Bearer {api_key}"},
            extract_openai_output_text,
        )
        result["elapsed_seconds"] = time.perf_counter() - started
        return result

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
        started = time.perf_counter()
        result = self._call_with_retries(
            "gemini",
            model,
            request_url,
            {
                "contents": [{"parts": [{"text": prompt_text}]}],
                "generationConfig": {"maxOutputTokens": 1200},
            },
            {"Content-Type": "application/json"},
            extract_gemini_output_text,
        )
        result["elapsed_seconds"] = time.perf_counter() - started
        return result

    def _query_claude_signal(self, prompt_text):
        api_key = self.settings.get("CLAUDE_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("CLAUDE_API_KEY missing from settings.txt")
        model = self.settings.get("CLAUDE_MODEL", CLAUDE_MODEL_DEFAULT).strip() or CLAUDE_MODEL_DEFAULT
        started = time.perf_counter()

        def run_for_model(model_name):
            return self._call_with_retries(
                "claude",
                model_name,
                CLAUDE_MESSAGES_URL,
                {
                    "model": model_name,
                    "max_tokens": 600,
                    "messages": [{"role": "user", "content": prompt_text}],
                },
                {
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                },
                extract_claude_output_text,
            )

        try:
            result = run_for_model(model)
        except Exception as exc:
            fallback_model = model[: -len("-latest")] if model.endswith("-latest") else ""
            if not fallback_model or fallback_model == model:
                raise
            self.log(f"claude retrying with fallback model -> {fallback_model}")
            try:
                result = run_for_model(fallback_model)
            except Exception as fallback_exc:
                raise RuntimeError(f"{exc} | fallback {fallback_model} failed: {fallback_exc}") from fallback_exc
        result["elapsed_seconds"] = time.perf_counter() - started
        return result

    def _query_perplexity_signal(self, prompt_text):
        api_key = self.settings.get("PERPLEXITY_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("PERPLEXITY_API_KEY missing from settings.txt")
        model = self.settings.get("PERPLEXITY_MODEL", PERPLEXITY_MODEL_DEFAULT).strip() or PERPLEXITY_MODEL_DEFAULT
        started = time.perf_counter()
        result = self._call_with_retries(
            "perplexity",
            model,
            PERPLEXITY_SONAR_URL,
            {
                "model": model,
                "messages": [{"role": "user", "content": prompt_text}],
                "max_tokens": 600,
                "web_search_options": {"search_context_size": "low"},
            },
            {"Authorization": f"Bearer {api_key}"},
            extract_chat_completion_text,
        )
        result["elapsed_seconds"] = time.perf_counter() - started
        return result

    def _query_grok_signal(self, prompt_text):
        api_key = self.settings.get("GROK_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("GROK_API_KEY missing from settings.txt")
        model = self.settings.get("GROK_MODEL", GROK_MODEL_DEFAULT).strip() or GROK_MODEL_DEFAULT
        started = time.perf_counter()
        result = self._call_with_retries(
            "grok",
            model,
            GROK_RESPONSES_URL,
            {"model": model, "input": prompt_text, "max_output_tokens": 1200},
            {"Authorization": f"Bearer {api_key}"},
            extract_openai_output_text,
        )
        result["elapsed_seconds"] = time.perf_counter() - started
        return result

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
        with ThreadPoolExecutor(max_workers=len(AI_PROVIDER_ORDER)) as executor:
            futures = {
                executor.submit(provider_methods[provider], prompt_text): provider
                for provider in AI_PROVIDER_ORDER
            }
            for future in as_completed(futures):
                provider = futures[future]
                try:
                    provider_results.append(future.result())
                except Exception as exc:
                    provider_errors[provider] = str(exc)
                    self.log(f"{provider} signal error -> {exc}")
        if not provider_results:
            error = RuntimeError(
                "All AI providers failed: "
                + " | ".join(f"{provider}: {error_text}" for provider, error_text in provider_errors.items())
            )
            error.provider_errors = provider_errors
            raise error
        provider_results.sort(key=lambda item: AI_PROVIDER_ORDER.index(item["provider"]))
        consensus = compute_consensus(provider_results, fear_greed)
        consensus["provider_results"] = provider_results
        consensus["provider_errors"] = provider_errors
        consensus["fear_greed"] = fear_greed
        consensus["snapshot"] = snapshot
        consensus["prompt_text"] = prompt_text
        return consensus

    def _queue_strategy_evaluation(self, ts, entry_price, provider_results, consensus):
        entry_price = safe_float(entry_price, 0.0)
        if entry_price <= 0:
            self.log("Strategy return log skipped: entry price unavailable.")
            return
        signal_map = {provider: "" for provider in AI_PROVIDER_ORDER}
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

    def maybe_run_signal(self):
        if now_ts() < self.next_signal_at:
            return
        ready, reason = self._signal_ready_state()
        if not ready:
            self._defer_signal(reason, STARTUP_SIGNAL_RETRY_SECONDS)
            return
        signal_time = now_ts()
        try:
            signal_result = self.query_signal()
            signal_value = signal_result["signal"]
            self.last_signal_why = signal_result["why"]
            self.last_signal_sources = list(signal_result["sources"])
            self.last_signal_score = int(signal_result.get("score", 0))
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
                    f"{provider_result['provider']} -> {provider_result['signal']} | "
                    f"{provider_result['model']} | {provider_result.get('elapsed_seconds', 0.0):.2f}s"
                )
            self.log(f"AI consensus signal -> {signal_value} | score {self.last_signal_score:+d}")
            if self.last_signal_why:
                self.log(f"AI consensus why -> {self.last_signal_why}")
        except Exception as exc:
            self.last_signal_why = ""
            self.last_signal_sources = []
            self.last_provider_errors = dict(getattr(exc, "provider_errors", {}))
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

    def api_state(self):
        market_state = self.market.get_state()
        feed = market_feed_status(market_state)
        provider_cards = []
        for provider in AI_PROVIDER_ORDER:
            result = self.last_provider_results.get(provider, {})
            provider_cards.append(
                {
                    "provider": provider,
                    "signal": result.get("signal", "PENDING"),
                    "why": result.get("why", ""),
                    "model": result.get("model", ""),
                    "elapsed_seconds": round(float(result.get("elapsed_seconds", 0.0) or 0.0), 3),
                    "error": self.last_provider_errors.get(provider, ""),
                }
            )
        recent_trades = list(self.trades)[:8]
        recent_logs = list(self.logs)[:18]
        next_signal_in = max(0.0, self.next_signal_at - now_ts())
        runtime_seconds = int(now_ts() - self.start_time)
        return {
            "app": {
                "title": "muzz.world sandbox control room",
                "subtitle": LATEST_CHANGE_SUMMARY,
                "build_label": self.build_info["label"],
                "build_modified_at": self.build_info["modified_at"],
                "runtime_seconds": runtime_seconds,
            },
            "quotes": {
                "bid": market_state["bid"],
                "ask": market_state["ask"],
                "mid": market_state["mid"],
                "spread_bps": market_state["spread_bps"],
                "last_tick_at": market_state["last_message_at"],
                "feed_label": feed["label"],
                "feed_detail": feed["detail"],
                "feed_style": feed["style"],
            },
            "account": {
                "available": self.available,
                "equity": self.equity(),
                "live_pnl": self.live_pnl(),
                "position_side": self.position["side"] if self.position else "FLAT",
                "next_signal_at": self.next_signal_at,
                "next_signal_in": next_signal_in,
                "leverage": LEVERAGE,
                "stack_fraction": STACK_FRACTION,
                "stop_loss_usdc": STOP_LOSS_USDC,
            },
            "signal": {
                "last_signal": self.last_signal,
                "last_signal_why": self.last_signal_why,
                "last_signal_score": self.last_signal_score,
                "last_signal_at": self.last_signal_at,
                "last_signal_sources": self.last_signal_sources,
                "last_error": self.last_signal_error,
                "providers": provider_cards,
            },
            "position": self.position,
            "trades": recent_trades,
            "logs": recent_logs,
        }


def read_csv_rows(path, limit=200):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if limit and limit > 0:
        rows = rows[-limit:]
    rows.reverse()
    return rows


class LocalDashboardApp:
    def __init__(self):
        self.stop_event = threading.Event()
        self.settings = load_settings(resolve_settings_path())
        self.market = MarketFeed(self.stop_event)
        self.trader = SandboxTrader(self.market, self.settings)
        self.engine_thread = None
        self.server = None
        self.server_thread = None
        self.host = "127.0.0.1"
        self.port = DEFAULT_HTTP_PORT

    def start(self):
        self.market.start()
        self.engine_thread = threading.Thread(target=self._engine_loop, daemon=True, name="sandbox-engine")
        self.engine_thread.start()
        self._start_server()

    def stop(self):
        self.stop_event.set()
        try:
            self.trader.persist_state(force=True)
        except Exception:
            pass
        if self.server:
            try:
                self.server.shutdown()
            except Exception:
                pass
            try:
                self.server.server_close()
            except Exception:
                pass
        self.market.stop()
        if self.engine_thread and self.engine_thread.is_alive():
            self.engine_thread.join(timeout=2)

    def _engine_loop(self):
        while not self.stop_event.is_set():
            try:
                self.trader.maybe_finalize_strategy_returns()
                self.trader.maybe_stop_loss()
                self.trader.maybe_run_signal()
                self.trader.persist_state()
            except Exception as exc:
                self.trader.log(f"Main loop error: {exc}")
            self.stop_event.wait(1)

    def _choose_port(self):
        for port in range(DEFAULT_HTTP_PORT, DEFAULT_HTTP_PORT + HTTP_PORT_SEARCH_ATTEMPTS):
            try:
                candidate = ThreadingHTTPServer((self.host, port), self._make_handler())
                return candidate, port
            except OSError:
                continue
        raise RuntimeError("Unable to bind a local HTTP port.")

    def _start_server(self):
        self.server, self.port = self._choose_port()
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True, name="local-dashboard")
        self.server_thread.start()

    def _make_handler(self):
        app = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format, *_args):
                return

            def do_GET(self):
                parsed = urllib_parse.urlparse(self.path)
                path = parsed.path
                if path == "/api/state":
                    return self._send_json(app.trader.api_state())
                if path == "/api/trades":
                    return self._send_json({"rows": read_csv_rows(TRADES_CSV_PATH, limit=200)})
                if path == "/api/strategy-returns":
                    return self._send_json({"rows": read_csv_rows(STRATEGY_RETURNS_CSV_PATH, limit=500)})
                if path == "/api/ai-responses":
                    return self._send_json({"rows": read_csv_rows(AI_RESPONSES_CSV_PATH, limit=200)})
                return self._serve_static(path)

            def _send_json(self, payload, status=200):
                body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

            def _serve_static(self, path):
                dist_dir = Path(WEB_DIST_DIR)
                if not dist_dir.exists():
                    body = (
                        "<html><body style='font-family: monospace; background:#111; color:#eee; padding:24px'>"
                        "<h2>Web UI build missing</h2>"
                        "<p>Run <code>npm run build</code> inside <code>webui</code>, then restart <code>runner.py</code>.</p>"
                        "</body></html>"
                    ).encode("utf-8")
                    self.send_response(500)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                requested = path.lstrip("/") or "index.html"
                candidate = (dist_dir / requested).resolve()
                if not str(candidate).startswith(str(dist_dir.resolve())):
                    return self._send_json({"error": "invalid path"}, status=400)
                if candidate.is_file():
                    return self._write_file(candidate)
                return self._write_file(dist_dir / "index.html")

            def _write_file(self, file_path):
                data = file_path.read_bytes()
                content_type, _ = mimetypes.guess_type(str(file_path))
                self.send_response(200)
                self.send_header("Content-Type", content_type or "application/octet-stream")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(data)

        return Handler

    def open_browser(self):
        url = f"http://{self.host}:{self.port}"
        deadline = time.time() + 8
        while time.time() < deadline:
            try:
                with urllib_request.urlopen(f"{url}/api/state", timeout=1):
                    break
            except Exception:
                time.sleep(0.2)
        print(f"Local dashboard: {url}")
        try:
            webbrowser.open(url, new=1)
            self.trader.log(f"Opened browser -> {url}")
        except Exception as exc:
            self.trader.log(f"Browser auto-open failed -> {exc}")
            print(f"Open this URL manually: {url}")


def main():
    app = LocalDashboardApp()

    def handle_stop(_signum=None, _frame=None):
        app.stop()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    try:
        app.start()
        app.open_browser()
        while not app.stop_event.is_set():
            time.sleep(1)
    finally:
        app.stop()
        print("\nStopped BTC sandbox runner.")


if __name__ == "__main__":
    main()
