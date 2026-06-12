#!/usr/bin/env python3
"""
BTC-only sandbox runner driven by periodic OpenAI directional calls.
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
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib import error as urllib_error
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
SETTINGS_PATH = os.path.join(BASE_DIR, "settings.txt")
LOG_CSV_PATH = os.path.join(BASE_DIR, "log.csv")
OPENAI_DEBUG_CSV_PATH = os.path.join(BASE_DIR, "openai_debug.csv")
TRADES_CSV_PATH = os.path.join(BASE_DIR, "trades.csv")
STATE_PATH = os.path.join(BASE_DIR, "state.txt")
SNAPSHOT_DIR = r"G:\My Drive\+tradebot"
PANEL_BORDER_STYLE = "rgb(237,125,175)"
HEADING_STYLE = "bold cyan"
BODY_STYLE = "white"
BTC_PERP = "BTC"
HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
OPENAI_MODEL_DEFAULT = "gpt-4.1"
STARTING_BALANCE_USDC = 10000.0
STACK_FRACTION = 0.95
LEVERAGE = 5.0
TAKER_FEE_PCT = 0.015
STOP_LOSS_USDC = 250.0
SIGNAL_INTERVAL_SECONDS = 15 * 60
DISPLAY_COLUMNS = 15
DISPLAY_MINUTE_SECONDS = 60
PRICE_HISTORY_SECONDS = (DISPLAY_COLUMNS + 2) * DISPLAY_MINUTE_SECONDS
FEED_STALE_AFTER_SECONDS = 20.0
LIVE_REFRESH_HZ = 2
OPENAI_TIMEOUT_SECONDS = 90
OPENAI_MAX_ATTEMPTS = 3
OPENAI_RETRY_DELAY_SECONDS = 3
SIGNAL_RETRY_DELAY_SECONDS = 30
STARTUP_SIGNAL_RETRY_SECONDS = 5
STATE_SAVE_INTERVAL_SECONDS = 3
SNAPSHOT_LEAD_SECONDS = 20
PROMPT = """I am trading on the Hyperliquid BTC Perpetual market using Taker orders and my rates a 0.015% and 0.015% each way, so looking to clear 0.03% on any trade to make profit.

Based on fresh market data, recent news, price action, momentum, volatility, and market structure, choose the single best directional trade for the next 15 minutes. Prefer LONG or SHORT whenever one direction appears to have a positive expected edge over the next 15 minutes.

Prioritize BTC price action and immediate market structure e.g. last 1h BTC price action, last 15m and 5m momentum.
Prioritize current BTC price action, momentum, and market structure over commentary. Only use recent, high-quality news sources. Ignore stale articles, evergreen explainers, and low-quality blog spam. Prefer sources from the last 6 hours unless an older event is still clearly driving BTC today.

Use NO_TRADE only when:
• Neither LONG nor SHORT appears likely to achieve +0.03% net profit.
• The directional edge is too small to overcome costs.
• News risk, event risk, or abnormal volatility makes short-term direction genuinely unclear.

Do not default to NO_TRADE simply because confidence is below 100%. If one direction has a measurable advantage, choose it.

Return valid JSON only with this exact shape:
{"signal":"LONG|SHORT|NO_TRADE","why":"1-3 short sentences","sources":["up to 3 short source strings, freshest first"]}"""
VALID_SIGNALS = {"LONG", "SHORT", "NO_TRADE"}
console = Console()


def now_ts():
    return time.time()


def format_ts(ts):
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")


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
        self.state_path = STATE_PATH
        self.snapshot_dir = SNAPSHOT_DIR
        self.last_snapshot_key = ""
        self.last_state_save_at = 0.0
        self.resume_note = ""
        self.lock = threading.Lock()
        self.log_csv_path = LOG_CSV_PATH
        self.openai_debug_csv_path = OPENAI_DEBUG_CSV_PATH
        self.trades_csv_path = TRADES_CSV_PATH
        self._reset_log_csv()
        self._reset_openai_debug_csv()
        self._ensure_trades_csv()
        set_terminal_title(
            f"muzz.world | Fingerprint {self.build_info['label']} | {self.build_info['modified_at']}"
        )
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

    def _reset_log_csv(self):
        with open(self.log_csv_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timestamp_utc", "epoch_ts", "message"])

    def _append_csv(self, ts, message):
        with open(self.log_csv_path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([iso_utc(ts), f"{ts:.6f}", message])

    def _reset_openai_debug_csv(self):
        with open(self.openai_debug_csv_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "timestamp_utc",
                    "epoch_ts",
                    "request_url",
                    "request_headers",
                    "request_body",
                    "response_text",
                    "parsed_signal",
                    "parsed_why",
                    "parsed_sources",
                    "error",
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

    def _append_openai_debug_csv(
        self,
        ts,
        request_url,
        request_headers,
        request_body,
        response_text,
        parsed_signal,
        parsed_why,
        parsed_sources,
        error_text,
    ):
        with open(self.openai_debug_csv_path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    iso_utc(ts),
                    f"{ts:.6f}",
                    request_url,
                    request_headers,
                    request_body,
                    response_text,
                    parsed_signal,
                    parsed_why,
                    json.dumps(parsed_sources, ensure_ascii=True),
                    error_text,
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

    def log(self, message):
        ts = now_ts()
        self.logs.appendleft(f"{format_ts(ts)} {message}")
        self._append_csv(ts, message)

    def _state_payload(self):
        return {
            "saved_at": now_ts(),
            "available": self.available,
            "position": self.position,
            "trades": list(self.trades),
            "logs": list(self.logs),
            "last_signal": self.last_signal,
            "last_signal_why": self.last_signal_why,
            "last_signal_sources": list(self.last_signal_sources),
            "last_signal_at": self.last_signal_at,
            "next_signal_at": self.next_signal_at,
            "last_snapshot_key": self.last_snapshot_key,
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
        if not os.path.exists(self.state_path):
            return False
        try:
            raw = Path(self.state_path).read_text(encoding="utf-8")
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
            sources = payload.get("last_signal_sources", [])
            self.last_signal_sources = [str(item) for item in sources] if isinstance(sources, list) else []
            self.last_signal_at = float(payload.get("last_signal_at", self.last_signal_at) or 0.0)
            self.next_signal_at = float(payload.get("next_signal_at", self.next_signal_at) or self.start_time)
            self.last_snapshot_key = str(payload.get("last_snapshot_key", self.last_snapshot_key))
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
        os.makedirs(self.snapshot_dir, exist_ok=True)
        stamp = datetime.fromtimestamp(self.next_signal_at, tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        png_path = os.path.join(self.snapshot_dir, f"tradebot_{stamp}.png")
        text_path = os.path.join(self.snapshot_dir, f"tradebot_{stamp}.txt")
        dashboard_text = render_dashboard_text(self, market)
        try:
            write_text_image(dashboard_text, png_path)
            self.last_snapshot_key = snapshot_key
            self.log(f"Saved dashboard snapshot -> {png_path}")
            self.persist_state(force=True)
            return
        except Exception as exc:
            Path(text_path).write_text(dashboard_text, encoding="utf-8")
            self.last_snapshot_key = snapshot_key
            self.log(f"Saved text snapshot -> {text_path} | png unavailable: {exc}")
            self.persist_state(force=True)

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

    def query_signal(self):
        api_key = self.settings.get("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY missing from settings.txt")
        model = self.settings.get("OPENAI_MODEL", OPENAI_MODEL_DEFAULT).strip() or OPENAI_MODEL_DEFAULT
        payload = {
            "model": model,
            "input": PROMPT,
            "tools": [{"type": "web_search_preview"}],
            "max_output_tokens": 10000,
        }
        debug_ts = now_ts()
        debug_request_headers = json.dumps(
            {
                "Content-Type": "application/json",
                "Authorization": "Bearer ***redacted***",
            },
            ensure_ascii=True,
        )
        debug_request_body = json.dumps(payload, ensure_ascii=True)
        last_exc = None
        data = None
        response_text = ""
        for attempt in range(1, OPENAI_MAX_ATTEMPTS + 1):
            response_text = ""
            try:
                data, response_text = post_json(
                    OPENAI_RESPONSES_URL,
                    payload,
                    timeout=OPENAI_TIMEOUT_SECONDS,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                    },
                    return_raw=True,
                )
                break
            except Exception as exc:
                last_exc = exc
                self._append_openai_debug_csv(
                    debug_ts,
                    OPENAI_RESPONSES_URL,
                    debug_request_headers,
                    debug_request_body,
                    response_text,
                    "",
                    "",
                    [],
                    f"attempt {attempt}/{OPENAI_MAX_ATTEMPTS}: {exc}",
                )
                retryable = "timeout" in str(exc).lower() or "network error" in str(exc).lower()
                if retryable and attempt < OPENAI_MAX_ATTEMPTS:
                    self.log(
                        f"OpenAI request issue on attempt {attempt}/{OPENAI_MAX_ATTEMPTS}: {exc}. Retrying..."
                    )
                    time.sleep(OPENAI_RETRY_DELAY_SECONDS)
                    continue
                raise
        if data is None:
            raise last_exc or RuntimeError("OpenAI request failed")
        parsed_response = extract_signal_response(data)
        text = parsed_response["signal"].strip().upper()
        why = parsed_response["why"]
        sources = parsed_response["sources"]
        self._append_openai_debug_csv(
            debug_ts,
            OPENAI_RESPONSES_URL,
            debug_request_headers,
            debug_request_body,
            response_text,
            text,
            why,
            sources,
            "",
        )
        if text not in VALID_SIGNALS:
            raw_preview = json.dumps(data, ensure_ascii=True)[:240]
            raise RuntimeError(f"Unexpected OpenAI response: {text or 'EMPTY'} | raw={raw_preview}")
        return {
            "signal": text,
            "why": why,
            "sources": sources,
        }

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
            self.log(f"OpenAI signal -> {signal_value}")
            if self.last_signal_why:
                self.log(f"OpenAI why -> {self.last_signal_why}")
            if self.last_signal_sources:
                self.log(f"OpenAI sources -> {' | '.join(self.last_signal_sources)}")
        except Exception as exc:
            self.last_signal_why = ""
            self.last_signal_sources = []
            self._defer_signal(f"OpenAI signal error -> {exc}", SIGNAL_RETRY_DELAY_SECONDS)
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
        return "green"
    if value < 0:
        return "red"
    return "white"


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
    cycle_seconds = max(1, SIGNAL_INTERVAL_SECONDS)
    remaining = max(0.0, trader.next_signal_at - now_ts())
    remaining = max(0.0, min(cycle_seconds, remaining))
    remaining_ratio = remaining / float(cycle_seconds)
    minutes = int(remaining // 60)
    seconds = int(remaining % 60)
    countdown_text = f"{minutes:02d}:{seconds:02d}"
    label = f"Next 15m Check {countdown_text}"
    console_width = max(70, int(getattr(console, "width", 120) or 120))
    bar_width = max(len(label) + 8, console_width - 6)
    filled = int(round(remaining_ratio * bar_width))
    filled = max(0, min(bar_width, filled))
    if hasattr(Text(""), "stylize"):
        row_chars = [" "] * bar_width
        start = max(0, (bar_width - len(label)) // 2)
        end = min(bar_width, start + len(label))
        for idx, char in enumerate(label):
            pos = start + idx
            if pos >= bar_width:
                break
            row_chars[pos] = char
        row = Text("".join(row_chars), style="on rgb(95,88,28)")
        if filled > 0:
            row.stylize("on rgb(170,155,55)", 0, filled)
        row.stylize("bold black", start, end)
        return row
    else:
        bar = ("#" * filled) + ("-" * (bar_width - filled))
        return Text(f"{bar} {countdown_text}")


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
    labels = [f"-{minute}m" for minute in range(DISPLAY_COLUMNS, 0, -1)]
    for label in labels:
        table.add_column(label, justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    prices = market_state["minute_prices"]
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
    table.add_column("Exit", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Entry USDC", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Exit USDC", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    table.add_column("Net PnL", justify="right", no_wrap=True, header_style=HEADING_STYLE, style=BODY_STYLE)
    if not trader.trades:
        table.add_row("-", "No trades yet", "-", "-", "-", "-")
        return table
    for trade in list(trader.trades)[:8]:
        pnl_style = style_pct(float(trade["net_pnl"]))
        table.add_row(
            format_ts(float(trade["timestamp"])),
            trade["side"],
            trade["reason"],
            f"{float(trade['entry_usdc']):,.2f}",
            f"{float(trade['exit_usdc']):,.2f}",
            Text(f"{float(trade['net_pnl']):,.4f}", style=pnl_style),
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
    lines = [Text.assemble(("Signal: ", HEADING_STYLE), (trader.last_signal, BODY_STYLE))]
    if trader.last_signal_why:
        lines.append(Text.assemble(("Why: ", HEADING_STYLE), (trader.last_signal_why, BODY_STYLE)))
    if trader.last_signal_sources:
        lines.append(Text("Sources:", style=HEADING_STYLE))
        for index, source in enumerate(trader.last_signal_sources, start=1):
            lines.append(Text.assemble((f"{index}. ", HEADING_STYLE), (source, BODY_STYLE)))
    if len(lines) == 1 and trader.last_signal == "PENDING":
        lines.append(Text("No model rationale yet.", style=BODY_STYLE))
    return Group(*lines)


def build_dashboard(trader, market):
    state = market.get_state()
    market_state = {
        **state,
        "minute_prices": market.get_minute_prices(),
    }
    status_text = trader.last_signal_error or (state["last_error"] if state["last_error"] else ("Managing position." if trader.position else "Waiting for next signal."))
    header = [
        Text(
            f"muzz.world | Fingerprint {trader.build_info['label']} | {trader.build_info['modified_at']}",
            style=BODY_STYLE,
        ),
        Text(
            f"BTC only | Runtime {int(now_ts() - trader.start_time)}s | WS open {format_ts(state['last_open_at']) or 'n/a'}",
            style=HEADING_STYLE,
        ),
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
    settings = load_settings(SETTINGS_PATH)
    trader = SandboxTrader(market, settings)
    market.start()

    def handle_stop(_signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    def trader_loop():
        while not stop_event.is_set():
            try:
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
        with Live(build_dashboard(trader, market), console=console, refresh_per_second=LIVE_REFRESH_HZ, screen=True) as live:
            while not stop_event.is_set():
                live.update(build_dashboard(trader, market))
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
