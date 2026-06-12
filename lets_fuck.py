#!/usr/bin/env python3
"""
BTC-only sandbox runner driven by periodic OpenAI directional calls.
"""

import csv
import json
import os
import re
import signal
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request

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
BTC_PERP = "BTC"
HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
OPENAI_MODEL_DEFAULT = "gpt-4.1"
STARTING_BALANCE_USDC = 10000.0
STACK_FRACTION = 0.95
LEVERAGE = 5.0
TAKER_FEE_PCT = 0.015
SIGNAL_INTERVAL_SECONDS = 15 * 60
DISPLAY_COLUMNS = 15
DISPLAY_MINUTE_SECONDS = 60
PRICE_HISTORY_SECONDS = (DISPLAY_COLUMNS + 2) * DISPLAY_MINUTE_SECONDS
FEED_STALE_AFTER_SECONDS = 20.0
LIVE_REFRESH_HZ = 2
PROMPT = """I am trading on the Hyperliquid BTC Perpetual market using Taker orders and my rates a 0.015% and 0.015% each way, so looking to clear 0.03% on any trade to make profit.

Based on fresh market data, recent news, price action, momentum, volatility, and market structure, choose the single best directional trade for the next 15 minutes. Prefer LONG or SHORT whenever one direction appears to have a positive expected edge over the next 15 minutes.

Use NO_TRADE only when:
• Neither LONG nor SHORT appears likely to achieve +0.03% net profit.
• The directional edge is too small to overcome costs.
• News risk, event risk, or abnormal volatility makes short-term direction genuinely unclear.

Do not default to NO_TRADE simply because confidence is below 100%. If one direction has a measurable advantage, choose it.

Answer only:

LONG
SHORT
NO_TRADE"""
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
    except urllib_error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {raw}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Network error: {exc}") from exc
    parsed = json.loads(raw)
    if return_raw:
        return parsed, raw
    return parsed


def extract_signal_from_payload(payload):
    candidates = []

    def visit(value):
        if isinstance(value, str):
            candidates.append(value)
            return
        if isinstance(value, dict):
            for nested in value.values():
                visit(nested)
            return
        if isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(payload)
    for text in candidates:
        upper = text.upper().replace("`", " ")
        match = re.search(r"\b(NO_TRADE|LONG|SHORT)\b", upper)
        if match:
            return match.group(1)
    return ""


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
        self.available = STARTING_BALANCE_USDC
        self.position = None
        self.trades = deque(maxlen=12)
        self.logs = deque(maxlen=12)
        self.last_signal = "PENDING"
        self.last_signal_at = 0.0
        self.next_signal_at = self.start_time
        self.last_signal_error = ""
        self.lock = threading.Lock()
        self.log_csv_path = LOG_CSV_PATH
        self.openai_debug_csv_path = OPENAI_DEBUG_CSV_PATH
        self._reset_log_csv()
        self._reset_openai_debug_csv()
        self.log("BTC sandbox runner started.")

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
                    "error",
                ]
            )

    def _append_openai_debug_csv(self, ts, request_url, request_headers, request_body, response_text, parsed_signal, error_text):
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
                    error_text,
                ]
            )

    def log(self, message):
        ts = now_ts()
        self.logs.appendleft(f"{format_ts(ts)} {message}")
        self._append_csv(ts, message)

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
        response_text = ""
        try:
            data, response_text = post_json(
                OPENAI_RESPONSES_URL,
                payload,
                timeout=45,
                headers={
                    "Authorization": f"Bearer {api_key}",
                },
                return_raw=True,
            )
        except Exception as exc:
            self._append_openai_debug_csv(
                debug_ts,
                OPENAI_RESPONSES_URL,
                debug_request_headers,
                debug_request_body,
                response_text,
                "",
                str(exc),
            )
            raise
        text = extract_signal_from_payload(data).strip().upper()
        self._append_openai_debug_csv(
            debug_ts,
            OPENAI_RESPONSES_URL,
            debug_request_headers,
            debug_request_body,
            response_text,
            text,
            "",
        )
        if text not in VALID_SIGNALS:
            raw_preview = json.dumps(data, ensure_ascii=True)[:240]
            raise RuntimeError(f"Unexpected OpenAI response: {text or 'EMPTY'} | raw={raw_preview}")
        return text

    def maybe_run_signal(self):
        if now_ts() < self.next_signal_at:
            return
        signal_time = now_ts()
        try:
            signal_value = self.query_signal()
            self.last_signal_error = ""
            self.log(f"OpenAI signal -> {signal_value}")
        except Exception as exc:
            signal_value = None
            self.last_signal_error = str(exc)
            self.log(f"OpenAI signal error -> {exc}")
        self.last_signal_at = signal_time
        self.next_signal_at = signal_time + SIGNAL_INTERVAL_SECONDS
        if signal_value is None:
            self.last_signal = "NO_TRADE"
            if self.position:
                self.close_position("SIGNAL_ERROR_EXIT")
            return
        self.last_signal = signal_value
        self.process_signal(signal_value)

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
            "entry_usdc": float(self.position["initial_margin"]),
            "exit_usdc": float(self.position["initial_margin"]) + net_pnl,
            "gross_pnl": gross_pnl,
            "net_pnl": net_pnl,
            "fees_paid": entry_fee + exit_fee,
            "reason": reason,
            "seconds_open": now_ts() - float(self.position["entry_time"]),
        }
        self.trades.appendleft(trade)
        self.position = None
        self.log(
            f"{side} exit {reason} at {close_price:,.2f}. Gross {gross_pnl:.4f} | "
            f"Net {net_pnl:.4f} USDC | Fees {(entry_fee + exit_fee):.4f}"
        )


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


def build_summary_table(trader, market_state):
    table = Table.grid(expand=True)
    for _ in range(8):
        table.add_column(justify="left")
    position_side = trader.position["side"] if trader.position else "FLAT"
    next_signal_in = max(0, int(round(trader.next_signal_at - now_ts())))
    feed_age = max(0.0, now_ts() - market_state["last_message_at"]) if market_state["last_message_at"] else 9999.0
    table.add_row(
        f"[bold]Available[/bold]\n{trader.available:,.2f} USDC",
        f"[bold]Equity[/bold]\n{trader.equity():,.2f} USDC",
        f"[bold]Live PnL[/bold]\n{trader.live_pnl():,.2f} USDC",
        f"[bold]BTC Mid[/bold]\n{market_state['mid']:,.2f}" if market_state["mid"] else "[bold]BTC Mid[/bold]\n-",
        f"[bold]Signal[/bold]\n{trader.last_signal}",
        f"[bold]Next Ask[/bold]\n{next_signal_in}s",
        f"[bold]Position[/bold]\n{position_side}",
        f"[bold]Feed Age[/bold]\n{feed_age:.1f}s",
    )
    return table


def build_price_table(market_state):
    table = Table(expand=True, padding=(0, 0), pad_edge=False, collapse_padding=True, box=box.SIMPLE_HEAD)
    table.add_column("Perp", style="bold", no_wrap=True)
    labels = [f"-{minute}m" for minute in range(DISPLAY_COLUMNS, 0, -1)]
    for label in labels:
        table.add_column(label, justify="right", no_wrap=True)
    prices = market_state["minute_prices"]
    cells = []
    previous = None
    for price in prices:
        cells.append(render_price_cell(price, previous))
        previous = price if price is not None else previous
    table.add_row(Text(BTC_PERP, style="bold white"), *cells)
    return table


def build_position_table(trader, market_state):
    table = Table(expand=True, padding=(0, 0), pad_edge=False, collapse_padding=True, box=box.SIMPLE_HEAD)
    table.add_column("Perp", no_wrap=True)
    table.add_column("Side", no_wrap=True)
    table.add_column("Entry", justify="right", no_wrap=True)
    table.add_column("Current", justify="right", no_wrap=True)
    table.add_column("Gain %", justify="right", no_wrap=True)
    table.add_column("Opened", no_wrap=True)
    if not trader.position:
        table.add_row("-", "No active position", "-", "-", "-", "-")
        return table
    current_mid = float(market_state["mid"] or trader.position["entry_price"])
    direction = 1.0 if trader.position["side"] == "LONG" else -1.0
    pnl_pct = pct_change(float(trader.position["entry_price"]), current_mid) * direction
    pnl_text = Text(f"{pnl_pct:,.4f}%", style=style_pct(pnl_pct))
    table.add_row(
        BTC_PERP,
        trader.position["side"],
        f"{float(trader.position['entry_price']):,.2f}",
        f"{current_mid:,.2f}",
        pnl_text,
        format_ts(float(trader.position["entry_time"])),
    )
    return table


def build_trades_table(trader):
    table = Table(expand=True, padding=(0, 0), pad_edge=False, collapse_padding=True, box=box.SIMPLE_HEAD)
    table.add_column("Time", no_wrap=True)
    table.add_column("Side", no_wrap=True)
    table.add_column("Exit", no_wrap=True)
    table.add_column("Entry USDC", justify="right", no_wrap=True)
    table.add_column("Exit USDC", justify="right", no_wrap=True)
    table.add_column("Net PnL", justify="right", no_wrap=True)
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
        lines = ["No logs yet."]
    return "\n".join(lines)


def build_dashboard(trader, market):
    state = market.get_state()
    market_state = {
        **state,
        "minute_prices": market.get_minute_prices(),
    }
    status_text = trader.last_signal_error or (state["last_error"] if state["last_error"] else ("Managing position." if trader.position else "Waiting for next signal."))
    header = [
        Text("muzz.world", style="bold white"),
        Text(
            f"BTC only | Runtime {int(now_ts() - trader.start_time)}s | WS open {format_ts(state['last_open_at']) or 'n/a'}",
            style="cyan",
        ),
        Text(status_text, style="bold yellow" if (trader.last_signal_error or state["last_error"]) else "bold green"),
    ]
    return Group(
        *header,
        Rule("Account", style="white"),
        build_summary_table(trader, market_state),
        Rule("BTC 1m Tape", style="white"),
        build_price_table(market_state),
        Rule("Open Position", style="white"),
        build_position_table(trader, market_state),
        Rule("Recent Trades", style="white"),
        build_trades_table(trader),
        Rule("Action Log", style="white"),
        build_logs_panel(trader),
    )


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
                trader.maybe_run_signal()
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
        market.stop()
        console.print("\nStopped BTC sandbox runner.")


if __name__ == "__main__":
    main()
