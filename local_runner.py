#!/usr/bin/env python3
"""
Local terminal runner for the MuzzWorld 2m acceleration sandbox bot.

This script is intentionally self-contained:
- No Flask
- No Postgres
- No web UI
- Hyperliquid allMids websocket feed
- Real order-book snapshots for spread/liquidity checks
- Rich terminal dashboard

It simulates the current sandbox strategy locally so you can run and tune it
from your own machine/network.
"""

import argparse
import json
import math
import os
import re
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event, Lock, Thread

import requests
from rich.console import Console, Group
from rich.live import Live
from rich import box
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

try:
    from hyperliquid.info import Info
    from hyperliquid.utils import constants as hl_constants
    import websocket

    IMPORT_ERROR = ""
except Exception as exc:  # pragma: no cover
    Info = None
    hl_constants = None
    websocket = None
    IMPORT_ERROR = str(exc)


HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
MARKET_HISTORY_SECONDS = 3900
MARKET_STALE_AFTER_SECONDS = 20.0
META_REFRESH_SECONDS = 900.0
SCAN_INTERVAL_SECONDS = 2.0
XYZ_PERP_DEX = "xyz"
BOOK_SNAPSHOT_TIMEOUT_SECONDS = 1.5
MAX_BOOK_CHECKS_PER_SCAN = 3
BLOCK_WINDOW_SECONDS = 120
BLOCK_STEP_SECONDS = 5
BLOCK_COLUMN_LABELS = [f"-{sec}s" for sec in range(BLOCK_WINDOW_SECONDS, 0, -BLOCK_STEP_SECONDS)] + ["Latest"]
# Standard Hyperliquid perps use bare asset names. XYZ HIP-3 perps use `xyz:<ticker>` in the API.
CURATED_PERP_SYMBOLS = [
    "BTC",
    "ETH",
    "BNB",
    "XRP",
    "SOL",
    "TRX",
    "HYPE",
    "DOGE",
    "ZEC",
    "XMR",
    "CC",
    "XLM",
    "ADA",
    "LINK",
    "TON",
    "BCH",
    "HBAR",
    "LTC",
    "SUI",
    "AVAX",
    "xyz:GOLD",
    "xyz:NVDA",
    "xyz:AAPL",
    "xyz:GOOGL",
    "xyz:MSFT",
    "xyz:SILVER",
    "xyz:AMZN",
    "xyz:META",
    "xyz:TSLA",
    "xyz:NFLX",
]

console = Console()


def utc_now():
    return datetime.now(timezone.utc)


def format_timestamp(ts):
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")


def pct_change(from_price, to_price):
    if from_price <= 0:
        return 0.0
    return ((to_price - from_price) / from_price) * 100.0


def trim_float(value, digits=6):
    return round(float(value), digits)


def floor_to_decimals(value, decimals):
    factor = 10 ** max(0, int(decimals))
    return math.floor(float(value) * factor) / factor if factor > 0 else float(value)


def read_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def apply_fee(notional, fee_pct):
    return float(notional) * (float(fee_pct) / 100.0)


def apply_slippage(price, slippage_pct, side):
    if side == "buy":
        return float(price) * (1.0 + (float(slippage_pct) / 100.0))
    return float(price) * (1.0 - (float(slippage_pct) / 100.0))


def nearest_value(history, seconds_ago, field):
    if not history:
        return None
    target_ts = time.time() - seconds_ago
    candidate = None
    for item in history:
        if item["ts"] <= target_ts:
            candidate = item
        else:
            break
    if candidate is None:
        return None
    return candidate.get(field)


def min_mid_since(history, seconds_ago):
    if not history:
        return None
    cutoff = time.time() - seconds_ago
    mids = [item["mid"] for item in history if item["ts"] >= cutoff]
    if not mids:
        mids = [item["mid"] for item in history]
    return min(mids) if mids else None


def rolling_block_changes(history, total_window=BLOCK_WINDOW_SECONDS, step=BLOCK_STEP_SECONDS):
    if not history:
        return []
    changes = []
    for start_seconds_ago in range(total_window, 0, -step):
        older_mid = nearest_value(history, start_seconds_ago, "mid")
        newer_seconds_ago = max(start_seconds_ago - step, 0)
        newer_mid = nearest_value(history, newer_seconds_ago, "mid")
        pct = None if older_mid is None or newer_mid is None else pct_change(older_mid, newer_mid)
        changes.append({"label": f"-{start_seconds_ago}s", "pct": pct})
    latest_mid = nearest_value(history, 0, "mid")
    if latest_mid is None:
        return []
    changes.append({"label": "Latest", "pct": 0.0})
    return changes


def has_three_real_5s_blocks(history):
    checkpoints = (15, 10, 5, 0)
    return bool(history) and all(nearest_value(history, seconds_ago, "mid") is not None for seconds_ago in checkpoints)


def format_coin_label(coin):
    if not coin:
        return "-"
    if coin.startswith(f"{XYZ_PERP_DEX}:"):
        return f"{coin.split(':', 1)[1]}-USDC"
    return coin


FX_CODE_TO_NAME = {
    "AUD": "Australian dollar",
    "CAD": "Canadian dollar",
    "CHF": "Swiss franc",
    "EUR": "Euro",
    "GBP": "British pound",
    "JPY": "Japanese yen",
    "NZD": "New Zealand dollar",
    "USD": "US dollar",
}

KNOWN_PERP_METADATA = {
    "SPX": ("Equity Index", "S&P 500 index"),
    "NDX": ("Equity Index", "Nasdaq 100 index"),
    "DJI": ("Equity Index", "Dow Jones Industrial Average"),
    "VIX": ("Volatility Index", "Cboe Volatility Index"),
    "XAU": ("Commodity", "Gold"),
    "XAG": ("Commodity", "Silver"),
    "WTI": ("Commodity", "WTI crude oil"),
    "BRENT": ("Commodity", "Brent crude oil"),
    "NATGAS": ("Commodity", "Natural gas"),
    "COPPER": ("Commodity", "Copper"),
    "XYZ:GOLD": ("Commodity", "Gold"),
    "XYZ:SILVER": ("Commodity", "Silver"),
    "XYZ:AAPL": ("Equity", "Apple"),
    "XYZ:AMZN": ("Equity", "Amazon"),
    "XYZ:GOOGL": ("Equity", "Alphabet"),
    "XYZ:META": ("Equity", "Meta"),
    "XYZ:MSFT": ("Equity", "Microsoft"),
    "XYZ:NFLX": ("Equity", "Netflix"),
    "XYZ:NVDA": ("Equity", "NVIDIA"),
    "XYZ:TSLA": ("Equity", "Tesla"),
}


def infer_perp_metadata(coin, asset=None):
    asset = asset or {}
    raw_category = (
        asset.get("category")
        or asset.get("type")
        or asset.get("sector")
        or asset.get("group")
        or asset.get("tag")
        or ""
    )
    raw_description = (
        asset.get("description")
        or asset.get("displayName")
        or asset.get("fullName")
        or asset.get("longName")
        or ""
    )
    if raw_category or raw_description:
        return {
            "category": raw_category or "Perp",
            "description": raw_description or f"{coin} perp",
        }

    symbol = (coin or "").upper()
    if symbol in KNOWN_PERP_METADATA:
        category, description = KNOWN_PERP_METADATA[symbol]
        return {"category": category, "description": description}

    if re.fullmatch(r"[A-Z]{6}", symbol):
        base = symbol[:3]
        quote = symbol[3:]
        if base in FX_CODE_TO_NAME and quote in FX_CODE_TO_NAME:
            return {
                "category": "FX",
                "description": f"{FX_CODE_TO_NAME[base]} / {FX_CODE_TO_NAME[quote]}",
            }

    if symbol.startswith("K") and len(symbol) > 1:
        return {"category": "Crypto", "description": f"{symbol[1:]} scaled crypto perp"}

    return {"category": "Crypto", "description": f"{symbol} crypto perp"}


def compute_market_metrics(snapshot, history):
    mid = snapshot.get("mid", 0.0)
    anchor_5s = nearest_value(history, 5, "mid")
    anchor_15s = nearest_value(history, 15, "mid")
    anchor_30s = nearest_value(history, 30, "mid")
    anchor_1m = nearest_value(history, 60, "mid")
    anchor_2m = nearest_value(history, 120, "mid")
    anchor_15m = nearest_value(history, 900, "mid")
    anchor_60m = nearest_value(history, 3600, "mid")
    low_2m = min_mid_since(history, 120)
    imbalance_10s = nearest_value(history, 10, "book_imbalance")
    return_5s = pct_change(anchor_5s or mid, mid)
    return_15s = pct_change(anchor_15s or mid, mid)
    return_30s = pct_change(anchor_30s or mid, mid)
    return_1m = pct_change(anchor_1m or mid, mid)
    return_2m = pct_change(anchor_2m or mid, mid)
    prior_10s_return = pct_change(anchor_15s, anchor_5s) if anchor_15s is not None and anchor_5s is not None else 0.0
    prior_15s_return = pct_change(anchor_30s, anchor_15s) if anchor_30s is not None and anchor_15s is not None else 0.0
    prior_90s_return = pct_change(anchor_2m, anchor_30s) if anchor_2m is not None and anchor_30s is not None else 0.0
    acceleration_5s = return_5s - (prior_10s_return / 2.0) if anchor_15s is not None and anchor_5s is not None else 0.0
    acceleration_15s = return_15s - prior_15s_return if anchor_30s is not None and anchor_15s is not None else 0.0
    acceleration_30s = return_30s - (prior_90s_return / 3.0) if anchor_2m is not None and anchor_30s is not None else 0.0
    block_changes_5s = rolling_block_changes(history)
    real_block_changes_5s = [item for item in (block_changes_5s[:-1] if block_changes_5s else []) if item["pct"] is not None]
    latest_three_blocks = [item["pct"] for item in real_block_changes_5s[-3:]] if len(real_block_changes_5s) >= 3 else []
    latest_5s_block = float(real_block_changes_5s[-1]["pct"]) if real_block_changes_5s else 0.0
    latest_three_increasing = (
        len(latest_three_blocks) == 3
        and latest_three_blocks[0] > 0
        and latest_three_blocks[0] < latest_three_blocks[1] < latest_three_blocks[2]
    )
    return {
        "mid": mid,
        "best_bid": snapshot.get("best_bid", 0.0),
        "best_ask": snapshot.get("best_ask", 0.0),
        "spread_pct": snapshot.get("spread_pct", 0.0),
        "book_imbalance": snapshot.get("book_imbalance", 0.5),
        "book_imbalance_10s_ago": imbalance_10s if imbalance_10s is not None else snapshot.get("book_imbalance", 0.5),
        "has_return_5s": anchor_5s is not None,
        "has_return_15s": anchor_15s is not None,
        "has_return_30s": anchor_30s is not None,
        "has_return_1m": anchor_1m is not None,
        "has_return_2m": anchor_2m is not None,
        "has_return_15m": anchor_15m is not None,
        "has_return_60m": anchor_60m is not None,
        "return_5s": return_5s,
        "return_15s": return_15s,
        "return_30s": return_30s,
        "return_1m": return_1m,
        "return_2m": return_2m,
        "return_15m": pct_change(anchor_15m or mid, mid),
        "return_60m": pct_change(anchor_60m or mid, mid),
        "prior_10s_return": prior_10s_return,
        "prior_15s_return": prior_15s_return,
        "prior_90s_return": prior_90s_return,
        "acceleration_5s": acceleration_5s,
        "acceleration_15s": acceleration_15s,
        "acceleration_30s": acceleration_30s,
        "block_changes_5s": block_changes_5s,
        "real_block_changes_5s": real_block_changes_5s,
        "latest_three_blocks": latest_three_blocks,
        "latest_5s_block": latest_5s_block,
        "latest_three_increasing": latest_three_increasing,
        "bounce_from_2m_low": pct_change(low_2m or mid, mid),
        "market_data_age": max(0.0, time.time() - snapshot.get("ts", 0.0)),
    }


@dataclass
class BotConfig:
    trade_notional_usdc: float = 250.0
    max_notional_usdc: float = 1000.0
    max_open_positions: int = 10
    leverage: float = 1.0
    take_profit_pct: float = 0.25
    stop_loss_pct: float = 0.25
    time_stop_seconds: int = 60
    emergency_exit_drop_pct: float = 0.20
    emergency_window_seconds: int = 30
    return_2m_trend_threshold_pct: float = 0.20
    acceleration_min_delta_pct: float = 0.05
    early_entry_return_30s_pct: float = 0.08
    acceleration_15s_min_delta_pct: float = 0.03
    acceleration_5s_min_delta_pct: float = 0.015
    spread_pct_max: float = 0.025
    min_top5_depth_usdc: float = 2000.0
    starting_balance_usdc: float = 10000.0
    maker_fee_pct: float = 0.015
    taker_fee_pct: float = 0.045
    maker_entry_slippage_pct: float = 0.0
    maker_exit_slippage_pct: float = 0.0
    taker_exit_slippage_pct: float = 0.02
    min_price_usdc: float = 0.01


class MarketUniverse:
    def __init__(self, stop_event):
        if Info is None or hl_constants is None or websocket is None:
            raise RuntimeError(f"Hyperliquid dependencies unavailable: {IMPORT_ERROR}")
        self.stop_event = stop_event
        self.lock = Lock()
        self.history = {}
        self.current_mids = {}
        self.universe = []
        self.meta_by_coin = {}
        self.sz_decimals = {}
        self.first_message_at = 0.0
        self.last_message_at = 0.0
        self.last_meta_refresh_at = 0.0
        self.last_error = ""
        self.last_open_at = 0.0
        self.last_close_at = 0.0
        self.last_close_code = ""
        self.last_close_reason = ""
        self.ws_app = None
        self.ws_thread = None
        self.loop_thread = None
        self.rest_info = Info(hl_constants.MAINNET_API_URL, skip_ws=True)

    def _post_info(self, payload):
        timeout = BOOK_SNAPSHOT_TIMEOUT_SECONDS if payload.get("type") == "l2Book" else 10
        response = requests.post(HYPERLIQUID_INFO_URL, json=payload, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def refresh_meta(self, force=False):
        now = time.time()
        with self.lock:
            if not force and self.universe and (now - self.last_meta_refresh_at) < META_REFRESH_SECONDS:
                return
        universe = []
        meta_by_coin = {}
        sz_decimals = {}
        available_assets = {}
        for meta_payload in ({"type": "meta"}, {"type": "meta", "dex": XYZ_PERP_DEX}):
            meta = self._post_info(meta_payload)
            for asset in meta.get("universe") or []:
                coin = asset.get("name")
                if not coin:
                    continue
                if asset.get("isDelisted"):
                    continue
                available_assets[coin] = asset
        for coin in CURATED_PERP_SYMBOLS:
            asset = available_assets.get(coin)
            universe.append(coin)
            meta_by_coin[coin] = infer_perp_metadata(coin, asset)
            sz_decimals[coin] = int(asset.get("szDecimals", 0)) if asset else 0
        with self.lock:
            self.universe = universe
            self.meta_by_coin = meta_by_coin
            self.sz_decimals = sz_decimals
            self.last_meta_refresh_at = now

    def start(self):
        if self.loop_thread and self.loop_thread.is_alive():
            return
        self.loop_thread = Thread(target=self._loop, daemon=True, name="market-universe-loop")
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

    def _record_mid_update(self, mids):
        now = time.time()
        with self.lock:
            if not self.first_message_at:
                self.first_message_at = now
            allowed = set(self.universe)
            for coin, raw_mid in (mids or {}).items():
                if allowed and coin not in allowed:
                    continue
                try:
                    mid = float(raw_mid)
                except Exception:
                    continue
                self.current_mids[coin] = mid
                history = self.history.setdefault(coin, deque())
                history.append({"ts": now, "mid": mid})
                cutoff = now - MARKET_HISTORY_SECONDS
                while history and history[0]["ts"] < cutoff:
                    history.popleft()
            self.last_message_at = now
            self.last_error = ""

    def _on_ws_open(self, ws_app):
        self.refresh_meta(force=True)
        with self.lock:
            self.last_open_at = time.time()
            self.last_error = ""
        ws_app.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))
        ws_app.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids", "dex": XYZ_PERP_DEX}}))

    def _on_ws_message(self, _ws_app, raw_message):
        try:
            if raw_message == "Websocket connection established.":
                return
            msg = json.loads(raw_message)
        except Exception:
            return
        if msg.get("channel") in {"subscriptionResponse", None}:
            return
        if msg.get("channel") != "allMids":
            return
        data = msg.get("data") or {}
        mids = data.get("mids") if isinstance(data, dict) and "mids" in data else data
        if isinstance(mids, dict):
            self._record_mid_update(mids)

    def _on_ws_error(self, _ws_app, error):
        with self.lock:
            self.last_error = f"Websocket error: {error}"

    def _on_ws_close(self, _ws_app, status_code, close_msg):
        with self.lock:
            self.last_close_at = time.time()
            self.last_close_code = "" if status_code is None else str(status_code)
            self.last_close_reason = close_msg or ""
            if not self.last_error:
                self.last_error = f"Websocket closed ({status_code}): {close_msg or 'no message'}"

    def _connect_stream(self):
        self.stop()
        self.refresh_meta(force=True)
        self.ws_app = websocket.WebSocketApp(
            HYPERLIQUID_WS_URL,
            on_open=self._on_ws_open,
            on_message=self._on_ws_message,
            on_error=self._on_ws_error,
            on_close=self._on_ws_close,
        )
        self.ws_thread = Thread(
            target=lambda: self.ws_app.run_forever(ping_interval=20, ping_timeout=10),
            daemon=True,
            name="allmids-websocket",
        )
        self.ws_thread.start()

    def _loop(self):
        while not self.stop_event.is_set():
            try:
                self._connect_stream()
                while not self.stop_event.is_set():
                    time.sleep(1)
                    now = time.time()
                    with self.lock:
                        ws_thread = self.ws_thread
                        last_message_at = self.last_message_at
                        last_meta_refresh_at = self.last_meta_refresh_at
                    if ws_thread and not ws_thread.is_alive():
                        raise RuntimeError("Hyperliquid websocket thread exited unexpectedly.")
                    if last_meta_refresh_at and (now - last_meta_refresh_at) > META_REFRESH_SECONDS:
                        self.refresh_meta(force=True)
                    if last_message_at and (now - last_message_at) > MARKET_STALE_AFTER_SECONDS:
                        raise RuntimeError(f"Market data stale ({now - last_message_at:.1f}s).")
            except Exception as exc:
                with self.lock:
                    self.last_error = str(exc)
            finally:
                self.stop()
            time.sleep(2)

    def build_book_snapshot(self, book_data):
        levels = book_data.get("levels") or [[], []]
        bids = levels[0] if len(levels) > 0 else []
        asks = levels[1] if len(levels) > 1 else []
        if not bids or not asks:
            raise RuntimeError("Order book snapshot missing bids or asks.")
        best_bid = float(bids[0]["px"])
        best_ask = float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2.0
        spread_pct = ((best_ask - best_bid) / mid) * 100.0 if mid > 0 else 0.0
        bid_depth = sum(float(level["sz"]) for level in bids[:5])
        ask_depth = sum(float(level["sz"]) for level in asks[:5])
        total_depth = bid_depth + ask_depth
        book_imbalance = (bid_depth / total_depth) if total_depth > 0 else 0.5
        raw_time = book_data.get("time")
        event_ts = (float(raw_time) / 1000.0) if raw_time else time.time()
        return {
            "ts": event_ts,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid": mid,
            "spread_pct": spread_pct,
            "bid_depth_top5": bid_depth,
            "ask_depth_top5": ask_depth,
            "book_imbalance": book_imbalance,
        }

    def fetch_coin_book_snapshot(self, coin):
        book = self._post_info({"type": "l2Book", "coin": coin})
        snapshot = self.build_book_snapshot(book)
        snapshot["coin"] = coin
        return snapshot

    def get_sz_decimals(self, coin):
        with self.lock:
            return int(self.sz_decimals.get(coin, 0))

    def get_metrics_for_coin(self, coin):
        with self.lock:
            history = list(self.history.get(coin, []))
            latest_mid = float(self.current_mids.get(coin, 0.0))
            last_message_at = self.last_message_at
        if not history or latest_mid <= 0:
            return None
        return_5s = self._return_for(history, 5)
        return_15s = self._return_for(history, 15)
        return_30s = self._return_for(history, 30)
        return_1m = self._return_for(history, 60)
        return_2m = self._return_for(history, 120)
        return_15m = self._return_for(history, 900)
        return_60m = self._return_for(history, 3600)
        prior_10s_return = self._segment_return_for(history, 15, 5)
        prior_15s_return = self._segment_return_for(history, 30, 15)
        prior_90s_return = self._segment_return_for(history, 120, 30)
        acceleration_5s = (
            float(return_5s) - (float(prior_10s_return) / 2.0)
            if return_5s is not None and prior_10s_return is not None
            else 0.0
        )
        acceleration_15s = (
            float(return_15s) - float(prior_15s_return)
            if return_15s is not None and prior_15s_return is not None
            else 0.0
        )
        acceleration_30s = (
            float(return_30s) - (float(prior_90s_return) / 3.0)
            if return_30s is not None and prior_90s_return is not None
            else 0.0
        )
        now = time.time()
        return {
            "coin": coin,
            "mid": latest_mid,
            "has_return_5s": return_5s is not None,
            "has_return_15s": return_15s is not None,
            "has_return_30s": return_30s is not None,
            "has_return_1m": return_1m is not None,
            "has_return_2m": return_2m is not None,
            "has_return_15m": return_15m is not None,
            "has_return_60m": return_60m is not None,
            "return_5s": return_5s or 0.0,
            "return_15s": return_15s or 0.0,
            "return_30s": return_30s or 0.0,
            "return_1m": return_1m or 0.0,
            "return_2m": return_2m or 0.0,
            "return_15m": return_15m or 0.0,
            "return_60m": return_60m or 0.0,
            "prior_10s_return": prior_10s_return or 0.0,
            "prior_15s_return": prior_15s_return or 0.0,
            "prior_90s_return": prior_90s_return or 0.0,
            "acceleration_5s": acceleration_5s,
            "acceleration_15s": acceleration_15s,
            "acceleration_30s": acceleration_30s,
            "market_data_age": max(0.0, now - last_message_at) if last_message_at else 9999.0,
            "history": history,
        }

    def _return_for(self, history, seconds_ago):
        if not history:
            return None
        target_ts = time.time() - seconds_ago
        candidate = None
        for item in history:
            if item["ts"] <= target_ts:
                candidate = item
            else:
                break
        if candidate is None:
            return None
        latest_mid = history[-1]["mid"]
        return pct_change(candidate["mid"], latest_mid)

    def _segment_return_for(self, history, older_seconds_ago, newer_seconds_ago):
        if not history:
            return None
        older_mid = nearest_value(history, older_seconds_ago, "mid")
        newer_mid = nearest_value(history, newer_seconds_ago, "mid")
        if older_mid is None or newer_mid is None:
            return None
        return pct_change(older_mid, newer_mid)

    def build_coin_snapshot_and_metrics(self, coin):
        metrics = self.get_metrics_for_coin(coin)
        if not metrics:
            return None, None
        snapshot = self.fetch_coin_book_snapshot(coin)
        computed = compute_market_metrics(snapshot, metrics["history"])
        computed["market_data_age"] = metrics["market_data_age"]
        return snapshot, computed

    def get_hot_perps(self, limit=10, primary_basis="2m_accel", min_price=0.0, direction="up"):
        with self.lock:
            universe = list(self.universe)
            histories = {coin: list(items) for coin, items in self.history.items()}
            mids = dict(self.current_mids)
            meta_by_coin = dict(self.meta_by_coin)
            last_error = self.last_error
        leaders = []
        warmup_waiting = False
        for coin in universe:
            history = histories.get(coin, [])
            latest_mid = float(mids.get(coin, 0.0) or 0.0)
            metadata = meta_by_coin.get(coin) or infer_perp_metadata(coin)
            leader = {
                "coin": coin,
                "mid": trim_float(latest_mid, 6),
                "score_pct": 0.0,
                "latest_5s_block": 0.0,
                "previous_5s_block": 0.0,
                "score_basis": "5s jump",
                "score_basis_description": "fixed curated universe",
                "category": metadata.get("category", "Crypto"),
                "description": metadata.get("description", f"{coin} crypto perp"),
                "return_5s": 0.0,
                "return_15s": 0.0,
                "return_30s": 0.0,
                "return_1m": 0.0,
                "return_2m": 0.0,
                "acceleration_5s": 0.0,
                "acceleration_15s": 0.0,
                "acceleration_30s": 0.0,
                "block_changes_5s": [None] * len(BLOCK_COLUMN_LABELS),
                "latest_three_blocks": [],
                "latest_three_increasing": False,
                "has_live_data": bool(history and latest_mid > 0),
            }
            if not history or latest_mid < min_price:
                leaders.append(leader)
                if not history:
                    warmup_waiting = True
                continue
            return_5s = self._return_for(history, 5)
            return_15s = self._return_for(history, 15)
            return_30s = self._return_for(history, 30)
            return_1m = self._return_for(history, 60)
            return_2m = self._return_for(history, 120)
            prior_10s_return = self._segment_return_for(history, 15, 5)
            prior_15s_return = self._segment_return_for(history, 30, 15)
            prior_90s_return = self._segment_return_for(history, 120, 30)
            acceleration_5s = (
                float(return_5s) - (float(prior_10s_return) / 2.0)
                if return_5s is not None and prior_10s_return is not None
                else None
            )
            acceleration_15s = (
                float(return_15s) - float(prior_15s_return)
                if return_15s is not None and prior_15s_return is not None
                else None
            )
            acceleration_30s = (
                float(return_30s) - (float(prior_90s_return) / 3.0)
                if return_30s is not None and prior_90s_return is not None
                else None
            )
            block_changes = rolling_block_changes(history)
            if primary_basis == "2m_accel" and (
                return_2m is None
                or acceleration_30s is None
                or acceleration_15s is None
                or acceleration_5s is None
            ):
                warmup_waiting = True
            real_block_changes = [item for item in block_changes[:-1] if item["pct"] is not None]
            if not real_block_changes:
                warmup_waiting = True
                leaders.append(leader)
                continue
            latest_three_blocks = [item["pct"] for item in real_block_changes[-3:]] if len(real_block_changes) >= 3 else []
            latest_three_increasing = (
                len(latest_three_blocks) == 3
                and latest_three_blocks[0] > 0
                and latest_three_blocks[0] < latest_three_blocks[1] < latest_three_blocks[2]
            )
            latest_5s_block = float(real_block_changes[-1]["pct"]) if real_block_changes else 0.0
            previous_5s_block = float(real_block_changes[-2]["pct"]) if len(real_block_changes) >= 2 else 0.0
            leader.update(
                {
                    "mid": trim_float(latest_mid, 6),
                    "score_pct": trim_float(latest_5s_block, 4),
                    "latest_5s_block": trim_float(latest_5s_block, 4),
                    "previous_5s_block": trim_float(previous_5s_block, 4),
                    "score_basis": "5s jump",
                    "score_basis_description": "fixed curated universe",
                    "return_5s": trim_float(return_5s or 0.0, 4),
                    "return_15s": trim_float(return_15s or 0.0, 4),
                    "return_30s": trim_float(return_30s or 0.0, 4),
                    "return_1m": trim_float(return_1m or 0.0, 4),
                    "return_2m": trim_float(return_2m or 0.0, 4),
                    "acceleration_5s": trim_float(acceleration_5s or 0.0, 4),
                    "acceleration_15s": trim_float(acceleration_15s or 0.0, 4),
                    "acceleration_30s": trim_float(acceleration_30s or 0.0, 4),
                    "block_changes_5s": [None if item["pct"] is None else trim_float(item["pct"], 4) for item in block_changes],
                    "latest_three_blocks": [trim_float(value, 4) for value in latest_three_blocks],
                    "latest_three_increasing": latest_three_increasing,
                    "has_live_data": True,
                }
            )
            leaders.append(leader)
        if (
            not any(any(value is not None for value in (item.get("block_changes_5s") or [])) for item in leaders)
            and primary_basis == "2m_accel"
            and warmup_waiting
            and not last_error
        ):
            last_error = "Waiting for three real 5s blocks."
        return {"leaders": leaders[:limit], "last_error": last_error}

    def diagnostics(self):
        with self.lock:
            universe = list(self.universe)
            histories = {coin: list(history) for coin, history in self.history.items()}
            mids = dict(self.current_mids)
            age = max(0.0, time.time() - self.last_message_at) if self.last_message_at else 9999.0
            warmup_elapsed = max(0.0, time.time() - self.first_message_at) if self.first_message_at else 0.0
            ready_2m = sum(1 for coin in universe if self._return_for(histories.get(coin, []), 120) is not None)
            ready_entry = sum(1 for coin in universe if has_three_real_5s_blocks(histories.get(coin, [])))
            tracked = sum(1 for coin in universe if float(mids.get(coin, 0.0) or 0.0) > 0.0)
            return {
                "configured": len(universe),
                "tracked": tracked,
                "ready_entry": ready_entry,
                "ready_2m": ready_2m,
                "missing_feed": max(0, len(universe) - tracked),
                "market_age": age,
                "first_message_at": self.first_message_at,
                "warmup_elapsed": warmup_elapsed,
                "last_error": self.last_error,
                "last_open_at": self.last_open_at,
                "last_close_at": self.last_close_at,
            }


class LocalSandboxBot:
    def __init__(self, market, config):
        self.market = market
        self.config = config
        self.start_time = time.time()
        self.available = float(config.starting_balance_usdc)
        self.positions = {}
        self.trades = deque(maxlen=30)
        self.logs = deque(maxlen=12)
        self.last_signal_reason = "Waiting for three real 5s blocks."
        self.last_scan_error = ""
        self.last_scan_at = 0.0
        self.hot_perps = []
        self.stats = {
            "total_pnl": 0.0,
            "daily_pnl": 0.0,
            "consecutive_losses": 0,
            "trades_today": 0,
            "time_stops": 0,
            "emergency_exits": 0,
            "last_entry_fill_at": 0.0,
        }
        self.lock = Lock()
        self.log("Local sandbox runner started.")

    def log(self, message):
        self.logs.appendleft(f"{format_timestamp(time.time())} {message}")

    def live_pnl(self):
        total = 0.0
        for position in self.positions.values():
            metrics = self.market.get_metrics_for_coin(position["coin"])
            if not metrics:
                continue
            direction = 1.0 if position["side"] == "LONG" else -1.0
            total += ((float(metrics["mid"]) - float(position["filled_price"])) * float(position["size"])) * direction
        return total

    def reserved_margin(self):
        total = 0.0
        for position in self.positions.values():
            total += float(position.get("initial_margin", 0.0))
        return total

    def equity(self):
        return self.available + self.reserved_margin() + self.live_pnl()

    def update_position_extremes(self, position, current_mid):
        position["highest_mid"] = max(position.get("highest_mid", current_mid), current_mid)
        position["lowest_mid"] = min(position.get("lowest_mid", current_mid), current_mid)
        entry = float(position["filled_price"])
        side = position.get("side", "LONG")
        direction = 1.0 if side == "LONG" else -1.0
        favourable = pct_change(entry, position["highest_mid"]) * direction
        adverse = pct_change(entry, position["lowest_mid"]) * direction
        position["max_favourable_excursion"] = max(float(position.get("max_favourable_excursion", 0.0)), favourable)
        position["max_adverse_excursion"] = min(float(position.get("max_adverse_excursion", 0.0)), adverse)

    def evaluate_entry_candidate(self, coin, snapshot, metrics):
        reasons = []
        intended_side = "LONG"
        if coin in self.positions:
            reasons.append("position already active")
        if len(self.positions) >= int(self.config.max_open_positions):
            reasons.append("max open positions reached")
        if metrics["market_data_age"] > MARKET_STALE_AFTER_SECONDS:
            reasons.append("market data stale")
        if float(snapshot.get("mid", 0.0)) < float(self.config.min_price_usdc):
            reasons.append("price below minimum")
        real_block_changes = metrics.get("real_block_changes_5s") or []
        if len(real_block_changes) < 3:
            reasons.append("waiting for three real 5s blocks")
        latest_three_blocks = metrics.get("latest_three_blocks") or []
        if len(latest_three_blocks) < 3:
            reasons.append("latest 5s blocks unavailable")
        else:
            if latest_three_blocks[0] <= 0:
                reasons.append("oldest of latest three 5s blocks not positive")
            if not (latest_three_blocks[0] < latest_three_blocks[1] < latest_three_blocks[2]):
                reasons.append("latest three 5s blocks not sequentially increasing")
        if float(metrics.get("spread_pct", 0.0)) > float(self.config.spread_pct_max):
            reasons.append(f"spread {metrics['spread_pct']:.4f}% above max")
        bid_depth = float(snapshot.get("bid_depth_top5", 0.0) or 0.0)
        ask_depth = float(snapshot.get("ask_depth_top5", 0.0) or 0.0)
        depth_usdc = (bid_depth + ask_depth) * float(snapshot.get("mid", 0.0) or 0.0)
        if self.config.min_top5_depth_usdc > 0 and depth_usdc < self.config.min_top5_depth_usdc:
            reasons.append(f"top5 depth {depth_usdc:.0f} below minimum")
        if reasons:
            return False, " | ".join(reasons), intended_side, ""
        entry_context = (
            f"Three rising 5s blocks: "
            f"{latest_three_blocks[0]:.2f}% -> {latest_three_blocks[1]:.2f}% -> {latest_three_blocks[2]:.2f}% | "
            f"spread {float(metrics.get('spread_pct', 0.0)):.4f}% | "
            f"depth {depth_usdc:,.0f} USDC"
        )
        return True, "Entry conditions passed (three rising 5s blocks).", intended_side, entry_context

    def enter_position(self, coin, snapshot, intended_side, entry_reason=""):
        leverage = max(1.0, float(self.config.leverage))
        fee_rate = float(self.config.taker_fee_pct) / 100.0
        max_affordable_notional = self.available / ((1.0 / leverage) + fee_rate) if leverage > 0 else self.available
        requested_notional = float(self.config.trade_notional_usdc) * leverage
        target_notional = min(requested_notional, float(self.config.max_notional_usdc), max_affordable_notional)
        initial_margin = target_notional / leverage if leverage > 0 else target_notional
        if initial_margin < 10.0:
            self.log(f"Entry blocked for {coin}: available balance below 10 USDC minimum.")
            return False
        order_side = "buy" if intended_side == "LONG" else "sell"
        submitted_price = float(snapshot["best_ask"] if order_side == "buy" else snapshot["best_bid"])
        filled_price = apply_slippage(submitted_price, self.config.taker_exit_slippage_pct, order_side)
        sz_decimals = self.market.get_sz_decimals(coin)
        raw_size = target_notional / filled_price if filled_price > 0 else 0.0
        size = floor_to_decimals(raw_size, sz_decimals)
        if size <= 0:
            self.log(f"Entry blocked for {coin}: rounded size is zero at current price.")
            return False
        notional = size * filled_price
        initial_margin = notional / leverage if leverage > 0 else notional
        fee = apply_fee(notional, self.config.taker_fee_pct)
        self.available -= initial_margin + fee
        self.positions[coin] = {
            "coin": coin,
            "side": intended_side,
            "size": size,
            "submitted_price": submitted_price,
            "filled_price": filled_price,
            "notional": notional,
            "sz_decimals": sz_decimals,
            "initial_margin": initial_margin,
            "leverage": leverage,
            "entry_time": time.time(),
            "entry_fees_paid": fee,
            "highest_mid": snapshot["mid"],
            "lowest_mid": snapshot["mid"],
            "max_favourable_excursion": 0.0,
            "max_adverse_excursion": 0.0,
            "last_hold_check_at": 0.0,
            "extension_checks_completed": 0,
            "entry_reason": entry_reason,
        }
        self.stats["last_entry_fill_at"] = time.time()
        self.log(f"{intended_side} entry filled for {coin} at {filled_price:.5f}. Fee {fee:.4f} USDC. {entry_reason}")
        return True

    def exit_position(self, position, exit_reason):
        coin = position["coin"]
        snapshot = self.market.fetch_coin_book_snapshot(coin)
        side = position.get("side", "LONG")
        close_side = "buy" if side == "SHORT" else "sell"
        submitted_price = float(snapshot["best_ask"] if close_side == "buy" else snapshot["best_bid"])
        fill_price = apply_slippage(submitted_price, self.config.taker_exit_slippage_pct, close_side)
        size = float(position["size"])
        direction = 1.0 if side == "LONG" else -1.0
        final_change_pct = pct_change(float(position["filled_price"]), fill_price) * direction
        gross_pnl = ((fill_price - float(position["filled_price"])) * size) * direction
        exit_notional = size * fill_price
        exit_fee = apply_fee(exit_notional, self.config.taker_fee_pct)
        entry_fee = float(position.get("entry_fees_paid", 0.0))
        net_pnl = gross_pnl - entry_fee - exit_fee
        initial_margin = float(position.get("initial_margin", 0.0))
        self.available += initial_margin + gross_pnl - exit_fee
        trade = {
            "timestamp": time.time(),
            "coin": coin,
            "side": side,
            "entry_price": float(position["filled_price"]),
            "exit_price": fill_price,
            "notional": float(position["notional"]),
            "entry_usdc": initial_margin,
            "exit_usdc": initial_margin + net_pnl,
            "entry_reason": position.get("entry_reason", ""),
            "final_change_pct": final_change_pct,
            "gross_pnl": gross_pnl,
            "net_pnl": net_pnl,
            "fees_paid": entry_fee + exit_fee,
            "exit_reason": exit_reason,
            "seconds_open": time.time() - float(position["entry_time"]),
            "equity_after_trade": self.equity(),
        }
        self.trades.appendleft(trade)
        del self.positions[coin]
        self.stats["total_pnl"] += net_pnl
        self.stats["daily_pnl"] += net_pnl
        self.stats["trades_today"] += 1
        if net_pnl < 0:
            self.stats["consecutive_losses"] += 1
        else:
            self.stats["consecutive_losses"] = 0
        if exit_reason == "TIME_STOP":
            self.stats["time_stops"] += 1
        if exit_reason == "EMERGENCY_EXIT":
            self.stats["emergency_exits"] += 1
        self.log(
            f"{side} exit {exit_reason} for {coin} at {fill_price:.5f}. "
            f"Final {final_change_pct:.4f}%. Net PnL {net_pnl:.4f} USDC. "
            f"{position.get('entry_reason', '')}"
        )

    def manage_positions(self):
        for coin, position in list(self.positions.items()):
            market_metrics = self.market.get_metrics_for_coin(coin)
            if not market_metrics:
                continue
            current_mid = float(market_metrics["mid"])
            self.update_position_extremes(position, current_mid)
            side = position.get("side", "LONG")
            direction = 1.0 if side == "LONG" else -1.0
            change_pct = pct_change(float(position["filled_price"]), current_mid) * direction
            seconds_open = time.time() - float(position["entry_time"])
            if change_pct <= -abs(float(self.config.emergency_exit_drop_pct)) and seconds_open <= int(self.config.emergency_window_seconds):
                self.exit_position(position, "EMERGENCY_EXIT")
                continue
            if change_pct <= -abs(float(self.config.stop_loss_pct)):
                self.exit_position(position, "STOP_LOSS")
                continue
            if seconds_open < 60:
                continue
            if (time.time() - float(position.get("last_hold_check_at", 0.0) or 0.0)) < 10:
                continue
            if change_pct >= float(self.config.take_profit_pct):
                self.exit_position(position, "TAKE_PROFIT")
                continue
            if change_pct <= 0 and (float(market_metrics.get("return_1m", 0.0)) * direction) < 0:
                self.exit_position(position, "TIME_STOP")
                continue
            position["last_hold_check_at"] = time.time()
            position["extension_checks_completed"] = int(position.get("extension_checks_completed", 0)) + 1

    def attempt_entries(self):
        if (time.time() - self.last_scan_at) < SCAN_INTERVAL_SECONDS:
            return
        self.last_scan_at = time.time()
        hot = self.market.get_hot_perps(
            limit=len(CURATED_PERP_SYMBOLS),
            primary_basis="2m_accel",
            min_price=float(self.config.min_price_usdc),
            direction="up",
        )
        self.hot_perps = hot["leaders"]
        self.last_scan_error = hot["last_error"] or ""
        if not self.hot_perps:
            self.last_signal_reason = self.last_scan_error or "No candidates."
            return
        candidates_for_entry = sorted(
            [item for item in self.hot_perps if item.get("has_live_data")],
            key=lambda item: (
                float(item.get("latest_5s_block", 0.0)),
                float(item.get("previous_5s_block", 0.0)),
            ),
            reverse=True,
        )
        checks_completed = 0
        for candidate in candidates_for_entry:
            if len(self.positions) >= int(self.config.max_open_positions):
                self.last_signal_reason = "Max open positions reached."
                return
            coin = candidate["coin"]
            if coin in self.positions:
                continue
            latest_three_blocks = candidate.get("latest_three_blocks") or []
            if len(latest_three_blocks) < 3:
                self.last_signal_reason = f"{format_coin_label(coin)}: waiting for three real 5s blocks."
                continue
            if latest_three_blocks[0] <= 0 or not (
                latest_three_blocks[0] < latest_three_blocks[1] < latest_three_blocks[2]
            ):
                self.last_signal_reason = f"{format_coin_label(coin)}: latest three 5s blocks not sequentially increasing."
                continue
            if checks_completed >= MAX_BOOK_CHECKS_PER_SCAN:
                self.last_signal_reason = f"Checked top {MAX_BOOK_CHECKS_PER_SCAN} symbols; waiting for next scan."
                return
            try:
                snapshot, metrics = self.market.build_coin_snapshot_and_metrics(coin)
                checks_completed += 1
            except Exception as exc:
                checks_completed += 1
                self.last_signal_reason = f"{format_coin_label(coin)}: snapshot error: {exc}"
                continue
            if not snapshot or not metrics:
                continue
            passed, reason, intended_side, entry_reason = self.evaluate_entry_candidate(coin, snapshot, metrics)
            self.last_signal_reason = f"{format_coin_label(coin)}: {reason}"
            if not passed:
                continue
            if self.enter_position(coin, snapshot, intended_side, entry_reason):
                return

    def step(self):
        with self.lock:
            self.manage_positions()
            self.attempt_entries()

    def status_text(self):
        diag = self.market.diagnostics()
        if diag["last_error"]:
            return f"WS issue: {diag['last_error']}"
        if diag["tracked"] <= 0:
            return "Waiting for live market data."
        if diag["ready_entry"] <= 0:
            return "Building first three 5s blocks."
        if self.positions:
            return f"Managing {len(self.positions)} position(s)."
        if diag["missing_feed"] > 0:
            return f"{diag['missing_feed']} symbols have no public feed in this runner."
        return self.last_signal_reason


def style_pct(value):
    if value > 0:
        return "[green]"
    if value < 0:
        return "[red]"
    return "[dim]"


def render_block_pct(value):
    if value is None:
        return Text("-", style="dim")
    if value > 0:
        style = "green"
    elif value < 0:
        style = "red"
    else:
        style = "white"
    return Text(f"{value:.3f}%", style=style)


def build_summary_table(bot, market):
    diag = market.diagnostics()
    table = Table.grid(expand=True)
    table.add_column(justify="left")
    table.add_column(justify="left")
    table.add_column(justify="left")
    table.add_column(justify="left")
    table.add_column(justify="left")
    table.add_column(justify="left")
    table.add_column(justify="left")
    table.add_column(justify="left")
    table.add_row(
        f"[bold]Available[/bold]\n{bot.available:,.2f} USDC",
        f"[bold]Equity[/bold]\n{bot.equity():,.2f} USDC",
        f"[bold]Live PnL[/bold]\n{bot.live_pnl():,.2f} USDC",
        f"[bold]Entry Ready[/bold]\n{diag['ready_entry']} / {diag['tracked']}",
        f"[bold]Open Positions[/bold]\n{len(bot.positions)}",
        f"[bold]Total PnL[/bold]\n{bot.stats['total_pnl']:,.2f} USDC",
        f"[bold]Trades[/bold]\n{bot.stats['trades_today']}",
        f"[bold]Feed Age[/bold]\n{diag['market_age']:.1f}s",
    )
    return table


def build_hot_table(bot):
    table = Table(
        expand=True,
        padding=(0, 0),
        pad_edge=False,
        collapse_padding=True,
        box=box.SIMPLE_HEAD,
    )
    table.add_column("Perp", style="bold", no_wrap=True)
    for label in BLOCK_COLUMN_LABELS:
        table.add_column(label, justify="right", no_wrap=True)
    leaders = bot.hot_perps
    if not leaders:
        table.add_row(*(["-"] + [bot.last_scan_error or "Waiting for three real 5s blocks."] + (["-"] * (len(BLOCK_COLUMN_LABELS) - 1))))
        return table
    for item in leaders:
        block_values = item.get("block_changes_5s") or []
        block_cells = [render_block_pct(value) for value in block_values]
        if len(block_cells) < len(BLOCK_COLUMN_LABELS):
            block_cells.extend(["-"] * (len(BLOCK_COLUMN_LABELS) - len(block_cells)))
        table.add_row(
            Text(format_coin_label(item["coin"]), style="bold white" if item.get("has_live_data") else "dim"),
            *block_cells,
        )
    return table


def build_positions_table(bot, market):
    table = Table(
        expand=True,
        padding=(0, 0),
        pad_edge=False,
        collapse_padding=True,
        box=box.SIMPLE_HEAD,
    )
    table.add_column("Perp", style="bold", no_wrap=True)
    table.add_column("Side", no_wrap=True)
    table.add_column("Entry", justify="right", no_wrap=True)
    table.add_column("Current", justify="right", no_wrap=True)
    table.add_column("Gain %", justify="right", no_wrap=True)
    table.add_column("Status", no_wrap=True, overflow="ellipsis", max_width=18)
    table.add_column("Opened Why", overflow="fold")
    if not bot.positions:
        table.add_row("-", "No active positions", "-", "-", "-", "-", "-")
        return table
    for coin, position in sorted(bot.positions.items()):
        metrics = market.get_metrics_for_coin(coin)
        current_mid = float(metrics["mid"]) if metrics else float(position["filled_price"])
        direction = 1.0 if position["side"] == "LONG" else -1.0
        pnl_pct = pct_change(float(position["filled_price"]), current_mid) * direction
        pnl_style = style_pct(pnl_pct)
        seconds_open = int(time.time() - float(position["entry_time"]))
        if seconds_open < 60:
            status = "First 60s"
        else:
            extension_checks = int(position.get("extension_checks_completed", 0))
            status = "10s review due" if extension_checks <= 0 else f"10s extension - {extension_checks}"
        table.add_row(
            format_coin_label(coin),
            position["side"],
            f"{position['filled_price']:.5f}",
            f"{current_mid:.5f}",
            f"{pnl_style}{pnl_pct:,.4f}%[/]",
            status,
            position.get("entry_reason", ""),
        )
    return table


def build_trades_table(bot):
    table = Table(
        expand=True,
        padding=(0, 0),
        pad_edge=False,
        collapse_padding=True,
        box=box.SIMPLE_HEAD,
    )
    table.add_column("Time", no_wrap=True)
    table.add_column("Perp", no_wrap=True)
    table.add_column("Side", no_wrap=True)
    table.add_column("Exit", no_wrap=True, overflow="ellipsis", max_width=14)
    table.add_column("Entry USDC", justify="right", no_wrap=True)
    table.add_column("Exit USDC", justify="right", no_wrap=True)
    table.add_column("Final %", justify="right", no_wrap=True)
    table.add_column("PnL", justify="right", no_wrap=True)
    table.add_column("Net PnL", justify="right", no_wrap=True)
    table.add_column("Opened Why", overflow="fold")
    if not bot.trades:
        table.add_row("-", "No trades yet", "-", "-", "-", "-", "-", "-", "-", "-")
        return table
    for trade in list(bot.trades)[:8]:
        final_style = style_pct(float(trade.get("final_change_pct", 0.0)))
        gross_style = style_pct(trade["gross_pnl"])
        pnl_style = style_pct(trade["net_pnl"])
        table.add_row(
            format_timestamp(trade["timestamp"]),
            format_coin_label(trade["coin"]),
            trade["side"],
            trade["exit_reason"],
            f"{trade.get('entry_usdc', trade['notional']):,.2f}",
            f"{trade.get('exit_usdc', trade.get('equity_after_trade', 0.0)):,.2f}",
            f"{final_style}{trade.get('final_change_pct', 0.0):,.4f}%[/]",
            f"{gross_style}{trade['gross_pnl']:,.4f}[/]",
            f"{pnl_style}{trade['net_pnl']:,.4f}[/]",
            trade.get("entry_reason", ""),
        )
    return table


def build_logs_panel(bot):
    lines = list(bot.logs)[:10]
    if not lines:
        lines = ["No logs yet."]
    return "\n".join(lines)


def build_warmup_bar(diag):
    if diag["ready_2m"] > 0:
        return None
    if not diag.get("first_message_at"):
        return Text("Warm-up: waiting for first market data...", style="bold yellow")
    total_seconds = 120.0
    elapsed = min(total_seconds, float(diag.get("warmup_elapsed", 0.0)))
    pct = elapsed / total_seconds if total_seconds > 0 else 1.0
    width = 36
    filled = min(width, max(0, int(round(width * pct))))
    bar = ("█" * filled) + ("░" * (width - filled))
    style = "green" if pct >= 1.0 else "bold yellow"
    return Text(f"2m tape warm-up [{bar}] {int(elapsed)}/{int(total_seconds)}s ({pct * 100:0.0f}%)", style=style)


def build_dashboard(bot, market):
    diag = market.diagnostics()
    title = Text("muzz.world", style="bold white")
    subtitle = Text(
        f"2m acceleration scan | Runtime {int(time.time() - bot.start_time)}s | "
        f"WS open {format_timestamp(diag['last_open_at']) or 'n/a'}",
        style="cyan",
    )
    status_value = bot.status_text()
    status_style = "bold yellow" if any(word in status_value for word in ("Waiting", "Building")) else "bold green"
    status = Text(status_value, style=status_style)
    warmup_bar = build_warmup_bar(diag)
    header_items = [title, subtitle, status]
    if warmup_bar is not None:
        header_items.append(warmup_bar)
    return Group(
        *header_items,
        Rule("Account", style="white"),
        build_summary_table(bot, market),
        Rule("Tracked Symbols", style="white"),
        build_hot_table(bot),
        Rule("Open Positions", style="white"),
        build_positions_table(bot, market),
        Rule("Recent Trades", style="white"),
        build_trades_table(bot),
        Rule("Action Log", style="white"),
        build_logs_panel(bot),
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Local MuzzWorld terminal runner")
    parser.add_argument("--trade-notional", type=float, default=250.0, help="Base trade notional in USDC")
    parser.add_argument("--max-notional", type=float, default=1000.0, help="Maximum notional per trade")
    parser.add_argument("--max-open-positions", type=int, default=10, help="Maximum simultaneous sandbox positions")
    parser.add_argument("--leverage", type=float, default=1.0, help="Sandbox leverage")
    parser.add_argument("--starting-balance", type=float, default=10000.0, help="Starting sandbox balance in USDC")
    parser.add_argument("--trend-trigger", type=float, default=0.20, help="Minimum 2m move required to consider entry")
    parser.add_argument("--accel-trigger", type=float, default=0.05, help="Minimum 30s acceleration edge versus prior 90s")
    parser.add_argument("--accel15-trigger", type=float, default=0.03, help="Minimum 15s acceleration edge versus prior 15s")
    parser.add_argument("--accel5-trigger", type=float, default=0.015, help="Minimum 5s acceleration edge versus prior 10s")
    parser.add_argument("--early-30s-trigger", type=float, default=0.08, help="Minimum 30s move to allow early entry before full 2m trigger")
    parser.add_argument("--spread-max", type=float, default=0.025, help="Maximum spread percent allowed")
    parser.add_argument("--min-top5-depth", type=float, default=2000.0, help="Minimum combined top5 book depth in USDC")
    return parser.parse_args()


def prompt_float_value(label, default):
    while True:
        try:
            raw = input(f"{label} [{default}]: ").strip()
        except EOFError:
            return default
        if not raw:
            return default
        try:
            return float(raw)
        except ValueError:
            console.print(f"Invalid number: {raw}", style="red")


def main():
    if IMPORT_ERROR:
        raise RuntimeError(f"Cannot start local runner: {IMPORT_ERROR}")
    args = parse_args()
    args.leverage = prompt_float_value("Leverage", args.leverage)
    config = BotConfig(
        trade_notional_usdc=args.trade_notional,
        max_notional_usdc=args.max_notional,
        max_open_positions=args.max_open_positions,
        leverage=args.leverage,
        starting_balance_usdc=args.starting_balance,
        return_2m_trend_threshold_pct=args.trend_trigger,
        acceleration_min_delta_pct=args.accel_trigger,
        acceleration_15s_min_delta_pct=args.accel15_trigger,
        acceleration_5s_min_delta_pct=args.accel5_trigger,
        early_entry_return_30s_pct=args.early_30s_trigger,
        spread_pct_max=args.spread_max,
        min_top5_depth_usdc=args.min_top5_depth,
    )
    stop_event = Event()
    market = MarketUniverse(stop_event)
    bot = LocalSandboxBot(market, config)
    market.start()

    def handle_stop(_signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    refresh_hz = 4
    step_interval_seconds = 1.0 / refresh_hz

    def bot_loop():
        while not stop_event.is_set():
            try:
                bot.step()
            except Exception as exc:
                bot.log(f"Loop error: {exc}")
            time.sleep(step_interval_seconds)

    bot_thread = Thread(target=bot_loop, daemon=True, name="bot-step-loop")
    bot_thread.start()
    try:
        with Live(build_dashboard(bot, market), console=console, refresh_per_second=refresh_hz, screen=True) as live:
            while not stop_event.is_set():
                live.update(build_dashboard(bot, market))
                time.sleep(step_interval_seconds)
    finally:
        stop_event.set()
        market.stop()
        console.print("\nStopped local sandbox runner.")


if __name__ == "__main__":
    main()
