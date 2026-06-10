import json
import math
import os
import re
import time
import uuid
from collections import deque
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock, Thread
from urllib import error as urllib_error
from urllib import request as urllib_request

from flask import Flask, jsonify, redirect, render_template, request, session, url_for
from psycopg2.extras import Json
from psycopg2.pool import ThreadedConnectionPool

try:
    from eth_account import Account
    from hyperliquid.exchange import Exchange
    from hyperliquid.info import Info
    from hyperliquid.utils import constants as hl_constants
    import websocket

    HYPERLIQUID_IMPORT_ERROR = ''
except Exception as exc:  # pragma: no cover - import availability depends on runtime image
    Account = None
    Exchange = None
    Info = None
    hl_constants = None
    websocket = None
    HYPERLIQUID_IMPORT_ERROR = str(exc)

app = Flask(__name__)


@app.after_request
def disable_api_caching(response):
    if request.path.startswith('/api/') or request.path.startswith('/sandbox/api/'):
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response

app.secret_key = os.environ.get('FLASK_SECRET_KEY', str(uuid.uuid4()))
ADMIN_PASSWORD = (os.environ.get('ADMIN_PASSWORD') or '').strip()
if not ADMIN_PASSWORD:
    raise RuntimeError('ADMIN_PASSWORD is required.')
RENDER_INTERNAL_DATABASE = os.environ.get('RENDER_INTERNAL_DATABASE') or os.environ.get('DATABASE_URL')
HYPERLIQUID_API_KEY = os.environ.get('HYPERLIQUID_API_KEY')
HYPERLIQUID_API_SECRET = os.environ.get('HYPERLIQUID_API_SECRET')

GLOBAL_NS = 'global'
REAL_NS = 'real'
SANDBOX_NS = 'sandbox'
ENVIRONMENTS = (REAL_NS, SANDBOX_NS)
INSTANCE_ID = str(uuid.uuid4())[:8]

MARKET_COIN = 'HYPE'
MARKET_POLL_INTERVAL = 1.0
TRADING_LOOP_INTERVAL = 1.0
MARKET_HISTORY_SECONDS = 3900
MARKET_STALE_AFTER_SECONDS = 20.0
HYPERLIQUID_WS_URL = 'wss://api.hyperliquid.xyz/ws'
HYPERLIQUID_INFO_URL = 'https://api.hyperliquid.xyz/info'
UNIVERSE_REFRESH_INTERVAL_SECONDS = 2.0
UNIVERSE_META_REFRESH_INTERVAL_SECONDS = 900.0
RESULTS_TRADE_LIMIT = 500
TRADE_LOG_LIMIT = 500
SIGNAL_LOG_LIMIT = 5000
ACTION_LOG_LIMIT = 400
REAL_START_CONFIRM_TEXT = 'START REAL'
REAL_MARKET_ORDER_TOLERANCE = 0.003


class PostgresStateStore:
    def __init__(self, dsn, minconn=1, maxconn=8):
        if not dsn:
            raise RuntimeError('RENDER_INTERNAL_DATABASE is required.')
        self.pool = ThreadedConnectionPool(minconn, maxconn, dsn=dsn)
        self._bootstrap()

    @contextmanager
    def connection(self):
        conn = self.pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

    def _bootstrap(self):
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    '''
                    CREATE TABLE IF NOT EXISTS state_kv (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        expires_at TIMESTAMPTZ NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    '''
                )
                cur.execute(
                    '''
                    CREATE TABLE IF NOT EXISTS state_lists (
                        key TEXT PRIMARY KEY,
                        items JSONB NOT NULL DEFAULT '[]'::jsonb,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    '''
                )

    def ping(self):
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT 1')
                return cur.fetchone()[0] == 1

    def _purge_expired_key(self, cur, key):
        cur.execute(
            'DELETE FROM state_kv WHERE key = %s AND expires_at IS NOT NULL AND expires_at <= NOW()',
            (key,),
        )

    @staticmethod
    def _expires_at(ex_seconds):
        if ex_seconds is None:
            return None
        return datetime.now(timezone.utc) + timezone.utc.utcoffset(datetime.now(timezone.utc)) + (
            datetime.now(timezone.utc) - datetime.now(timezone.utc)
        )

    def get(self, key):
        with self.connection() as conn:
            with conn.cursor() as cur:
                self._purge_expired_key(cur, key)
                cur.execute('SELECT value FROM state_kv WHERE key = %s', (key,))
                row = cur.fetchone()
                return None if row is None else row[0]

    def set(self, key, value, nx=False, ex=None):
        expires_at = None
        if ex is not None:
            expires_at = datetime.now(timezone.utc).timestamp() + int(ex)
            expires_at = datetime.fromtimestamp(expires_at, tz=timezone.utc)
        with self.connection() as conn:
            with conn.cursor() as cur:
                self._purge_expired_key(cur, key)
                if nx:
                    cur.execute(
                        '''
                        INSERT INTO state_kv (key, value, expires_at, updated_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (key) DO NOTHING
                        RETURNING key
                        ''',
                        (key, str(value), expires_at),
                    )
                    return cur.fetchone() is not None
                cur.execute(
                    '''
                    INSERT INTO state_kv (key, value, expires_at, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE
                    SET value = EXCLUDED.value,
                        expires_at = EXCLUDED.expires_at,
                        updated_at = NOW()
                    ''',
                    (key, str(value), expires_at),
                )
                return True

    def delete(self, *keys):
        if not keys:
            return 0
        key_list = list(keys)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute('DELETE FROM state_kv WHERE key = ANY(%s)', (key_list,))
                deleted = cur.rowcount
                cur.execute('DELETE FROM state_lists WHERE key = ANY(%s)', (key_list,))
                return deleted + cur.rowcount

    def incr(self, key):
        with self.connection() as conn:
            with conn.cursor() as cur:
                self._purge_expired_key(cur, key)
                cur.execute(
                    '''
                    INSERT INTO state_kv (key, value, expires_at, updated_at)
                    VALUES (%s, '1', NULL, NOW())
                    ON CONFLICT (key) DO UPDATE
                    SET value = ((state_kv.value)::BIGINT + 1)::TEXT,
                        expires_at = NULL,
                        updated_at = NOW()
                    RETURNING value
                    ''',
                    (key,),
                )
                return int(cur.fetchone()[0])

    @staticmethod
    def _slice_redis_range(items, start, end):
        if not items:
            return []
        length = len(items)
        if start < 0:
            start += length
        if end < 0:
            end += length
        start = max(start, 0)
        if start >= length or end < 0:
            return []
        end = min(end, length - 1)
        if end < start:
            return []
        return items[start : end + 1]

    def _get_list(self, cur, key, for_update=False):
        query = 'SELECT items FROM state_lists WHERE key = %s'
        if for_update:
            query += ' FOR UPDATE'
        cur.execute(query, (key,))
        row = cur.fetchone()
        return [] if row is None else list(row[0])

    def _write_list(self, cur, key, items):
        cur.execute(
            '''
            INSERT INTO state_lists (key, items, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE
            SET items = EXCLUDED.items,
                updated_at = NOW()
            ''',
            (key, Json(items)),
        )

    def lpush(self, key, value):
        with self.connection() as conn:
            with conn.cursor() as cur:
                items = self._get_list(cur, key, for_update=True)
                items.insert(0, str(value))
                self._write_list(cur, key, items)
                return len(items)

    def ltrim(self, key, start, end):
        with self.connection() as conn:
            with conn.cursor() as cur:
                items = self._slice_redis_range(self._get_list(cur, key, for_update=True), start, end)
                if items:
                    self._write_list(cur, key, items)
                else:
                    cur.execute('DELETE FROM state_lists WHERE key = %s', (key,))
                return True

    def lrange(self, key, start, end):
        with self.connection() as conn:
            with conn.cursor() as cur:
                return self._slice_redis_range(self._get_list(cur, key), start, end)


redis = PostgresStateStore(RENDER_INTERNAL_DATABASE)


DEFAULT_CONFIGS = {
    SANDBOX_NS: {
        'maker_fee_pct': 0.015,
        'taker_fee_pct': 0.045,
        'maker_entry_slippage_pct': 0.0,
        'maker_exit_slippage_pct': 0.0,
        'taker_exit_slippage_pct': 0.02,
        'trade_notional_usdc': 250.0,
        'max_notional_usdc': 1000.0,
        'max_open_positions': 10,
        'leverage': 1.0,
        'daily_loss_limit_usdc': 200.0,
        'max_trades_per_hour': 0,
        'consecutive_loss_limit': 4,
        'cooldown_after_loss_seconds': 0,
        'entry_cooldown_seconds': 0,
        'take_profit_pct': 0.25,
        'stop_loss_pct': 0.25,
        'time_stop_seconds': 120,
        'emergency_exit_drop_pct': 0.20,
        'emergency_window_seconds': 30,
        'entry_timeout_seconds': 10,
        'return_30s_threshold_pct': 0.25,
        'return_1m_threshold_pct': 0.25,
        'return_2m_threshold_pct': -0.20,
        'bounce_from_2m_low_threshold_pct': 0.05,
        'return_60m_min_pct': -99.0,
        'spread_pct_max': 0.025,
        'book_imbalance_min': 0.50,
        'require_imbalance_improvement': False,
        'starting_balance_usdc': 10000.0,
    },
    REAL_NS: {
        'maker_fee_pct': 0.015,
        'taker_fee_pct': 0.045,
        'trade_notional_usdc': 10.0,
        'max_notional_usdc': 1000.0,
        'max_open_positions': 3,
        'leverage': 1.0,
        'daily_loss_limit_usdc': 2.0,
        'max_trades_per_hour': 3,
        'consecutive_loss_limit': 2,
        'cooldown_after_loss_seconds': 300,
        'entry_cooldown_seconds': 300,
        'take_profit_pct': 0.25,
        'stop_loss_pct': 0.18,
        'time_stop_seconds': 420,
        'emergency_exit_drop_pct': 0.20,
        'emergency_window_seconds': 30,
        'entry_timeout_seconds': 10,
        'return_30s_threshold_pct': 0.25,
        'return_1m_threshold_pct': 0.25,
        'return_2m_threshold_pct': -0.20,
        'bounce_from_2m_low_threshold_pct': 0.05,
        'return_60m_min_pct': -1.00,
        'spread_pct_max': 0.025,
        'book_imbalance_min': 0.52,
        'require_imbalance_improvement': True,
        'simulate_slippage': False,
    },
}

EDITABLE_CONFIG_FIELDS = {
    'maker_fee_pct',
    'taker_fee_pct',
    'maker_entry_slippage_pct',
    'maker_exit_slippage_pct',
    'taker_exit_slippage_pct',
    'trade_notional_usdc',
    'max_notional_usdc',
    'max_open_positions',
    'leverage',
    'daily_loss_limit_usdc',
    'max_trades_per_hour',
    'consecutive_loss_limit',
    'cooldown_after_loss_seconds',
    'entry_cooldown_seconds',
    'take_profit_pct',
    'stop_loss_pct',
    'time_stop_seconds',
    'emergency_exit_drop_pct',
    'emergency_window_seconds',
    'entry_timeout_seconds',
    'return_30s_threshold_pct',
    'return_1m_threshold_pct',
    'return_2m_threshold_pct',
    'bounce_from_2m_low_threshold_pct',
    'return_60m_min_pct',
    'spread_pct_max',
    'book_imbalance_min',
    'require_imbalance_improvement',
    'starting_balance_usdc',
}


class MarketDataStore:
    def __init__(self):
        self.lock = Lock()
        self.snapshot = {}
        self.history = deque()
        self.status = 'BOOTING'
        self.error = ''
        self.rest_info = None
        self.ws_app = None
        self.ws_thread = None
        self.loop_thread = None
        self.last_message_at = 0.0
        self.last_rest_snapshot_at = 0.0
        self.connected_at = 0.0
        self.reconnect_count = 0
        self.last_open_at = 0.0
        self.last_close_at = 0.0
        self.last_close_code = ''
        self.last_close_reason = ''
        self.last_ws_error = ''
        self.last_loop_heartbeat_at = 0.0
        self.last_loop_error = ''

    def ensure_rest_client(self):
        if Info is None or hl_constants is None:
            raise RuntimeError(f'Hyperliquid SDK unavailable: {HYPERLIQUID_IMPORT_ERROR}')
        if self.rest_info is None:
            self.rest_info = Info(hl_constants.MAINNET_API_URL, skip_ws=True)
        return self.rest_info

    def ensure_ws_client(self):
        if websocket is None:
            raise RuntimeError(f'Hyperliquid websocket client unavailable: {HYPERLIQUID_IMPORT_ERROR}')
        return websocket

    def close_stream(self):
        ws_app = self.ws_app
        ws_thread = self.ws_thread
        self.ws_app = None
        self.ws_thread = None
        if not ws_app:
            return
        try:
            ws_app.close()
        except Exception:
            pass
        try:
            if ws_thread and ws_thread.is_alive():
                ws_thread.join(timeout=2)
        except Exception:
            pass

    def ensure_running(self):
        with self.lock:
            loop_thread = self.loop_thread
            if loop_thread and loop_thread.is_alive():
                return False
            loop_thread = Thread(target=self.loop, daemon=True, name='market-data-loop')
            self.loop_thread = loop_thread
        loop_thread.start()
        return True

    def _record_ws_callback_error(self, exc, context, ws_app=None):
        message = f'Hyperliquid websocket {context} failed: {exc}'
        with self.lock:
            self.status = 'ERROR'
            self.error = message
            self.last_ws_error = message
        if ws_app is not None:
            try:
                ws_app.close()
            except Exception:
                pass

    def _run_ws_forever(self):
        ws_app = self.ws_app
        if ws_app is None:
            return
        try:
            ws_app.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            self._record_ws_callback_error(exc, 'run_forever', ws_app)
            return
        with self.lock:
            if self.ws_app is ws_app and not self.last_close_at and not self.last_ws_error:
                self.last_ws_error = 'Hyperliquid websocket run_forever exited without close callback.'
    def _build_snapshot(self, book_data, source):
        levels = book_data.get('levels') or [[], []]
        bids = levels[0] if len(levels) > 0 else []
        asks = levels[1] if len(levels) > 1 else []
        if not bids or not asks:
            raise RuntimeError('Order book snapshot missing bids or asks.')
        best_bid = float(bids[0]['px'])
        best_ask = float(asks[0]['px'])
        mid = (best_bid + best_ask) / 2.0
        spread_pct = ((best_ask - best_bid) / mid) * 100.0 if mid > 0 else 0.0
        bid_depth = sum(float(level['sz']) for level in bids[:5])
        ask_depth = sum(float(level['sz']) for level in asks[:5])
        total_depth = bid_depth + ask_depth
        book_imbalance = (bid_depth / total_depth) if total_depth > 0 else 0.5
        event_ts = time.time()
        raw_time = book_data.get('time')
        if raw_time:
            event_ts = float(raw_time) / 1000.0
        return {
            'ts': event_ts,
            'source': source,
            'best_bid': best_bid,
            'best_ask': best_ask,
            'mid': mid,
            'spread_pct': spread_pct,
            'bid_depth_top5': bid_depth,
            'ask_depth_top5': ask_depth,
            'book_imbalance': book_imbalance,
        }

    def _record_snapshot(self, snapshot, websocket_event):
        with self.lock:
            self.snapshot = snapshot
            self.history.append(snapshot)
            cutoff = snapshot['ts'] - MARKET_HISTORY_SECONDS
            while self.history and self.history[0]['ts'] < cutoff:
                self.history.popleft()
            self.error = ''
            now = time.time()
            if websocket_event:
                self.status = 'LIVE'
                self.last_message_at = now
            else:
                self.status = 'SNAPSHOT_ONLY'
                self.last_rest_snapshot_at = now

    def on_book_message(self, book_msg):
        if not isinstance(book_msg, dict):
            return
        book_data = book_msg.get('data') or {}
        if book_data.get('coin') != MARKET_COIN:
            return
        snapshot = self._build_snapshot(book_data, source='websocket')
        self._record_snapshot(snapshot, websocket_event=True)

    def _on_ws_open(self, ws_app):
        try:
            with self.lock:
                self.status = 'SUBSCRIBING'
                self.error = ''
                self.connected_at = time.time()
                self.last_open_at = self.connected_at
            ws_app.send(json.dumps({'method': 'subscribe', 'subscription': {'type': 'l2Book', 'coin': MARKET_COIN}}))
        except Exception as exc:
            self._record_ws_callback_error(exc, 'open callback', ws_app)

    def _on_ws_message(self, _ws_app, raw_message):
        try:
            if raw_message == 'Websocket connection established.':
                return
            ws_msg = json.loads(raw_message)
        except Exception:
            return
        try:
            if ws_msg.get('channel') == 'pong':
                return
            if ws_msg.get('channel') == 'subscriptionResponse':
                with self.lock:
                    self.status = 'SUBSCRIBED'
                return
            if ws_msg.get('channel') != 'l2Book':
                return
            self.on_book_message(ws_msg)
        except Exception as exc:
            self._record_ws_callback_error(exc, 'message handler', _ws_app)

    def _on_ws_error(self, _ws_app, error):
        with self.lock:
            self.status = 'ERROR'
            self.error = f'Hyperliquid websocket error: {error}'
            self.last_ws_error = str(error)

    def _on_ws_close(self, _ws_app, status_code, close_msg):
        with self.lock:
            self.last_close_at = time.time()
            self.last_close_code = '' if status_code is None else str(status_code)
            self.last_close_reason = close_msg or ''
            if self.last_message_at:
                self.status = 'DISCONNECTED'
            else:
                self.status = 'ERROR'
            if not self.error:
                self.error = f'Hyperliquid websocket closed ({status_code}): {close_msg or "no message"}'

    def connect_stream(self):
        ws_module = self.ensure_ws_client()
        self.close_stream()
        with self.lock:
            self.status = 'CONNECTING'
            self.error = ''
            self.last_message_at = 0.0
            self.last_rest_snapshot_at = 0.0
            self.connected_at = 0.0
            self.last_close_at = 0.0
            self.last_close_code = ''
            self.last_close_reason = ''
            self.last_ws_error = ''
            self.reconnect_count += 1
        snapshot_book = self.ensure_rest_client().l2_snapshot(MARKET_COIN)
        self._record_snapshot(self._build_snapshot(snapshot_book, source='rest_seed'), websocket_event=False)
        self.ws_app = ws_module.WebSocketApp(
            HYPERLIQUID_WS_URL,
            on_open=self._on_ws_open,
            on_message=self._on_ws_message,
            on_error=self._on_ws_error,
            on_close=self._on_ws_close,
        )
        self.ws_thread = Thread(
            target=self._run_ws_forever,
            daemon=True,
        )
        self.ws_thread.start()

    def loop(self):
        while True:
            try:
                with self.lock:
                    self.last_loop_heartbeat_at = time.time()
                    self.last_loop_error = ''
                self.connect_stream()
                while True:
                    time.sleep(1)
                    with self.lock:
                        self.last_loop_heartbeat_at = time.time()
                        last_message_at = self.last_message_at
                        connected_at = self.connected_at
                        ws_thread = self.ws_thread
                    if ws_thread and not ws_thread.is_alive():
                        raise RuntimeError('Hyperliquid websocket thread exited unexpectedly.')
                    if not last_message_at:
                        if connected_at and (time.time() - connected_at) > MARKET_STALE_AFTER_SECONDS:
                            raise RuntimeError('Hyperliquid websocket connected but no l2Book messages arrived.')
                        continue
                    age = time.time() - last_message_at
                    if age > MARKET_STALE_AFTER_SECONDS:
                        raise RuntimeError(f'Hyperliquid websocket stale ({age:.1f}s without book update).')
            except Exception as exc:
                with self.lock:
                    self.status = 'ERROR'
                    self.error = str(exc)
                    self.last_loop_error = str(exc)
            finally:
                self.close_stream()
            time.sleep(2)

    def get_snapshot(self):
        self.ensure_running()
        with self.lock:
            snapshot = dict(self.snapshot)
            history = list(self.history)
            status = self.status
            error = self.error
            last_message_at = self.last_message_at
            last_rest_snapshot_at = self.last_rest_snapshot_at
        now = time.time()
        effective_snapshot = dict(snapshot)
        effective_status = status
        effective_error = error
        if last_message_at:
            ws_age = now - last_message_at
            if ws_age > MARKET_STALE_AFTER_SECONDS:
                effective_status = 'STALE'
                effective_error = f'Hyperliquid websocket stale ({ws_age:.1f}s without book update).'
                effective_snapshot['source'] = 'websocket_stale'
        elif last_rest_snapshot_at:
            rest_age = now - last_rest_snapshot_at
            if rest_age > MARKET_STALE_AFTER_SECONDS:
                effective_status = 'STALE'
                effective_error = f'Only REST seed available ({rest_age:.1f}s old); no websocket book updates received.'
                effective_snapshot['source'] = 'rest_seed_stale'
        return effective_snapshot, history, effective_status, effective_error

    def get_diagnostics(self):
        with self.lock:
            return {
                'status': self.status,
                'error': self.error,
                'last_message_at': self.last_message_at,
                'last_rest_snapshot_at': self.last_rest_snapshot_at,
                'connected_at': self.connected_at,
                'reconnect_count': self.reconnect_count,
                'last_open_at': self.last_open_at,
                'last_close_at': self.last_close_at,
                'last_close_code': self.last_close_code,
                'last_close_reason': self.last_close_reason,
                'last_ws_error': self.last_ws_error,
                'loop_thread_alive': bool(self.loop_thread and self.loop_thread.is_alive()),
                'last_loop_heartbeat_at': self.last_loop_heartbeat_at,
                'last_loop_error': self.last_loop_error,
                'ws_thread_alive': bool(self.ws_thread and self.ws_thread.is_alive()),
            }


class MarketUniverseStore:
    def __init__(self):
        self.lock = Lock()
        self.history = {}
        self.current_mids = {}
        self.universe = []
        self.sz_decimals = {}
        self.meta_by_coin = {}
        self.last_message_at = 0.0
        self.last_meta_refresh_at = 0.0
        self.last_error = ''
        self.last_open_at = 0.0
        self.last_close_at = 0.0
        self.last_close_code = ''
        self.last_close_reason = ''
        self.loop_thread = None
        self.ws_thread = None
        self.ws_app = None

    def _post_info(self, payload):
        req = urllib_request.Request(
            HYPERLIQUID_INFO_URL,
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        with urllib_request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode('utf-8'))

    def refresh_meta(self, force=False):
        now = time.time()
        with self.lock:
            if not force and self.universe and (now - self.last_meta_refresh_at) < UNIVERSE_META_REFRESH_INTERVAL_SECONDS:
                return
        meta = self._post_info({'type': 'meta'})
        universe = []
        sz_decimals = {}
        meta_by_coin = {}
        for asset in meta.get('universe') or []:
            coin = asset.get('name')
            if not coin:
                continue
            universe.append(coin)
            sz_decimals[coin] = int(asset.get('szDecimals', 0))
            meta_by_coin[coin] = infer_perp_metadata(coin, asset)
        with self.lock:
            self.universe = universe
            self.sz_decimals = sz_decimals
            self.meta_by_coin = meta_by_coin
            self.last_meta_refresh_at = now

    def ensure_running(self):
        with self.lock:
            loop_thread = self.loop_thread
            if loop_thread and loop_thread.is_alive():
                return False
            loop_thread = Thread(target=self.loop, daemon=True, name='market-universe-loop')
            self.loop_thread = loop_thread
        loop_thread.start()
        return True

    def close_stream(self):
        ws_app = self.ws_app
        ws_thread = self.ws_thread
        self.ws_app = None
        self.ws_thread = None
        if not ws_app:
            return
        try:
            ws_app.close()
        except Exception:
            pass
        try:
            if ws_thread and ws_thread.is_alive():
                ws_thread.join(timeout=2)
        except Exception:
            pass

    def _record_mid_update(self, mids):
        now = time.time()
        with self.lock:
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
                history.append({'ts': now, 'mid': mid})
                cutoff = now - MARKET_HISTORY_SECONDS
                while history and history[0]['ts'] < cutoff:
                    history.popleft()
            self.last_message_at = now
            self.last_error = ''

    def _on_ws_open(self, ws_app):
        self.refresh_meta(force=True)
        with self.lock:
            self.last_open_at = time.time()
            self.last_error = ''
        ws_app.send(json.dumps({'method': 'subscribe', 'subscription': {'type': 'allMids'}}))

    def _on_ws_message(self, _ws_app, raw_message):
        try:
            if raw_message == 'Websocket connection established.':
                return
            msg = json.loads(raw_message)
        except Exception:
            return
        if msg.get('channel') == 'subscriptionResponse':
            return
        if msg.get('channel') != 'allMids':
            return
        data = msg.get('data') or {}
        mids = data.get('mids') if isinstance(data, dict) and 'mids' in data else data
        if isinstance(mids, dict):
            self._record_mid_update(mids)

    def _on_ws_error(self, _ws_app, error):
        with self.lock:
            self.last_error = f'Hyperliquid allMids websocket error: {error}'

    def _on_ws_close(self, _ws_app, status_code, close_msg):
        with self.lock:
            self.last_close_at = time.time()
            self.last_close_code = '' if status_code is None else str(status_code)
            self.last_close_reason = close_msg or ''
            if not self.last_error:
                self.last_error = f'Hyperliquid allMids websocket closed ({status_code}): {close_msg or "no message"}'

    def connect_stream(self):
        if websocket is None:
            raise RuntimeError(f'Hyperliquid websocket client unavailable: {HYPERLIQUID_IMPORT_ERROR}')
        self.close_stream()
        self.refresh_meta(force=True)
        self.ws_app = websocket.WebSocketApp(
            HYPERLIQUID_WS_URL,
            on_open=self._on_ws_open,
            on_message=self._on_ws_message,
            on_error=self._on_ws_error,
            on_close=self._on_ws_close,
        )
        self.ws_thread = Thread(target=lambda: self.ws_app.run_forever(ping_interval=20, ping_timeout=10), daemon=True)
        self.ws_thread.start()

    def loop(self):
        while True:
            try:
                self.connect_stream()
                while True:
                    time.sleep(1)
                    now = time.time()
                    with self.lock:
                        ws_thread = self.ws_thread
                        last_message_at = self.last_message_at
                        last_meta_refresh_at = self.last_meta_refresh_at
                    if ws_thread and not ws_thread.is_alive():
                        raise RuntimeError('Hyperliquid allMids websocket thread exited unexpectedly.')
                    if last_meta_refresh_at and (now - last_meta_refresh_at) > UNIVERSE_META_REFRESH_INTERVAL_SECONDS:
                        self.refresh_meta(force=True)
                    if last_message_at and (now - last_message_at) > MARKET_STALE_AFTER_SECONDS:
                        raise RuntimeError(f'Hyperliquid allMids websocket stale ({now - last_message_at:.1f}s without update).')
            except Exception as exc:
                with self.lock:
                    self.last_error = str(exc)
            finally:
                self.close_stream()
            time.sleep(2)

    def _return_for(self, history, seconds_ago):
        if not history:
            return None
        target_ts = time.time() - seconds_ago
        candidate = None
        for item in history:
            if item['ts'] <= target_ts:
                candidate = item
            else:
                break
        if candidate is None:
            return None
        latest_mid = history[-1]['mid']
        return pct_change(candidate['mid'], latest_mid)

    def get_metrics_for_coin(self, coin):
        self.ensure_running()
        with self.lock:
            history = list(self.history.get(coin, []))
            latest_mid = float(self.current_mids.get(coin, 0.0))
            last_message_at = self.last_message_at
        if not history or latest_mid <= 0:
            return None
        now = time.time()
        return {
            'coin': coin,
            'mid': latest_mid,
            'return_1m': self._return_for(history, 60) or 0.0,
            'return_2m': self._return_for(history, 120) or 0.0,
            'return_5m': self._return_for(history, 300) or 0.0,
            'return_15m': self._return_for(history, 900) or 0.0,
            'return_60m': self._return_for(history, 3600) or 0.0,
            'mid_2m_low': min_mid_since(history, 120) or latest_mid,
            'market_data_age': max(0.0, now - last_message_at) if last_message_at else 9999.0,
            'history': history,
        }

    def get_hot_perps(self, limit=10, primary_basis='5m'):
        self.ensure_running()
        with self.lock:
            histories = {coin: list(items) for coin, items in self.history.items()}
            mids = dict(self.current_mids)
            meta_by_coin = dict(self.meta_by_coin)
            last_error = self.last_error
        ranked = []
        for coin, history in histories.items():
            if not history or coin not in mids:
                continue
            latest_mid = mids[coin]
            return_30s = self._return_for(history, 30)
            return_1m = self._return_for(history, 60)
            return_5m = self._return_for(history, 300)
            return_15m = self._return_for(history, 900)
            oldest_mid = history[0]['mid']
            warmup_return = pct_change(oldest_mid, latest_mid) if len(history) > 1 else 0.0
            basis_candidates = []
            fallback_bases = ('1m', '5m', 'warmup') if primary_basis == '30s' else ('5m', '1m', 'warmup')
            for basis_name in (primary_basis, *fallback_bases):
                if basis_name not in basis_candidates:
                    basis_candidates.append(basis_name)
            score = None
            basis = 'warmup'
            for basis_name in basis_candidates:
                if basis_name == '30s' and return_30s is not None:
                    score = return_30s
                    basis = '30s'
                    break
                if basis_name == '1m' and return_1m is not None:
                    score = return_1m
                    basis = '1m'
                    break
                if basis_name == '5m' and return_5m is not None:
                    score = return_5m
                    basis = '5m'
                    break
                if basis_name == '15m' and return_15m is not None:
                    score = return_15m
                    basis = '15m'
                    break
                if basis_name == 'warmup':
                    score = warmup_return
                    basis = 'warmup'
                    break
            basis_description = {
                '30s': 'micro momentum',
                '15m': 'trend momentum',
                '5m': 'medium momentum',
                '1m': 'short momentum',
                'warmup': 'limited history',
            }.get(basis, '')
            metadata = meta_by_coin.get(coin) or infer_perp_metadata(coin)
            category = (metadata.get('category') or '').strip()
            description = (metadata.get('description') or '').strip()
            ranked.append(
                {
                    'coin': coin,
                    'mid': trim_float(latest_mid, 6),
                    'score_pct': trim_float(score, 4),
                    'score_basis': basis,
                    'score_basis_description': basis_description,
                    'category': category,
                    'description': description,
                    'return_30s': trim_float(return_30s or 0.0, 4),
                    'return_1m': trim_float(return_1m or 0.0, 4),
                    'return_5m': trim_float(return_5m or 0.0, 4),
                    'return_15m': trim_float(return_15m or 0.0, 4),
                }
            )
        ranked.sort(key=lambda item: item['score_pct'], reverse=True)
        return {'leaders': ranked[:limit], 'last_error': last_error}


market_data = MarketDataStore()
market_universe = MarketUniverseStore()
real_client_lock = Lock()
real_client_bundle = {'info': None, 'exchange': None, 'account_address': '', 'size_decimals': 2}


def ns_key(namespace, key):
    return f'{namespace}:{key}'


def json_default(value):
    return deepcopy(value)


def ns_get(namespace, key, default=None):
    try:
        value = redis.get(ns_key(namespace, key))
        return default if value is None else value
    except Exception:
        return default


def ns_set(namespace, key, value):
    redis.set(ns_key(namespace, key), str(value))


def ns_delete(namespace, *keys):
    for key in keys:
        redis.delete(ns_key(namespace, key))


def ns_incr(namespace, key):
    return int(redis.incr(ns_key(namespace, key)))


def ns_get_json(namespace, key, default=None):
    raw = ns_get(namespace, key)
    if raw is None:
        return json_default(default)
    try:
        return json.loads(raw)
    except Exception:
        return json_default(default)


def ns_set_json(namespace, key, value):
    ns_set(namespace, key, json.dumps(value))


def ns_lpush(namespace, key, value):
    redis.lpush(ns_key(namespace, key), value)


def ns_ltrim(namespace, key, start, end):
    redis.ltrim(ns_key(namespace, key), start, end)


def ns_lrange(namespace, key, start, end):
    return redis.lrange(ns_key(namespace, key), start, end)


def read_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def utc_now():
    return datetime.now(timezone.utc)


def iso_now():
    return utc_now().isoformat()


def format_timestamp(ts):
    if not ts:
        return ''
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


FX_CODE_TO_NAME = {
    'AUD': 'Australian dollar',
    'CAD': 'Canadian dollar',
    'CHF': 'Swiss franc',
    'EUR': 'Euro',
    'GBP': 'British pound',
    'JPY': 'Japanese yen',
    'NZD': 'New Zealand dollar',
    'USD': 'US dollar',
}

KNOWN_PERP_METADATA = {
    'SPX': ('Equity Index', 'S&P 500 index'),
    'NDX': ('Equity Index', 'Nasdaq 100 index'),
    'DJI': ('Equity Index', 'Dow Jones Industrial Average'),
    'VIX': ('Volatility Index', 'Cboe Volatility Index'),
    'XAU': ('Commodity', 'Gold'),
    'XAG': ('Commodity', 'Silver'),
    'WTI': ('Commodity', 'WTI crude oil'),
    'BRENT': ('Commodity', 'Brent crude oil'),
    'NATGAS': ('Commodity', 'Natural gas'),
    'COPPER': ('Commodity', 'Copper'),
}


def infer_perp_metadata(coin, asset=None):
    asset = asset or {}
    raw_category = (
        asset.get('category')
        or asset.get('type')
        or asset.get('sector')
        or asset.get('group')
        or asset.get('tag')
        or ''
    )
    raw_description = (
        asset.get('description')
        or asset.get('displayName')
        or asset.get('fullName')
        or asset.get('longName')
        or ''
    )
    if raw_category or raw_description:
        return {
            'category': raw_category or 'Perp',
            'description': raw_description or f'{coin} perp',
        }

    symbol = (coin or '').upper()
    if symbol in KNOWN_PERP_METADATA:
        category, description = KNOWN_PERP_METADATA[symbol]
        return {'category': category, 'description': description}

    if re.fullmatch(r'[A-Z]{6}', symbol):
        base = symbol[:3]
        quote = symbol[3:]
        if base in FX_CODE_TO_NAME and quote in FX_CODE_TO_NAME:
            return {
                'category': 'FX',
                'description': f'{FX_CODE_TO_NAME[base]} / {FX_CODE_TO_NAME[quote]}',
            }

    if re.fullmatch(r'US\d{1,2}Y', symbol):
        years = symbol[2:-1]
        return {
            'category': 'Rates',
            'description': f'US {years}-year Treasury yield',
        }

    if symbol.startswith('K') and len(symbol) > 1:
        return {
            'category': 'Crypto',
            'description': f'{symbol[1:]} scaled crypto perp',
        }

    return {
        'category': 'Crypto',
        'description': f'{symbol} crypto perp',
    }


def current_day_key():
    return utc_now().strftime('%Y-%m-%d')


def pct_change(from_price, to_price):
    if from_price <= 0:
        return 0.0
    return ((to_price - from_price) / from_price) * 100.0


def trim_float(value, digits=6):
    return round(float(value), digits)


def position_default():
    return None


def open_order_default():
    return None


def positions_default():
    return {}


def open_orders_default():
    return {}


def stats_default(namespace):
    return {
        'day': current_day_key(),
        'daily_pnl': 0.0,
        'total_pnl': 0.0,
        'consecutive_losses': 0,
        'trades_today': 0,
        'cancelled_entries': 0,
        'time_stops': 0,
        'emergency_exits': 0,
        'last_entry_fill_at': 0.0,
        'last_loss_at': 0.0,
        'last_close_at': 0.0,
        'last_real_sync_error': '',
        'real_open_orders': [],
        'real_balance': {'available': 0.0, 'equity': 0.0},
        'unsupported_external_position': '',
        'sandbox_balance_initialized': namespace == SANDBOX_NS,
    }


def get_config(namespace):
    stored = ns_get_json(namespace, 'config', {}) or {}
    merged = deepcopy(DEFAULT_CONFIGS[namespace])
    merged.update(stored)
    if namespace == SANDBOX_NS:
        merged['trade_notional_usdc'] = min(merged['trade_notional_usdc'], merged['max_notional_usdc'])
    return merged


def save_config(namespace, config):
    ns_set_json(namespace, 'config', config)


def get_stats(namespace):
    stats = ns_get_json(namespace, 'stats', stats_default(namespace)) or stats_default(namespace)
    if stats.get('day') != current_day_key():
        stats['day'] = current_day_key()
        stats['daily_pnl'] = 0.0
        stats['trades_today'] = 0
        save_stats(namespace, stats)
    return stats


def save_stats(namespace, stats):
    ns_set_json(namespace, 'stats', stats)


def get_position(namespace):
    positions = get_positions(namespace)
    if not positions:
        return None
    return next(iter(positions.values()))


def save_position(namespace, position):
    if position is None:
        save_positions(namespace, {})
        return
    coin = position.get('coin') or MARKET_COIN
    positions = get_positions(namespace)
    positions[coin] = position
    save_positions(namespace, positions)


def get_open_order(namespace):
    open_orders = get_open_orders(namespace)
    if not open_orders:
        return None
    return next(iter(open_orders.values()))


def save_open_order(namespace, order):
    if order is None:
        save_open_orders(namespace, {})
        return
    coin = order.get('coin') or MARKET_COIN
    open_orders = get_open_orders(namespace)
    open_orders[coin] = order
    save_open_orders(namespace, open_orders)


def get_positions(namespace):
    positions = ns_get_json(namespace, 'positions', positions_default()) or {}
    if positions:
        return positions
    legacy = ns_get_json(namespace, 'position', position_default())
    if legacy:
        coin = legacy.get('coin') or MARKET_COIN
        return {coin: legacy}
    return {}


def save_positions(namespace, positions):
    positions = positions or {}
    if positions:
        ns_set_json(namespace, 'positions', positions)
    else:
        ns_delete(namespace, 'positions')
    ns_delete(namespace, 'position')


def get_position_for_coin(namespace, coin):
    return get_positions(namespace).get(coin)


def save_position_for_coin(namespace, coin, position):
    positions = get_positions(namespace)
    if position is None:
        positions.pop(coin, None)
    else:
        position['coin'] = coin
        positions[coin] = position
    save_positions(namespace, positions)


def get_open_orders(namespace):
    open_orders = ns_get_json(namespace, 'open_orders', open_orders_default()) or {}
    if open_orders:
        return open_orders
    legacy = ns_get_json(namespace, 'open_order', open_order_default())
    if legacy:
        coin = legacy.get('coin') or MARKET_COIN
        return {coin: legacy}
    return {}


def save_open_orders(namespace, open_orders):
    open_orders = open_orders or {}
    if open_orders:
        ns_set_json(namespace, 'open_orders', open_orders)
    else:
        ns_delete(namespace, 'open_orders')
    ns_delete(namespace, 'open_order')


def get_open_order_for_coin(namespace, coin):
    return get_open_orders(namespace).get(coin)


def save_open_order_for_coin(namespace, coin, order):
    open_orders = get_open_orders(namespace)
    if order is None:
        open_orders.pop(coin, None)
    else:
        order['coin'] = coin
        open_orders[coin] = order
    save_open_orders(namespace, open_orders)


def get_balances(namespace):
    if namespace == SANDBOX_NS:
        default = {'available': DEFAULT_CONFIGS[SANDBOX_NS]['starting_balance_usdc'], 'equity': DEFAULT_CONFIGS[SANDBOX_NS]['starting_balance_usdc']}
    else:
        default = {'available': 0.0, 'equity': 0.0}
    return ns_get_json(namespace, 'balances', default) or default


def save_balances(namespace, balances):
    ns_set_json(namespace, 'balances', balances)


def get_bot_state(namespace):
    return ns_get(namespace, 'bot_state', 'PAUSED')


def set_bot_state(namespace, value):
    ns_set(namespace, 'bot_state', value)


def get_engine_status(namespace):
    return ns_get(namespace, 'engine_status', 'WAITING_FOR_START')


def set_engine_status(namespace, value):
    ns_set(namespace, 'engine_status', value)


def get_reason_not_taken(namespace):
    return ns_get(namespace, 'reason_not_taken', 'Waiting for first signal evaluation.')


def set_reason_not_taken(namespace, value):
    ns_set(namespace, 'reason_not_taken', value)


def get_last_signal(namespace):
    return ns_get_json(namespace, 'last_signal', {}) or {}


def set_last_signal(namespace, value):
    ns_set_json(namespace, 'last_signal', value)


def get_last_trade(namespace):
    return ns_get_json(namespace, 'last_trade', {}) or {}


def set_last_trade(namespace, value):
    ns_set_json(namespace, 'last_trade', value)


def push_text_log(namespace, message):
    timestamp = utc_now().strftime('%H:%M:%S')
    env_label = namespace.upper()
    line = f'[{timestamp}] [{env_label}] {message}'
    ns_lpush(namespace, 'action_logs', line)
    ns_ltrim(namespace, 'action_logs', 0, ACTION_LOG_LIMIT - 1)
    print(line)


def push_json_log(namespace, key, payload, limit):
    ns_lpush(namespace, key, json.dumps(payload))
    ns_ltrim(namespace, key, 0, limit - 1)


def load_json_log(namespace, key, limit):
    records = []
    for raw in ns_lrange(namespace, key, 0, max(0, limit - 1)):
        try:
            records.append(json.loads(raw))
        except Exception:
            continue
    return records


def load_action_logs(namespace):
    return ns_lrange(namespace, 'action_logs', 0, ACTION_LOG_LIMIT - 1)


def record_signal(namespace, snapshot, metrics, passed, reason, checks):
    payload = {
        'timestamp': iso_now(),
        'environment': namespace.upper(),
        'bot_state': get_bot_state(namespace),
        'coin': snapshot.get('coin', 'N/A'),
        'mid': trim_float(snapshot.get('mid', 0.0), 6),
        'best_bid': trim_float(snapshot.get('best_bid', 0.0), 6),
        'best_ask': trim_float(snapshot.get('best_ask', 0.0), 6),
        'spread_pct': trim_float(metrics['spread_pct'], 6),
        'return_30s': trim_float(metrics.get('return_30s', 0.0), 6),
        'return_1m': trim_float(metrics.get('return_1m', 0.0), 6),
        'return_2m': trim_float(metrics['return_2m'], 6),
        'return_5m': trim_float(metrics['return_5m'], 6),
        'return_15m': trim_float(metrics['return_15m'], 6),
        'return_60m': trim_float(metrics['return_60m'], 6),
        'bounce_from_2m_low': trim_float(metrics['bounce_from_2m_low'], 6),
        'book_imbalance': trim_float(metrics['book_imbalance'], 6),
        'entry_conditions_passed': passed,
        'reason_not_taken': reason,
        'checks': checks,
    }
    push_json_log(namespace, 'signal_log', payload, SIGNAL_LOG_LIMIT)
    set_last_signal(namespace, payload)
    set_reason_not_taken(namespace, reason)


def record_trade(namespace, trade):
    trade_id = ns_incr(namespace, 'trade_counter')
    trade['id'] = trade_id
    push_json_log(namespace, 'trade_log', trade, TRADE_LOG_LIMIT)
    set_last_trade(namespace, trade)


def round_down(value, decimals):
    factor = 10 ** max(int(decimals), 0)
    return math.floor(float(value) * factor) / factor if factor else float(value)


def get_real_clients():
    with real_client_lock:
        if real_client_bundle['info'] and real_client_bundle['exchange']:
            return real_client_bundle
        if Info is None or Exchange is None or Account is None or hl_constants is None:
            raise RuntimeError(f'Hyperliquid SDK unavailable: {HYPERLIQUID_IMPORT_ERROR}')
        if not HYPERLIQUID_API_KEY or not HYPERLIQUID_API_SECRET:
            raise RuntimeError('HYPERLIQUID_API_KEY and HYPERLIQUID_API_SECRET are required for REAL mode.')
        wallet = Account.from_key(HYPERLIQUID_API_SECRET)
        info = Info(hl_constants.MAINNET_API_URL, skip_ws=True)
        exchange = Exchange(wallet, hl_constants.MAINNET_API_URL, account_address=HYPERLIQUID_API_KEY)
        size_decimals = int(exchange.info.asset_to_sz_decimals[exchange.info.name_to_asset(MARKET_COIN)])
        real_client_bundle.update(
            {
                'info': info,
                'exchange': exchange,
                'account_address': HYPERLIQUID_API_KEY,
                'size_decimals': size_decimals,
            }
        )
        return real_client_bundle


def reset_sandbox_state(clear_logs=False):
    config = get_config(SANDBOX_NS)
    save_positions(SANDBOX_NS, {})
    save_open_orders(SANDBOX_NS, {})
    save_balances(
        SANDBOX_NS,
        {
            'available': float(config['starting_balance_usdc']),
            'equity': float(config['starting_balance_usdc']),
        },
    )
    ns_set_json(SANDBOX_NS, 'stats', stats_default(SANDBOX_NS))
    set_engine_status(SANDBOX_NS, 'PAUSED_WAITING_FOR_START')
    set_bot_state(SANDBOX_NS, 'PAUSED')
    set_reason_not_taken(SANDBOX_NS, 'Waiting for manual START.')
    set_last_signal(SANDBOX_NS, {})
    if clear_logs:
        redis.delete(ns_key(SANDBOX_NS, 'signal_log'))
        redis.delete(ns_key(SANDBOX_NS, 'trade_log'))
        redis.delete(ns_key(SANDBOX_NS, 'action_logs'))
    push_text_log(SANDBOX_NS, 'Sandbox reset to starting balance and paused.')


def bootstrap_environment(namespace):
    save_config(namespace, get_config(namespace))
    set_bot_state(namespace, 'PAUSED')
    set_engine_status(namespace, 'PAUSED_WAITING_FOR_START')
    set_reason_not_taken(namespace, 'Waiting for manual START.')
    if not get_last_signal(namespace):
        set_last_signal(namespace, {})
    if not get_last_trade(namespace):
        set_last_trade(namespace, {})
    if namespace == SANDBOX_NS:
        reset_sandbox_state(clear_logs=False)
    else:
        save_open_order(namespace, None)
        save_stats(namespace, get_stats(namespace))
        save_balances(namespace, get_balances(namespace))
        save_position(namespace, None)
        push_text_log(namespace, 'REAL environment booted in PAUSED mode. No orders submitted.')


for env_name in ENVIRONMENTS:
    bootstrap_environment(env_name)


def nearest_value(history, seconds_ago, field):
    if not history:
        return None
    target_ts = time.time() - seconds_ago
    candidate = None
    for item in history:
        if item['ts'] <= target_ts:
            candidate = item
        else:
            break
    if candidate is None:
        candidate = history[0]
    return candidate.get(field)


def min_mid_since(history, seconds_ago):
    if not history:
        return None
    cutoff = time.time() - seconds_ago
    mids = [item['mid'] for item in history if item['ts'] >= cutoff]
    if not mids:
        mids = [item['mid'] for item in history]
    return min(mids) if mids else None


def compute_market_metrics(snapshot, history):
    mid = snapshot.get('mid', 0.0)
    anchor_30s = nearest_value(history, 30, 'mid')
    anchor_1m = nearest_value(history, 60, 'mid')
    anchor_2m = nearest_value(history, 120, 'mid')
    anchor_5m = nearest_value(history, 300, 'mid')
    anchor_15m = nearest_value(history, 900, 'mid')
    anchor_60m = nearest_value(history, 3600, 'mid')
    imbalance_10s = nearest_value(history, 10, 'book_imbalance')
    low_2m = min_mid_since(history, 120)
    return {
        'mid': mid,
        'best_bid': snapshot.get('best_bid', 0.0),
        'best_ask': snapshot.get('best_ask', 0.0),
        'spread_pct': snapshot.get('spread_pct', 0.0),
        'book_imbalance': snapshot.get('book_imbalance', 0.0),
        'book_imbalance_10s_ago': imbalance_10s if imbalance_10s is not None else snapshot.get('book_imbalance', 0.0),
        'return_30s': pct_change(anchor_30s or mid, mid),
        'return_1m': pct_change(anchor_1m or mid, mid),
        'return_2m': pct_change(anchor_2m or mid, mid),
        'return_5m': pct_change(anchor_5m or mid, mid),
        'return_15m': pct_change(anchor_15m or mid, mid),
        'return_60m': pct_change(anchor_60m or mid, mid),
        'bounce_from_2m_low': pct_change(low_2m or mid, mid),
        'mid_2m_low': low_2m or mid,
        'market_data_age': max(0.0, time.time() - snapshot.get('ts', 0.0)) if snapshot else 0.0,
    }


def update_position_extremes(position, current_mid):
    if not position:
        return position
    position['highest_mid'] = max(position.get('highest_mid', current_mid), current_mid)
    position['lowest_mid'] = min(position.get('lowest_mid', current_mid), current_mid)
    entry_price = float(position['filled_price'])
    position['max_favourable_excursion'] = max(
        float(position.get('max_favourable_excursion', 0.0)),
        pct_change(entry_price, position['highest_mid']),
    )
    position['max_adverse_excursion'] = min(
        float(position.get('max_adverse_excursion', 0.0)),
        pct_change(entry_price, position['lowest_mid']),
    )
    return position


def compute_position_view(position, mid):
    if not position:
        return {'active': False}
    size = float(position['size'])
    entry = float(position['filled_price'])
    current_price = float(mid)
    gross = (mid - entry) * size
    pnl_pct = pct_change(entry, mid)
    coin = position.get('coin') or MARKET_COIN
    return {
        'active': True,
        'coin': coin,
        'side': position.get('side', 'LONG'),
        'size': trim_float(size, 4),
        'entry_price': trim_float(entry, 6),
        'current_price': trim_float(current_price, 6),
        'submitted_price': trim_float(position.get('submitted_price', entry), 6),
        'notional': trim_float(position.get('notional', size * entry), 4),
        'entry_time': position.get('entry_time'),
        'entry_time_label': format_timestamp(position.get('entry_time')),
        'seconds_open': max(0, int(time.time() - float(position.get('entry_time', time.time())))),
        'gross_unrealized_pnl': trim_float(gross, 4),
        'gross_unrealized_pnl_pct': trim_float(pnl_pct, 4),
        'max_favourable_excursion': trim_float(position.get('max_favourable_excursion', 0.0), 4),
        'max_adverse_excursion': trim_float(position.get('max_adverse_excursion', 0.0), 4),
        'entry_maker_or_taker': position.get('entry_maker_or_taker', 'maker'),
        'target_take_profit_pct': trim_float(position.get('take_profit_pct', 0.0), 4),
        'target_stop_loss_pct': trim_float(position.get('stop_loss_pct', 0.0), 4),
    }


def sync_real_account_state():
    stats = get_stats(REAL_NS)
    try:
        bundle = get_real_clients()
        info = bundle['info']
        account = bundle['account_address']
        user_state = info.user_state(account)
        open_orders = [order for order in info.open_orders(account) if order.get('coin')]
        margin_summary = user_state.get('marginSummary') or {}
        available_balance = read_float(user_state.get('withdrawable'), 0.0)
        equity = read_float(margin_summary.get('accountValue'), available_balance)
        save_balances(REAL_NS, {'available': available_balance, 'equity': equity})
        stats['real_open_orders'] = open_orders
        stats['real_balance'] = {'available': available_balance, 'equity': equity}
        stats['last_real_sync_error'] = ''
        stats['unsupported_external_position'] = ''
        synced_positions = {}
        existing_positions = get_positions(REAL_NS)
        for wrapper in user_state.get('assetPositions', []):
            pos = wrapper.get('position') or {}
            coin = pos.get('coin')
            if not coin:
                continue
            size = read_float(pos.get('szi'), 0.0)
            if abs(size) <= 0:
                continue
            if size < 0:
                stats['unsupported_external_position'] = f'Detected unsupported external SHORT {coin} position.'
                continue
            entry_px = read_float(pos.get('entryPx'), 0.0)
            position_value = read_float(pos.get('positionValue'), size * entry_px)
            existing = existing_positions.get(coin, {})
            active_position = {
                'coin': coin,
                'side': 'LONG',
                'size': abs(size),
                'submitted_price': existing.get('submitted_price', entry_px),
                'filled_price': entry_px,
                'notional': position_value,
                'entry_time': existing.get('entry_time', time.time()),
                'entry_fill_delay_seconds': existing.get('entry_fill_delay_seconds', 0.0),
                'entry_maker_or_taker': existing.get('entry_maker_or_taker', 'maker'),
                'entry_order_type': existing.get('entry_order_type', 'live_sync'),
                'entry_fees_paid': existing.get('entry_fees_paid', 0.0),
                'highest_mid': existing.get('highest_mid', entry_px),
                'lowest_mid': existing.get('lowest_mid', entry_px),
                'max_favourable_excursion': existing.get('max_favourable_excursion', 0.0),
                'max_adverse_excursion': existing.get('max_adverse_excursion', 0.0),
                'source': existing.get('source', 'REAL_SYNC'),
                'take_profit_pct': existing.get('take_profit_pct', get_config(REAL_NS)['take_profit_pct']),
                'stop_loss_pct': existing.get('stop_loss_pct', get_config(REAL_NS)['stop_loss_pct']),
            }
            active_position = update_position_extremes(active_position, active_position['filled_price'])
            synced_positions[coin] = active_position
        save_positions(REAL_NS, synced_positions)
        save_stats(REAL_NS, stats)
        return synced_positions
    except Exception as exc:
        stats['last_real_sync_error'] = str(exc)
        save_stats(REAL_NS, stats)
        set_engine_status(REAL_NS, 'REAL_SYNC_ERROR')
        push_text_log(REAL_NS, f'Real account sync error: {exc}')
        return {}


def get_hourly_trade_count(namespace):
    trades = load_json_log(namespace, 'trade_log', TRADE_LOG_LIMIT)
    cutoff = time.time() - 3600
    count = sum(1 for trade in trades if read_float(trade.get('entry_time')) >= cutoff)
    for position in get_positions(namespace).values():
        if read_float(position.get('entry_time')) >= cutoff:
            count += 1
    return count


def get_effective_open_order_count(namespace):
    local_orders = get_open_orders(namespace)
    if namespace == REAL_NS:
        stats = get_stats(namespace)
        return len(stats.get('real_open_orders', []))
    return len(local_orders)


def build_rejection_reason(reasons):
    if not reasons:
        return 'Entry conditions passed.'
    return ' | '.join(reasons)


def evaluate_entry(namespace, snapshot, metrics):
    config = get_config(namespace)
    stats = get_stats(namespace)
    position = get_position(namespace)
    open_order = get_open_order(namespace)
    reasons = []
    checks = []
    state = get_bot_state(namespace)
    if state != 'RUNNING':
        reasons.append(f'bot_state == {state}')
    if metrics['market_data_age'] > MARKET_STALE_AFTER_SECONDS:
        reasons.append('market data stale')
    if position:
        reasons.append('existing position active')
    if open_order:
        reasons.append('waiting on open order')
    if namespace == REAL_NS and len(get_stats(namespace).get('real_open_orders', [])) > 0 and not open_order:
        reasons.append('real account already has open HYPE order(s)')
    if metrics['return_2m'] > config['return_2m_threshold_pct']:
        reasons.append(f"return_2m {metrics['return_2m']:.4f}% above threshold")
    checks.append({'name': 'return_2m <= threshold', 'passed': metrics['return_2m'] <= config['return_2m_threshold_pct']})
    if metrics['bounce_from_2m_low'] < config['bounce_from_2m_low_threshold_pct']:
        reasons.append(f"bounce_from_2m_low {metrics['bounce_from_2m_low']:.4f}% below threshold")
    checks.append({'name': 'bounce_from_2m_low >= threshold', 'passed': metrics['bounce_from_2m_low'] >= config['bounce_from_2m_low_threshold_pct']})
    if metrics['return_60m'] <= config['return_60m_min_pct']:
        reasons.append(f"return_60m {metrics['return_60m']:.4f}% below threshold")
    checks.append({'name': 'return_60m > minimum', 'passed': metrics['return_60m'] > config['return_60m_min_pct']})
    if metrics['spread_pct'] > config['spread_pct_max']:
        reasons.append(f"spread_pct {metrics['spread_pct']:.4f}% above max")
    checks.append({'name': 'spread_pct <= max', 'passed': metrics['spread_pct'] <= config['spread_pct_max']})
    if metrics['book_imbalance'] < config['book_imbalance_min']:
        reasons.append(f"book_imbalance {metrics['book_imbalance']:.4f} below min")
    checks.append({'name': 'book_imbalance >= min', 'passed': metrics['book_imbalance'] >= config['book_imbalance_min']})
    if config.get('require_imbalance_improvement', True) and metrics['book_imbalance'] <= metrics['book_imbalance_10s_ago']:
        reasons.append('book imbalance not improving vs 10s ago')
    checks.append(
        {
            'name': 'book_imbalance > 10s ago',
            'passed': (not config.get('require_imbalance_improvement', True)) or metrics['book_imbalance'] > metrics['book_imbalance_10s_ago'],
        }
    )
    last_entry_at = read_float(stats.get('last_entry_fill_at'), 0.0)
    if last_entry_at and (time.time() - last_entry_at) < int(config['entry_cooldown_seconds']):
        reasons.append('entry cooldown active')
    if stats.get('daily_pnl', 0.0) <= -abs(float(config['daily_loss_limit_usdc'])):
        reasons.append('daily loss limit hit')
    if int(stats.get('consecutive_losses', 0)) >= int(config['consecutive_loss_limit']):
        reasons.append('consecutive loss limit hit')
    if get_hourly_trade_count(namespace) >= int(config['max_trades_per_hour']):
        reasons.append('max trades per hour reached')
    last_loss_at = read_float(stats.get('last_loss_at'), 0.0)
    if last_loss_at and (time.time() - last_loss_at) < int(config['cooldown_after_loss_seconds']):
        reasons.append('loss cooldown active')
    return len(reasons) == 0, build_rejection_reason(reasons), checks


def apply_fee(notional, fee_pct):
    return float(notional) * (float(fee_pct) / 100.0)


def apply_slippage(price, slippage_pct, side):
    if side == 'buy':
        return float(price) * (1.0 + (float(slippage_pct) / 100.0))
    return float(price) * (1.0 - (float(slippage_pct) / 100.0))


def place_sandbox_entry(snapshot, metrics):
    config = get_config(SANDBOX_NS)
    balances = get_balances(SANDBOX_NS)
    available = float(balances['available'])
    notional = min(float(config['trade_notional_usdc']), float(config['max_notional_usdc']), available)
    if notional < 10.0:
        push_text_log(SANDBOX_NS, 'Sandbox entry blocked: available balance below 10 USDC minimum.')
        return False
    submitted_price = float(snapshot['best_bid'])
    size = notional / submitted_price
    order = {
        'phase': 'entry',
        'side': 'buy',
        'maker_or_taker': 'maker',
        'order_type': 'limit',
        'submitted_price': submitted_price,
        'submitted_at': time.time(),
        'submitted_at_ms': int(time.time() * 1000),
        'timeout_seconds': int(config['entry_timeout_seconds']),
        'size': trim_float(size, 8),
        'notional': trim_float(notional, 6),
        'status': 'OPEN',
    }
    save_open_order(SANDBOX_NS, order)
    set_engine_status(SANDBOX_NS, 'ENTRY_ORDER_WORKING')
    push_text_log(
        SANDBOX_NS,
        f"Sandbox maker entry posted at {submitted_price:.5f} for {size:.6f} HYPE notional {notional:.2f} USDC.",
    )
    return True


def fill_sandbox_entry(order, snapshot):
    config = get_config(SANDBOX_NS)
    balances = get_balances(SANDBOX_NS)
    submitted_price = float(order['submitted_price'])
    filled_price = apply_slippage(submitted_price, config['maker_entry_slippage_pct'], 'buy')
    size = float(order['size'])
    notional = size * filled_price
    fee = apply_fee(notional, config['maker_fee_pct'])
    available = float(balances['available']) - notional - fee
    equity = available + (size * snapshot['mid'])
    save_balances(SANDBOX_NS, {'available': available, 'equity': equity})
    position = {
        'side': 'LONG',
        'size': size,
        'submitted_price': submitted_price,
        'filled_price': filled_price,
        'notional': notional,
        'entry_time': time.time(),
        'entry_fill_delay_seconds': time.time() - float(order['submitted_at']),
        'entry_maker_or_taker': 'maker',
        'entry_order_type': 'post_only_limit',
        'entry_fees_paid': fee,
        'highest_mid': snapshot['mid'],
        'lowest_mid': snapshot['mid'],
        'max_favourable_excursion': 0.0,
        'max_adverse_excursion': 0.0,
        'source': 'SANDBOX',
    }
    save_position(SANDBOX_NS, position)
    save_open_order(SANDBOX_NS, None)
    stats = get_stats(SANDBOX_NS)
    stats['last_entry_fill_at'] = time.time()
    save_stats(SANDBOX_NS, stats)
    set_engine_status(SANDBOX_NS, 'POSITION_OPEN')
    push_text_log(
        SANDBOX_NS,
        f"Sandbox entry filled at {filled_price:.5f} ({position['entry_fill_delay_seconds']:.2f}s delay). Fee {fee:.4f} USDC.",
    )


def cancel_sandbox_entry(reason):
    order = get_open_order(SANDBOX_NS)
    if order and order.get('phase') == 'entry':
        save_open_order(SANDBOX_NS, None)
        stats = get_stats(SANDBOX_NS)
        stats['cancelled_entries'] = int(stats.get('cancelled_entries', 0)) + 1
        save_stats(SANDBOX_NS, stats)
        set_engine_status(SANDBOX_NS, 'ENTRY_CANCELLED')
        push_text_log(SANDBOX_NS, f'Sandbox entry cancelled: {reason}')


def fill_sandbox_exit(position, exit_reason, maker_or_taker, snapshot, submitted_price=None):
    config = get_config(SANDBOX_NS)
    balances = get_balances(SANDBOX_NS)
    side = 'sell'
    if submitted_price is None:
        submitted_price = snapshot['best_bid']
    slippage_pct = config['maker_exit_slippage_pct'] if maker_or_taker == 'maker' else config['taker_exit_slippage_pct']
    fill_price = apply_slippage(submitted_price, slippage_pct, side)
    size = float(position['size'])
    gross_pnl = (fill_price - float(position['filled_price'])) * size
    exit_notional = size * fill_price
    exit_fee = apply_fee(exit_notional, config['maker_fee_pct'] if maker_or_taker == 'maker' else config['taker_fee_pct'])
    entry_fee = float(position.get('entry_fees_paid', 0.0))
    net_pnl = gross_pnl - entry_fee - exit_fee
    available = float(balances['available']) + exit_notional - exit_fee
    equity = available
    save_balances(SANDBOX_NS, {'available': available, 'equity': equity})
    trade = {
        'timestamp': iso_now(),
        'environment': 'SANDBOX',
        'side': 'LONG',
        'order_type': 'post_only_limit' if maker_or_taker == 'maker' else 'market_exit',
        'maker_or_taker': f"maker->{maker_or_taker}",
        'submitted_price': trim_float(position.get('submitted_price', position['filled_price']), 6),
        'filled_price': trim_float(position['filled_price'], 6),
        'fill_delay_seconds': trim_float(position.get('entry_fill_delay_seconds', 0.0), 4),
        'notional': trim_float(position.get('notional', 0.0), 4),
        'size': trim_float(size, 6),
        'entry_time': trim_float(position['entry_time'], 4),
        'entry_time_label': format_timestamp(position['entry_time']),
        'exit_time': trim_float(time.time(), 4),
        'exit_time_label': format_timestamp(time.time()),
        'exit_price': trim_float(fill_price, 6),
        'exit_reason': exit_reason,
        'gross_pnl': trim_float(gross_pnl, 6),
        'fees_paid': trim_float(entry_fee + exit_fee, 6),
        'net_pnl': trim_float(net_pnl, 6),
        'balance_after_trade': trim_float(available, 6),
        'max_favourable_excursion': trim_float(position.get('max_favourable_excursion', 0.0), 6),
        'max_adverse_excursion': trim_float(position.get('max_adverse_excursion', 0.0), 6),
    }
    record_trade(SANDBOX_NS, trade)
    save_position(SANDBOX_NS, None)
    save_open_order(SANDBOX_NS, None)
    stats = get_stats(SANDBOX_NS)
    stats['total_pnl'] = float(stats.get('total_pnl', 0.0)) + net_pnl
    stats['daily_pnl'] = float(stats.get('daily_pnl', 0.0)) + net_pnl
    stats['trades_today'] = int(stats.get('trades_today', 0)) + 1
    stats['last_close_at'] = time.time()
    if net_pnl < 0:
        stats['consecutive_losses'] = int(stats.get('consecutive_losses', 0)) + 1
        stats['last_loss_at'] = time.time()
    else:
        stats['consecutive_losses'] = 0
    if exit_reason == 'TIME_STOP':
        stats['time_stops'] = int(stats.get('time_stops', 0)) + 1
    if exit_reason == 'EMERGENCY_EXIT':
        stats['emergency_exits'] = int(stats.get('emergency_exits', 0)) + 1
    save_stats(SANDBOX_NS, stats)
    set_engine_status(SANDBOX_NS, 'POSITION_CLOSED')
    push_text_log(SANDBOX_NS, f"Sandbox exit {exit_reason} at {fill_price:.5f}. Net PnL {net_pnl:.4f} USDC.")


def submit_real_entry(snapshot):
    bundle = get_real_clients()
    exchange = bundle['exchange']
    size_decimals = bundle['size_decimals']
    config = get_config(REAL_NS)
    balances = get_balances(REAL_NS)
    available = float(balances.get('available', 0.0))
    notional = min(float(config['trade_notional_usdc']), float(config['max_notional_usdc']), available)
    if notional < 10.0:
        push_text_log(REAL_NS, 'REAL entry blocked: available balance below 10 USDC minimum.')
        return False
    submitted_price = float(snapshot['best_bid'])
    size = round_down(notional / submitted_price, size_decimals)
    if size <= 0:
        push_text_log(REAL_NS, 'REAL entry blocked: rounded size is zero for HYPE.')
        return False
    response = exchange.order(MARKET_COIN, True, size, submitted_price, {'limit': {'tif': 'Alo'}})
    status = ((response or {}).get('response') or {}).get('data', {}).get('statuses', [{}])[0]
    if 'error' in status:
        raise RuntimeError(status['error'])
    submitted_at = time.time()
    order = {
        'phase': 'entry',
        'side': 'buy',
        'maker_or_taker': 'maker',
        'order_type': 'limit',
        'submitted_price': submitted_price,
        'submitted_at': submitted_at,
        'submitted_at_ms': int(submitted_at * 1000),
        'timeout_seconds': int(config['entry_timeout_seconds']),
        'size': size,
        'notional': size * submitted_price,
        'status': 'OPEN',
    }
    if 'resting' in status:
        order['exchange_oid'] = status['resting']['oid']
        save_open_order(REAL_NS, order)
        set_engine_status(REAL_NS, 'ENTRY_ORDER_WORKING')
        push_text_log(REAL_NS, f'REAL maker entry posted at {submitted_price:.5f} for {size:.6f} HYPE.')
        return True
    fills = fetch_real_fills_since(order['submitted_at_ms'])
    matched = [fill for fill in fills if fill.get('coin') == MARKET_COIN]
    if matched:
        handle_real_entry_fill(order, matched)
        return True
    save_open_order(REAL_NS, order)
    return True


def fetch_real_fills_since(start_ms):
    bundle = get_real_clients()
    info = bundle['info']
    account = bundle['account_address']
    try:
        return info.user_fills_by_time(account, int(start_ms) - 1000, aggregate_by_time=False)
    except TypeError:
        return info.user_fills_by_time(account, int(start_ms) - 1000)


def weighted_fill_details(fills, oid=None):
    matched = []
    for fill in fills:
        if fill.get('coin') != MARKET_COIN:
            continue
        if oid is not None and fill.get('oid') != oid:
            continue
        matched.append(fill)
    if not matched:
        return 0.0, 0.0, 0.0, 0.0
    total_size = sum(read_float(fill.get('sz'), 0.0) for fill in matched)
    if total_size <= 0:
        return 0.0, 0.0, 0.0, 0.0
    weighted_px = sum(read_float(fill.get('px'), 0.0) * read_float(fill.get('sz'), 0.0) for fill in matched) / total_size
    latest_ts = max(read_float(fill.get('time'), 0.0) for fill in matched) / 1000.0
    crossed = any(bool(fill.get('crossed')) for fill in matched)
    return total_size, weighted_px, latest_ts, 1.0 if crossed else 0.0


def handle_real_entry_fill(order, fills):
    total_size, weighted_px, latest_ts, crossed_flag = weighted_fill_details(fills, order.get('exchange_oid'))
    if total_size <= 0:
        return False
    config = get_config(REAL_NS)
    entry_notional = total_size * weighted_px
    entry_fee = apply_fee(entry_notional, config['maker_fee_pct'])
    position = {
        'side': 'LONG',
        'size': total_size,
        'submitted_price': order['submitted_price'],
        'filled_price': weighted_px,
        'notional': entry_notional,
        'entry_time': latest_ts or time.time(),
        'entry_fill_delay_seconds': max(0.0, (latest_ts or time.time()) - float(order['submitted_at'])),
        'entry_maker_or_taker': 'taker' if crossed_flag else 'maker',
        'entry_order_type': 'post_only_limit',
        'entry_fees_paid': entry_fee,
        'highest_mid': weighted_px,
        'lowest_mid': weighted_px,
        'max_favourable_excursion': 0.0,
        'max_adverse_excursion': 0.0,
        'source': 'REAL',
    }
    save_position(REAL_NS, position)
    save_open_order(REAL_NS, None)
    stats = get_stats(REAL_NS)
    stats['last_entry_fill_at'] = time.time()
    save_stats(REAL_NS, stats)
    set_engine_status(REAL_NS, 'POSITION_OPEN')
    fill_slippage = pct_change(order['submitted_price'], weighted_px)
    push_text_log(
        REAL_NS,
        f'REAL entry filled at {weighted_px:.5f}. Delay {position["entry_fill_delay_seconds"]:.2f}s. Actual fill slippage {fill_slippage:.4f}%.',
    )
    return True


def cancel_real_order(order, reason):
    if not order or not order.get('exchange_oid'):
        return
    bundle = get_real_clients()
    exchange = bundle['exchange']
    exchange.cancel(MARKET_COIN, int(order['exchange_oid']))
    push_text_log(REAL_NS, f"REAL order {order['exchange_oid']} cancelled: {reason}")
    if order.get('phase') == 'entry':
        stats = get_stats(REAL_NS)
        stats['cancelled_entries'] = int(stats.get('cancelled_entries', 0)) + 1
        save_stats(REAL_NS, stats)
    save_open_order(REAL_NS, None)


def handle_real_exit_fill(position, fills, exit_reason, intended_order_type, maker_or_taker, submitted_price):
    total_size, weighted_px, latest_ts, crossed_flag = weighted_fill_details(fills)
    if total_size <= 0:
        return False
    config = get_config(REAL_NS)
    size = min(float(position['size']), total_size)
    gross_pnl = (weighted_px - float(position['filled_price'])) * size
    exit_notional = size * weighted_px
    fee_pct = config['taker_fee_pct'] if maker_or_taker == 'taker' or crossed_flag else config['maker_fee_pct']
    exit_fee = apply_fee(exit_notional, fee_pct)
    entry_fee = float(position.get('entry_fees_paid', 0.0))
    net_pnl = gross_pnl - entry_fee - exit_fee
    balances = get_balances(REAL_NS)
    estimated_available = float(balances.get('available', 0.0)) + exit_notional - exit_fee
    trade = {
        'timestamp': iso_now(),
        'environment': 'REAL',
        'side': 'LONG',
        'order_type': intended_order_type,
        'maker_or_taker': f"maker->{maker_or_taker}",
        'submitted_price': trim_float(submitted_price, 6),
        'filled_price': trim_float(position['filled_price'], 6),
        'fill_delay_seconds': trim_float(position.get('entry_fill_delay_seconds', 0.0), 4),
        'notional': trim_float(position.get('notional', 0.0), 4),
        'size': trim_float(size, 6),
        'entry_time': trim_float(position['entry_time'], 4),
        'entry_time_label': format_timestamp(position['entry_time']),
        'exit_time': trim_float(latest_ts or time.time(), 4),
        'exit_time_label': format_timestamp(latest_ts or time.time()),
        'exit_price': trim_float(weighted_px, 6),
        'exit_reason': exit_reason,
        'gross_pnl': trim_float(gross_pnl, 6),
        'fees_paid': trim_float(entry_fee + exit_fee, 6),
        'net_pnl': trim_float(net_pnl, 6),
        'balance_after_trade': trim_float(estimated_available, 6),
        'max_favourable_excursion': trim_float(position.get('max_favourable_excursion', 0.0), 6),
        'max_adverse_excursion': trim_float(position.get('max_adverse_excursion', 0.0), 6),
    }
    record_trade(REAL_NS, trade)
    stats = get_stats(REAL_NS)
    stats['total_pnl'] = float(stats.get('total_pnl', 0.0)) + net_pnl
    stats['daily_pnl'] = float(stats.get('daily_pnl', 0.0)) + net_pnl
    stats['trades_today'] = int(stats.get('trades_today', 0)) + 1
    stats['last_close_at'] = time.time()
    if net_pnl < 0:
        stats['consecutive_losses'] = int(stats.get('consecutive_losses', 0)) + 1
        stats['last_loss_at'] = time.time()
    else:
        stats['consecutive_losses'] = 0
    if exit_reason == 'TIME_STOP':
        stats['time_stops'] = int(stats.get('time_stops', 0)) + 1
    if exit_reason == 'EMERGENCY_EXIT':
        stats['emergency_exits'] = int(stats.get('emergency_exits', 0)) + 1
    save_stats(REAL_NS, stats)
    save_position(REAL_NS, None)
    save_open_order(REAL_NS, None)
    sync_real_account_state()
    fill_slippage = pct_change(submitted_price, weighted_px) if maker_or_taker == 'maker' else pct_change(submitted_price, weighted_px)
    push_text_log(REAL_NS, f'REAL exit {exit_reason} filled at {weighted_px:.5f}. Actual fill slippage {fill_slippage:.4f}%. Net PnL {net_pnl:.4f} USDC.')
    return True


def submit_real_maker_exit(position, snapshot, exit_reason):
    bundle = get_real_clients()
    exchange = bundle['exchange']
    submitted_price = float(snapshot['best_ask'])
    response = exchange.order(MARKET_COIN, False, float(position['size']), submitted_price, {'limit': {'tif': 'Alo'}}, reduce_only=True)
    status = ((response or {}).get('response') or {}).get('data', {}).get('statuses', [{}])[0]
    if 'error' in status:
        raise RuntimeError(status['error'])
    order = {
        'phase': 'exit',
        'side': 'sell',
        'maker_or_taker': 'maker',
        'order_type': 'limit',
        'submitted_price': submitted_price,
        'submitted_at': time.time(),
        'submitted_at_ms': int(time.time() * 1000),
        'timeout_seconds': int(get_config(REAL_NS)['entry_timeout_seconds']),
        'size': float(position['size']),
        'notional': float(position['size']) * submitted_price,
        'status': 'OPEN',
        'exit_reason': exit_reason,
    }
    if 'resting' in status:
        order['exchange_oid'] = status['resting']['oid']
        save_open_order(REAL_NS, order)
        set_engine_status(REAL_NS, f'{exit_reason}_ORDER_WORKING')
        push_text_log(REAL_NS, f'REAL maker exit posted for {exit_reason} at {submitted_price:.5f}.')
        return True
    fills = fetch_real_fills_since(order['submitted_at_ms'])
    handle_real_exit_fill(position, fills, exit_reason, 'post_only_limit', 'maker', submitted_price)
    return True


def execute_real_taker_exit(exit_reason):
    position = get_position(REAL_NS)
    if not position:
        return False
    bundle = get_real_clients()
    exchange = bundle['exchange']
    snapshot, _, _, _ = market_data.get_snapshot()
    submitted_price = snapshot.get('best_bid', position['filled_price'])
    started_ms = int(time.time() * 1000)
    exchange.market_close(MARKET_COIN, sz=float(position['size']), px=submitted_price, slippage=REAL_MARKET_ORDER_TOLERANCE)
    time.sleep(0.6)
    fills = fetch_real_fills_since(started_ms)
    if not handle_real_exit_fill(position, fills, exit_reason, 'market_exit', 'taker', submitted_price):
        push_text_log(REAL_NS, f'REAL taker exit requested for {exit_reason}, but no fill was detected yet.')
    return True


def manage_open_order(namespace, snapshot):
    order = get_open_order(namespace)
    if not order:
        return
    bot_state = get_bot_state(namespace)
    age = time.time() - float(order['submitted_at'])
    timeout_seconds = float(order.get('timeout_seconds', 10))
    if namespace == SANDBOX_NS:
        if order['phase'] == 'entry' and bot_state != 'RUNNING':
            cancel_sandbox_entry('bot no longer RUNNING')
            return
        if order['phase'] == 'entry' and snapshot['mid'] <= float(order['submitted_price']):
            fill_sandbox_entry(order, snapshot)
            return
        if order['phase'] == 'exit' and snapshot['mid'] >= float(order['submitted_price']):
            position = get_position(SANDBOX_NS)
            if position:
                fill_sandbox_exit(position, order['exit_reason'], 'maker', snapshot, submitted_price=float(order['submitted_price']))
            return
        if age >= timeout_seconds:
            if order['phase'] == 'entry':
                cancel_sandbox_entry('not filled within 10 seconds')
            else:
                save_open_order(SANDBOX_NS, None)
                push_text_log(SANDBOX_NS, f"Sandbox maker exit {order['exit_reason']} expired after {timeout_seconds:.0f}s.")
        return

    sync_real_account_state()
    stats = get_stats(REAL_NS)
    working_ids = {int(item['oid']) for item in stats.get('real_open_orders', []) if item.get('coin') == MARKET_COIN}
    oid = int(order.get('exchange_oid', 0)) if order.get('exchange_oid') else 0
    if oid and oid in working_ids and age < timeout_seconds:
        return
    if oid and oid in working_ids and age >= timeout_seconds:
        cancel_real_order(order, 'timeout exceeded')
        return
    fills = fetch_real_fills_since(order['submitted_at_ms'])
    if order['phase'] == 'entry':
        if handle_real_entry_fill(order, fills):
            return
        save_open_order(REAL_NS, None)
        push_text_log(REAL_NS, 'REAL entry order disappeared without detected fill; treating as cancelled.')
        return
    position = get_position(REAL_NS)
    if position and handle_real_exit_fill(position, fills, order.get('exit_reason', 'UNKNOWN'), 'post_only_limit', 'maker', float(order['submitted_price'])):
        return
    save_open_order(REAL_NS, None)
    push_text_log(REAL_NS, f"REAL maker exit {order.get('exit_reason', 'UNKNOWN')} no longer working and no fill was detected.")


def maybe_place_exit_order(namespace, exit_reason, maker_or_taker, snapshot):
    position = get_position(namespace)
    if not position:
        return False
    open_order = get_open_order(namespace)
    if open_order and open_order.get('phase') == 'exit':
        return False
    if namespace == SANDBOX_NS:
        if maker_or_taker == 'taker':
            fill_sandbox_exit(position, exit_reason, 'taker', snapshot, submitted_price=float(snapshot['best_bid']))
            return True
        order = {
            'phase': 'exit',
            'side': 'sell',
            'maker_or_taker': 'maker',
            'order_type': 'limit',
            'submitted_price': float(snapshot['best_ask']),
            'submitted_at': time.time(),
            'submitted_at_ms': int(time.time() * 1000),
            'timeout_seconds': int(get_config(SANDBOX_NS)['entry_timeout_seconds']),
            'size': float(position['size']),
            'notional': float(position['size']) * float(snapshot['best_ask']),
            'status': 'OPEN',
            'exit_reason': exit_reason,
        }
        save_open_order(SANDBOX_NS, order)
        set_engine_status(SANDBOX_NS, f'{exit_reason}_ORDER_WORKING')
        push_text_log(SANDBOX_NS, f"Sandbox maker exit posted for {exit_reason} at {order['submitted_price']:.5f}.")
        return True

    if maker_or_taker == 'taker':
        return execute_real_taker_exit(exit_reason)
    return submit_real_maker_exit(position, snapshot, exit_reason)


def manage_position(namespace, snapshot):
    position = get_position(namespace)
    if not position:
        return
    position = update_position_extremes(position, snapshot['mid'])
    save_position(namespace, position)
    bot_state = get_bot_state(namespace)
    if bot_state == 'KILLED':
        set_engine_status(namespace, 'KILLED_HOLDING_POSITION')
        return
    config = get_config(namespace)
    entry_price = float(position['filled_price'])
    change_pct = pct_change(entry_price, snapshot['mid'])
    seconds_open = time.time() - float(position['entry_time'])
    if change_pct <= -abs(float(config['emergency_exit_drop_pct'])) and seconds_open <= int(config['emergency_window_seconds']):
        maybe_place_exit_order(namespace, 'EMERGENCY_EXIT', 'taker', snapshot)
        return
    if change_pct <= -abs(float(config['stop_loss_pct'])):
        maybe_place_exit_order(namespace, 'STOP_LOSS', 'taker', snapshot)
        return
    if change_pct >= float(config['take_profit_pct']):
        maybe_place_exit_order(namespace, 'TAKE_PROFIT', 'maker', snapshot)
        return
    if seconds_open >= int(config['time_stop_seconds']):
        maybe_place_exit_order(namespace, 'TIME_STOP', 'maker', snapshot)
        return
    set_engine_status(namespace, 'MANAGING_POSITION')


def trade_loop(namespace):
    while True:
        try:
            lock_key = ns_key(namespace, 'loop_lock')
            acquired = redis.set(lock_key, INSTANCE_ID, nx=True, ex=15)
            if not acquired and redis.get(lock_key) != INSTANCE_ID:
                time.sleep(TRADING_LOOP_INTERVAL)
                continue
            redis.set(lock_key, INSTANCE_ID, ex=15)
            if namespace == REAL_NS:
                sync_real_account_state()
            snapshot, history, market_status, market_error = market_data.get_snapshot()
            if not snapshot:
                set_engine_status(namespace, f'WAITING_FOR_MARKET_DATA ({market_status})')
                time.sleep(TRADING_LOOP_INTERVAL)
                continue
            metrics = compute_market_metrics(snapshot, history)
            passed, reason, checks = evaluate_entry(namespace, snapshot, metrics)
            record_signal(namespace, snapshot, metrics, passed, reason, checks)
            if metrics['market_data_age'] > MARKET_STALE_AFTER_SECONDS:
                set_engine_status(namespace, f'MARKET_STALE: {metrics["market_data_age"]:.1f}s')
                time.sleep(TRADING_LOOP_INTERVAL)
                continue
            manage_open_order(namespace, snapshot)
            manage_position(namespace, snapshot)
            position = get_position(namespace)
            open_order = get_open_order(namespace)
            if position or open_order:
                time.sleep(TRADING_LOOP_INTERVAL)
                continue
            if get_bot_state(namespace) != 'RUNNING':
                set_engine_status(namespace, 'PAUSED_WAITING_FOR_START' if get_bot_state(namespace) == 'PAUSED' else 'KILLED')
                time.sleep(TRADING_LOOP_INTERVAL)
                continue
            if not passed:
                set_engine_status(namespace, 'RUNNING_WAITING_FOR_SIGNAL')
                time.sleep(TRADING_LOOP_INTERVAL)
                continue
            if namespace == SANDBOX_NS:
                place_sandbox_entry(snapshot, metrics)
            else:
                submit_real_entry(snapshot)
        except Exception as exc:
            set_engine_status(namespace, f'LOOP_ERROR: {exc}')
            push_text_log(namespace, f'Loop error: {exc}')
        time.sleep(TRADING_LOOP_INTERVAL)


def build_health_payload(namespace):
    snapshot, _, market_status, market_error = market_data.get_snapshot()
    ws_diag = market_data.get_diagnostics()
    payload = {
        'database': 'OK',
        'hyperliquid_sdk': 'OK' if not HYPERLIQUID_IMPORT_ERROR else 'ERROR',
        'hyperliquid_api': 'N/A',
        'market_data': market_status,
        'market_data_source': snapshot.get('source', 'none') if snapshot else 'none',
        'market_loop_alive': ws_diag['loop_thread_alive'],
        'ws_thread_alive': ws_diag['ws_thread_alive'],
        'ws_reconnect_count': ws_diag['reconnect_count'],
        'ws_last_open_at': ws_diag['last_open_at'],
        'ws_last_message_at': ws_diag['last_message_at'],
        'ws_last_close_at': ws_diag['last_close_at'],
        'ws_last_close_code': ws_diag['last_close_code'],
        'ws_last_close_reason': ws_diag['last_close_reason'],
        'ws_last_error': ws_diag['last_ws_error'],
        'market_loop_last_heartbeat_at': ws_diag['last_loop_heartbeat_at'],
        'market_loop_last_error': ws_diag['last_loop_error'],
        'engine': get_engine_status(namespace),
        'errors': [],
    }
    try:
        redis.ping()
    except Exception as exc:
        payload['database'] = 'ERROR'
        payload['errors'].append(str(exc))
    if HYPERLIQUID_IMPORT_ERROR:
        payload['errors'].append(HYPERLIQUID_IMPORT_ERROR)
    if market_error:
        payload['errors'].append(market_error)
    if namespace == REAL_NS:
        if not HYPERLIQUID_API_KEY or not HYPERLIQUID_API_SECRET:
            payload['hyperliquid_api'] = 'MISSING_KEYS'
        else:
            stats = get_stats(REAL_NS)
            payload['hyperliquid_api'] = 'READY' if not stats.get('last_real_sync_error') else 'ERROR'
            if stats.get('last_real_sync_error'):
                payload['errors'].append(stats['last_real_sync_error'])
    return payload


def build_state_payload(namespace):
    snapshot, history, market_status, market_error = market_data.get_snapshot()
    ws_diag = market_data.get_diagnostics()
    hot_perps = market_universe.get_hot_perps(limit=10)
    metrics = compute_market_metrics(snapshot, history) if snapshot else {
        'mid': 0.0,
        'best_bid': 0.0,
        'best_ask': 0.0,
        'spread_pct': 0.0,
        'book_imbalance': 0.0,
        'book_imbalance_10s_ago': 0.0,
        'return_30s': 0.0,
        'return_1m': 0.0,
        'return_2m': 0.0,
        'return_5m': 0.0,
        'return_15m': 0.0,
        'return_60m': 0.0,
        'bounce_from_2m_low': 0.0,
        'mid_2m_low': 0.0,
        'market_data_age': 0.0,
    }
    balances = get_balances(namespace)
    position = get_position(namespace)
    open_order = get_open_order(namespace)
    stats = get_stats(namespace)
    if namespace == SANDBOX_NS and position:
        balances['equity'] = float(balances['available']) + (float(position['size']) * metrics['mid'])
        save_balances(namespace, balances)
    position_view = compute_position_view(position, metrics['mid'])
    config = get_config(namespace)
    last_trade = get_last_trade(namespace)
    last_signal = get_last_signal(namespace)
    real_warning = namespace == REAL_NS
    payload = {
        'environment': namespace.upper(),
        'bot_state': get_bot_state(namespace),
        'engine_status': get_engine_status(namespace),
        'real_warning': real_warning,
        'requires_confirmation': namespace == REAL_NS,
        'market': {
            'coin': MARKET_COIN,
            'mid': trim_float(metrics['mid'], 6),
            'best_bid': trim_float(metrics['best_bid'], 6),
            'best_ask': trim_float(metrics['best_ask'], 6),
            'spread_pct': trim_float(metrics['spread_pct'], 6),
            'return_2m': trim_float(metrics['return_2m'], 6),
            'return_5m': trim_float(metrics['return_5m'], 6),
            'return_15m': trim_float(metrics['return_15m'], 6),
            'return_60m': trim_float(metrics['return_60m'], 6),
            'bounce_from_2m_low': trim_float(metrics['bounce_from_2m_low'], 6),
            'book_imbalance': trim_float(metrics['book_imbalance'], 6),
            'book_imbalance_10s_ago': trim_float(metrics['book_imbalance_10s_ago'], 6),
            'market_data_status': market_status,
            'market_data_source': snapshot.get('source', 'none') if snapshot else 'none',
            'market_data_error': market_error,
            'market_data_age': trim_float(metrics['market_data_age'], 2),
        },
        'market_diagnostics': {
            'status': ws_diag['status'],
            'error': ws_diag['error'],
            'reconnect_count': ws_diag['reconnect_count'],
            'ws_thread_alive': ws_diag['ws_thread_alive'],
            'loop_thread_alive': ws_diag['loop_thread_alive'],
            'last_open_at': ws_diag['last_open_at'],
            'last_open_at_label': format_timestamp(ws_diag['last_open_at']),
            'last_message_at': ws_diag['last_message_at'],
            'last_message_at_label': format_timestamp(ws_diag['last_message_at']),
            'last_close_at': ws_diag['last_close_at'],
            'last_close_at_label': format_timestamp(ws_diag['last_close_at']),
            'last_close_code': ws_diag['last_close_code'],
            'last_close_reason': ws_diag['last_close_reason'],
            'last_ws_error': ws_diag['last_ws_error'],
            'last_loop_heartbeat_at': ws_diag['last_loop_heartbeat_at'],
            'last_loop_heartbeat_at_label': format_timestamp(ws_diag['last_loop_heartbeat_at']),
            'last_loop_error': ws_diag['last_loop_error'],
        },
        'balances': {
            'available': trim_float(balances.get('available', 0.0), 6),
            'equity': trim_float(balances.get('equity', 0.0), 6),
        },
        'active_position': position_view,
        'open_orders': {
            'count': get_effective_open_order_count(namespace),
            'current': open_order or {},
        },
        'config': config,
        'stats': {
            'daily_pnl': trim_float(stats.get('daily_pnl', 0.0), 6),
            'total_pnl': trim_float(stats.get('total_pnl', 0.0), 6),
            'consecutive_losses': int(stats.get('consecutive_losses', 0)),
            'trades_today': int(stats.get('trades_today', 0)),
            'trades_last_hour': get_hourly_trade_count(namespace),
            'cancelled_entries': int(stats.get('cancelled_entries', 0)),
            'time_stops': int(stats.get('time_stops', 0)),
            'emergency_exits': int(stats.get('emergency_exits', 0)),
            'last_loss_at': stats.get('last_loss_at', 0.0),
            'last_loss_at_label': format_timestamp(stats.get('last_loss_at', 0.0)),
            'unsupported_external_position': stats.get('unsupported_external_position', ''),
        },
        'last_signal': last_signal,
        'last_trade': last_trade,
        'hot_perps': hot_perps['leaders'],
        'hot_perps_error': hot_perps['last_error'],
        'reason_last_trade_not_taken': get_reason_not_taken(namespace),
        'logs': load_action_logs(namespace),
    }
    if position:
        payload['position_targets'] = {
            'take_profit_price': trim_float(float(position['filled_price']) * (1.0 + (float(config['take_profit_pct']) / 100.0)), 6),
            'stop_loss_price': trim_float(float(position['filled_price']) * (1.0 - (float(config['stop_loss_pct']) / 100.0)), 6),
            'time_stop_seconds': int(config['time_stop_seconds']),
        }
    else:
        payload['position_targets'] = {}
    if namespace == REAL_NS:
        payload['real_account'] = {
            'api_key_present': bool(HYPERLIQUID_API_KEY),
            'api_secret_present': bool(HYPERLIQUID_API_SECRET),
            'open_orders': get_stats(REAL_NS).get('real_open_orders', []),
        }
    return payload


def compute_results_summary(namespace):
    trades = list(reversed(load_json_log(namespace, 'trade_log', RESULTS_TRADE_LIMIT)))
    stats = get_stats(namespace)
    total_trades = len(trades)
    wins = [trade for trade in trades if read_float(trade.get('net_pnl')) > 0]
    losses = [trade for trade in trades if read_float(trade.get('net_pnl')) < 0]
    gross_wins = sum(read_float(trade.get('net_pnl')) for trade in wins)
    gross_losses = abs(sum(read_float(trade.get('net_pnl')) for trade in losses))
    hold_times = [max(0.0, read_float(trade.get('exit_time')) - read_float(trade.get('entry_time'))) for trade in trades]
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for trade in trades:
        cumulative += read_float(trade.get('net_pnl'))
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)
    summary = {
        'total_trades': total_trades,
        'win_rate': (len(wins) / total_trades * 100.0) if total_trades else 0.0,
        'average_win': (gross_wins / len(wins)) if wins else 0.0,
        'average_loss': (sum(read_float(trade.get('net_pnl')) for trade in losses) / len(losses)) if losses else 0.0,
        'profit_factor': (gross_wins / gross_losses) if gross_losses > 0 else (float('inf') if gross_wins > 0 else 0.0),
        'net_pnl': sum(read_float(trade.get('net_pnl')) for trade in trades),
        'max_drawdown': max_drawdown,
        'average_hold_time_seconds': (sum(hold_times) / len(hold_times)) if hold_times else 0.0,
        'best_trade': max((read_float(trade.get('net_pnl')) for trade in trades), default=0.0),
        'worst_trade': min((read_float(trade.get('net_pnl')) for trade in trades), default=0.0),
        'pnl_after_fees': sum(read_float(trade.get('net_pnl')) for trade in trades),
        'cancelled_entries': int(stats.get('cancelled_entries', 0)),
        'time_stops': int(stats.get('time_stops', 0)),
        'emergency_exits': int(stats.get('emergency_exits', 0)),
    }
    return {'summary': summary, 'trades': trades}



def get_size_decimals(coin):
    market_universe.ensure_running()
    with market_universe.lock:
        decimals = market_universe.sz_decimals.get(coin)
    if decimals is not None:
        return int(decimals)
    market_universe.refresh_meta(force=True)
    with market_universe.lock:
        return int(market_universe.sz_decimals.get(coin, 0))


def fetch_coin_book_snapshot(coin):
    book = market_data.ensure_rest_client().l2_snapshot(coin)
    snapshot = market_data._build_snapshot(book, source='rest_book')
    snapshot['coin'] = coin
    return snapshot


def build_coin_snapshot_and_metrics(coin, require_book=True):
    market_metrics = market_universe.get_metrics_for_coin(coin)
    if not market_metrics:
        return None, None
    if require_book:
        snapshot = fetch_coin_book_snapshot(coin)
    else:
        snapshot = {
            'coin': coin,
            'ts': time.time(),
            'source': 'allMids',
            'mid': market_metrics['mid'],
            'best_bid': market_metrics['mid'],
            'best_ask': market_metrics['mid'],
            'spread_pct': 0.0,
            'book_imbalance': 0.5,
        }
    metrics = compute_market_metrics(snapshot, market_metrics['history'])
    metrics['market_data_age'] = market_metrics['market_data_age']
    return snapshot, metrics


def build_position_targets(position, config):
    entry = float(position['filled_price'])
    return {
        'take_profit_price': trim_float(entry * (1.0 + (float(config['take_profit_pct']) / 100.0)), 6),
        'stop_loss_price': trim_float(entry * (1.0 - (float(config['stop_loss_pct']) / 100.0)), 6),
        'time_stop_seconds': int(config['time_stop_seconds']),
    }


def compute_progress_pct(position_view, config):
    pnl_pct = float(position_view.get('gross_unrealized_pnl_pct', 0.0))
    take_profit_pct = max(float(config.get('take_profit_pct', 0.0)), 0.0001)
    stop_loss_pct = max(abs(float(config.get('stop_loss_pct', 0.0))), 0.0001)
    if pnl_pct >= 0:
        return min(100.0, max(0.0, (pnl_pct / take_profit_pct) * 100.0))
    return max(-100.0, min(0.0, -((abs(pnl_pct) / stop_loss_pct) * 100.0)))


def count_entry_orders(namespace):
    return sum(1 for order in get_open_orders(namespace).values() if order.get('phase') == 'entry')


def evaluate_entry_candidate(namespace, coin, snapshot, metrics):
    config = get_config(namespace)
    stats = get_stats(namespace)
    positions = get_positions(namespace)
    open_orders = get_open_orders(namespace)
    reasons = []
    checks = []
    bot_state = get_bot_state(namespace)
    if bot_state != 'RUNNING':
        reasons.append(f'bot_state == {bot_state}')
    if metrics['market_data_age'] > MARKET_STALE_AFTER_SECONDS:
        reasons.append('market data stale')
    if coin in positions:
        reasons.append('position already active for coin')
    if coin in open_orders:
        reasons.append('open order already working for coin')
    if (len(positions) + count_entry_orders(namespace)) >= int(config.get('max_open_positions', 1)):
        reasons.append('max open positions reached')
    if namespace == SANDBOX_NS:
        thirty_second_threshold = float(config.get('return_30s_threshold_pct', 0.25))
        if metrics['return_30s'] <= thirty_second_threshold:
            reasons.append(f"return_30s {metrics['return_30s']:.4f}% below trigger")
        checks.append({'name': f'return_30s > {thirty_second_threshold:.2f}%', 'passed': metrics['return_30s'] > thirty_second_threshold})
    else:
        if metrics['return_5m'] <= 0.25:
            reasons.append(f"return_5m {metrics['return_5m']:.4f}% below momentum floor")
        checks.append({'name': 'return_5m > 0.25%', 'passed': metrics['return_5m'] > 0.25})
        if metrics['return_15m'] <= 0.40:
            reasons.append(f"return_15m {metrics['return_15m']:.4f}% below momentum floor")
        checks.append({'name': 'return_15m > 0.40%', 'passed': metrics['return_15m'] > 0.40})
        if metrics['return_60m'] <= float(config['return_60m_min_pct']):
            reasons.append(f"return_60m {metrics['return_60m']:.4f}% below minimum")
        checks.append({'name': 'return_60m above minimum', 'passed': metrics['return_60m'] > float(config['return_60m_min_pct'])})
    if namespace == SANDBOX_NS:
        checks.append({'name': 'spread check relaxed in sandbox', 'passed': True})
        checks.append({'name': 'book imbalance check relaxed in sandbox', 'passed': True})
    else:
        if metrics['spread_pct'] > float(config['spread_pct_max']):
            reasons.append(f"spread_pct {metrics['spread_pct']:.4f}% above max")
        checks.append({'name': 'spread_pct <= max', 'passed': metrics['spread_pct'] <= float(config['spread_pct_max'])})
        if metrics['book_imbalance'] < max(0.5, float(config['book_imbalance_min']) - 0.02):
            reasons.append(f"book_imbalance {metrics['book_imbalance']:.4f} below min")
        checks.append({'name': 'book imbalance healthy', 'passed': metrics['book_imbalance'] >= max(0.5, float(config['book_imbalance_min']) - 0.02)})
    last_entry_at = read_float(stats.get('last_entry_fill_at'), 0.0)
    if int(config.get('entry_cooldown_seconds', 0)) > 0 and last_entry_at and (time.time() - last_entry_at) < int(config['entry_cooldown_seconds']):
        reasons.append('entry cooldown active')
    if namespace != SANDBOX_NS and stats.get('daily_pnl', 0.0) <= -abs(float(config['daily_loss_limit_usdc'])):
        reasons.append('daily loss limit hit')
    if namespace != SANDBOX_NS and int(stats.get('consecutive_losses', 0)) >= int(config['consecutive_loss_limit']):
        reasons.append('consecutive loss limit hit')
    if int(config.get('max_trades_per_hour', 0)) > 0 and get_hourly_trade_count(namespace) >= int(config['max_trades_per_hour']):
        reasons.append('max trades per hour reached')
    last_loss_at = read_float(stats.get('last_loss_at'), 0.0)
    if namespace != SANDBOX_NS and int(config.get('cooldown_after_loss_seconds', 0)) > 0 and last_loss_at and (time.time() - last_loss_at) < int(config['cooldown_after_loss_seconds']):
        reasons.append('loss cooldown active')
    return len(reasons) == 0, build_rejection_reason(reasons), checks


def place_sandbox_entry_multi(coin, snapshot):
    config = get_config(SANDBOX_NS)
    balances = get_balances(SANDBOX_NS)
    available = float(balances['available'])
    notional = min(float(config['trade_notional_usdc']), float(config['max_notional_usdc']), available)
    if notional < 10.0:
        push_text_log(SANDBOX_NS, f'Sandbox entry blocked for {coin}: available balance below 10 USDC minimum.')
        return False
    submitted_price = float(snapshot['best_ask'])
    size = notional / submitted_price
    order = {
        'coin': coin,
        'phase': 'entry',
        'side': 'buy',
        'maker_or_taker': 'taker',
        'order_type': 'marketable_limit',
        'submitted_price': submitted_price,
        'submitted_at': time.time(),
        'submitted_at_ms': int(time.time() * 1000),
        'timeout_seconds': int(config['entry_timeout_seconds']),
        'size': trim_float(size, 8),
        'notional': trim_float(notional, 6),
        'status': 'OPEN',
    }
    save_open_order_for_coin(SANDBOX_NS, coin, order)
    set_engine_status(SANDBOX_NS, f'ENTRY_ORDER_WORKING:{coin}')
    push_text_log(SANDBOX_NS, f"Sandbox taker-style entry posted for {coin} at {submitted_price:.5f} for {size:.6f}.")
    return True


def fill_sandbox_entry_multi(order, current_mid):
    coin = order['coin']
    config = get_config(SANDBOX_NS)
    balances = get_balances(SANDBOX_NS)
    submitted_price = float(order['submitted_price'])
    maker_or_taker = order.get('maker_or_taker', 'maker')
    entry_slippage_pct = config['taker_exit_slippage_pct'] if maker_or_taker == 'taker' else config['maker_entry_slippage_pct']
    filled_price = apply_slippage(submitted_price, entry_slippage_pct, 'buy')
    size = float(order['size'])
    notional = size * filled_price
    fee = apply_fee(notional, config['taker_fee_pct'] if maker_or_taker == 'taker' else config['maker_fee_pct'])
    available = float(balances['available']) - notional - fee
    save_balances(SANDBOX_NS, {'available': available, 'equity': float(balances.get('equity', available))})
    position = {
        'coin': coin,
        'side': 'LONG',
        'size': size,
        'submitted_price': submitted_price,
        'filled_price': filled_price,
        'notional': notional,
        'entry_time': time.time(),
        'entry_fill_delay_seconds': time.time() - float(order['submitted_at']),
        'entry_maker_or_taker': maker_or_taker,
        'entry_order_type': order.get('order_type', 'post_only_limit'),
        'entry_fees_paid': fee,
        'highest_mid': current_mid,
        'lowest_mid': current_mid,
        'max_favourable_excursion': 0.0,
        'max_adverse_excursion': 0.0,
        'source': 'SANDBOX',
        'take_profit_pct': float(config['take_profit_pct']),
        'stop_loss_pct': float(config['stop_loss_pct']),
    }
    save_position_for_coin(SANDBOX_NS, coin, position)
    save_open_order_for_coin(SANDBOX_NS, coin, None)
    stats = get_stats(SANDBOX_NS)
    stats['last_entry_fill_at'] = time.time()
    save_stats(SANDBOX_NS, stats)
    set_engine_status(SANDBOX_NS, f'POSITION_OPEN:{coin}')
    push_text_log(SANDBOX_NS, f"Sandbox {maker_or_taker} entry filled for {coin} at {filled_price:.5f}. Fee {fee:.4f} USDC.")


def cancel_sandbox_entry_multi(coin, reason):
    order = get_open_order_for_coin(SANDBOX_NS, coin)
    if order and order.get('phase') == 'entry':
        save_open_order_for_coin(SANDBOX_NS, coin, None)
        stats = get_stats(SANDBOX_NS)
        stats['cancelled_entries'] = int(stats.get('cancelled_entries', 0)) + 1
        save_stats(SANDBOX_NS, stats)
        push_text_log(SANDBOX_NS, f'Sandbox entry for {coin} cancelled: {reason}')


def fill_sandbox_exit_multi(position, exit_reason, maker_or_taker, current_mid, submitted_price=None):
    coin = position['coin']
    config = get_config(SANDBOX_NS)
    balances = get_balances(SANDBOX_NS)
    submitted_price = float(submitted_price or current_mid)
    slippage_pct = config['maker_exit_slippage_pct'] if maker_or_taker == 'maker' else config['taker_exit_slippage_pct']
    fill_price = apply_slippage(submitted_price, slippage_pct, 'sell')
    size = float(position['size'])
    gross_pnl = (fill_price - float(position['filled_price'])) * size
    exit_notional = size * fill_price
    exit_fee = apply_fee(exit_notional, config['maker_fee_pct'] if maker_or_taker == 'maker' else config['taker_fee_pct'])
    entry_fee = float(position.get('entry_fees_paid', 0.0))
    net_pnl = gross_pnl - entry_fee - exit_fee
    available = float(balances['available']) + exit_notional - exit_fee
    save_balances(SANDBOX_NS, {'available': available, 'equity': available})
    trade = {
        'timestamp': iso_now(),
        'environment': 'SANDBOX',
        'coin': coin,
        'side': 'LONG',
        'order_type': 'post_only_limit' if maker_or_taker == 'maker' else 'market_exit',
        'maker_or_taker': f"maker->{maker_or_taker}",
        'submitted_price': trim_float(position.get('submitted_price', position['filled_price']), 6),
        'filled_price': trim_float(position['filled_price'], 6),
        'fill_delay_seconds': trim_float(position.get('entry_fill_delay_seconds', 0.0), 4),
        'notional': trim_float(position.get('notional', 0.0), 4),
        'size': trim_float(size, 6),
        'entry_time': trim_float(position['entry_time'], 4),
        'entry_time_label': format_timestamp(position['entry_time']),
        'exit_time': trim_float(time.time(), 4),
        'exit_time_label': format_timestamp(time.time()),
        'exit_price': trim_float(fill_price, 6),
        'exit_reason': exit_reason,
        'gross_pnl': trim_float(gross_pnl, 6),
        'fees_paid': trim_float(entry_fee + exit_fee, 6),
        'net_pnl': trim_float(net_pnl, 6),
        'balance_after_trade': trim_float(available, 6),
        'max_favourable_excursion': trim_float(position.get('max_favourable_excursion', 0.0), 6),
        'max_adverse_excursion': trim_float(position.get('max_adverse_excursion', 0.0), 6),
    }
    record_trade(SANDBOX_NS, trade)
    save_position_for_coin(SANDBOX_NS, coin, None)
    save_open_order_for_coin(SANDBOX_NS, coin, None)
    stats = get_stats(SANDBOX_NS)
    stats['total_pnl'] = float(stats.get('total_pnl', 0.0)) + net_pnl
    stats['daily_pnl'] = float(stats.get('daily_pnl', 0.0)) + net_pnl
    stats['trades_today'] = int(stats.get('trades_today', 0)) + 1
    stats['last_close_at'] = time.time()
    if net_pnl < 0:
        stats['consecutive_losses'] = int(stats.get('consecutive_losses', 0)) + 1
        stats['last_loss_at'] = time.time()
    else:
        stats['consecutive_losses'] = 0
    if exit_reason == 'TIME_STOP':
        stats['time_stops'] = int(stats.get('time_stops', 0)) + 1
    if exit_reason == 'EMERGENCY_EXIT':
        stats['emergency_exits'] = int(stats.get('emergency_exits', 0)) + 1
    save_stats(SANDBOX_NS, stats)
    set_engine_status(SANDBOX_NS, f'POSITION_CLOSED:{coin}')
    push_text_log(SANDBOX_NS, f"Sandbox exit {exit_reason} for {coin} at {fill_price:.5f}. Net PnL {net_pnl:.4f} USDC.")


def weighted_fill_details_for_coin(fills, coin, oid=None):
    matched = []
    for fill in fills:
        if fill.get('coin') != coin:
            continue
        if oid is not None and fill.get('oid') != oid:
            continue
        matched.append(fill)
    if not matched:
        return 0.0, 0.0, 0.0, 0.0
    total_size = sum(read_float(fill.get('sz'), 0.0) for fill in matched)
    if total_size <= 0:
        return 0.0, 0.0, 0.0, 0.0
    weighted_px = sum(read_float(fill.get('px'), 0.0) * read_float(fill.get('sz'), 0.0) for fill in matched) / total_size
    latest_ts = max(read_float(fill.get('time'), 0.0) for fill in matched) / 1000.0
    crossed = any(bool(fill.get('crossed')) for fill in matched)
    return total_size, weighted_px, latest_ts, 1.0 if crossed else 0.0


def submit_real_entry_multi(coin, snapshot):
    bundle = get_real_clients()
    exchange = bundle['exchange']
    config = get_config(REAL_NS)
    balances = get_balances(REAL_NS)
    available = float(balances.get('available', 0.0))
    notional = min(float(config['trade_notional_usdc']), float(config['max_notional_usdc']), available)
    if notional < 10.0:
        push_text_log(REAL_NS, f'REAL entry blocked for {coin}: available balance below 10 USDC minimum.')
        return False
    submitted_price = float(snapshot['best_bid'])
    size = round_down(notional / submitted_price, get_size_decimals(coin))
    if size <= 0:
        push_text_log(REAL_NS, f'REAL entry blocked for {coin}: rounded size is zero.')
        return False
    response = exchange.order(coin, True, size, submitted_price, {'limit': {'tif': 'Alo'}})
    status = ((response or {}).get('response') or {}).get('data', {}).get('statuses', [{}])[0]
    if 'error' in status:
        raise RuntimeError(status['error'])
    submitted_at = time.time()
    order = {
        'coin': coin,
        'phase': 'entry',
        'side': 'buy',
        'maker_or_taker': 'maker',
        'order_type': 'limit',
        'submitted_price': submitted_price,
        'submitted_at': submitted_at,
        'submitted_at_ms': int(submitted_at * 1000),
        'timeout_seconds': int(config['entry_timeout_seconds']),
        'size': size,
        'notional': size * submitted_price,
        'status': 'OPEN',
    }
    if 'resting' in status:
        order['exchange_oid'] = status['resting']['oid']
        save_open_order_for_coin(REAL_NS, coin, order)
        set_engine_status(REAL_NS, f'ENTRY_ORDER_WORKING:{coin}')
        push_text_log(REAL_NS, f'REAL maker entry posted for {coin} at {submitted_price:.5f} for {size:.6f}.')
        return True
    fills = fetch_real_fills_since(order['submitted_at_ms'])
    matched = [fill for fill in fills if fill.get('coin') == coin]
    if matched:
        handle_real_entry_fill_multi(order, matched)
        return True
    save_open_order_for_coin(REAL_NS, coin, order)
    return True


def handle_real_entry_fill_multi(order, fills):
    coin = order['coin']
    total_size, weighted_px, latest_ts, crossed_flag = weighted_fill_details_for_coin(fills, coin, order.get('exchange_oid'))
    if total_size <= 0:
        return False
    config = get_config(REAL_NS)
    entry_notional = total_size * weighted_px
    fee_pct = config['taker_fee_pct'] if crossed_flag else config['maker_fee_pct']
    entry_fee = apply_fee(entry_notional, fee_pct)
    position = {
        'coin': coin,
        'side': 'LONG',
        'size': total_size,
        'submitted_price': order['submitted_price'],
        'filled_price': weighted_px,
        'notional': entry_notional,
        'entry_time': latest_ts or time.time(),
        'entry_fill_delay_seconds': max(0.0, (latest_ts or time.time()) - float(order['submitted_at'])),
        'entry_maker_or_taker': 'taker' if crossed_flag else 'maker',
        'entry_order_type': 'post_only_limit',
        'entry_fees_paid': entry_fee,
        'highest_mid': weighted_px,
        'lowest_mid': weighted_px,
        'max_favourable_excursion': 0.0,
        'max_adverse_excursion': 0.0,
        'source': 'REAL',
        'take_profit_pct': float(config['take_profit_pct']),
        'stop_loss_pct': float(config['stop_loss_pct']),
    }
    save_position_for_coin(REAL_NS, coin, position)
    save_open_order_for_coin(REAL_NS, coin, None)
    stats = get_stats(REAL_NS)
    stats['last_entry_fill_at'] = time.time()
    save_stats(REAL_NS, stats)
    set_engine_status(REAL_NS, f'POSITION_OPEN:{coin}')
    fill_slippage = pct_change(order['submitted_price'], weighted_px)
    push_text_log(REAL_NS, f'REAL entry filled for {coin} at {weighted_px:.5f}. Slippage {fill_slippage:.4f}%.')
    return True


def cancel_real_order_multi(order, reason):
    if not order or not order.get('exchange_oid'):
        return
    coin = order['coin']
    bundle = get_real_clients()
    exchange = bundle['exchange']
    exchange.cancel(coin, int(order['exchange_oid']))
    push_text_log(REAL_NS, f"REAL order {order['exchange_oid']} for {coin} cancelled: {reason}")
    if order.get('phase') == 'entry':
        stats = get_stats(REAL_NS)
        stats['cancelled_entries'] = int(stats.get('cancelled_entries', 0)) + 1
        save_stats(REAL_NS, stats)
    save_open_order_for_coin(REAL_NS, coin, None)


def handle_real_exit_fill_multi(position, fills, exit_reason, intended_order_type, maker_or_taker, submitted_price):
    coin = position['coin']
    total_size, weighted_px, latest_ts, crossed_flag = weighted_fill_details_for_coin(fills, coin)
    if total_size <= 0:
        return False
    config = get_config(REAL_NS)
    size = min(float(position['size']), total_size)
    gross_pnl = (weighted_px - float(position['filled_price'])) * size
    exit_notional = size * weighted_px
    fee_pct = config['taker_fee_pct'] if maker_or_taker == 'taker' or crossed_flag else config['maker_fee_pct']
    exit_fee = apply_fee(exit_notional, fee_pct)
    entry_fee = float(position.get('entry_fees_paid', 0.0))
    net_pnl = gross_pnl - entry_fee - exit_fee
    balances = get_balances(REAL_NS)
    estimated_available = float(balances.get('available', 0.0)) + exit_notional - exit_fee
    trade = {
        'timestamp': iso_now(),
        'environment': 'REAL',
        'coin': coin,
        'side': 'LONG',
        'order_type': intended_order_type,
        'maker_or_taker': f"maker->{maker_or_taker}",
        'submitted_price': trim_float(submitted_price, 6),
        'filled_price': trim_float(position['filled_price'], 6),
        'fill_delay_seconds': trim_float(position.get('entry_fill_delay_seconds', 0.0), 4),
        'notional': trim_float(position.get('notional', 0.0), 4),
        'size': trim_float(size, 6),
        'entry_time': trim_float(position['entry_time'], 4),
        'entry_time_label': format_timestamp(position['entry_time']),
        'exit_time': trim_float(latest_ts or time.time(), 4),
        'exit_time_label': format_timestamp(latest_ts or time.time()),
        'exit_price': trim_float(weighted_px, 6),
        'exit_reason': exit_reason,
        'gross_pnl': trim_float(gross_pnl, 6),
        'fees_paid': trim_float(entry_fee + exit_fee, 6),
        'net_pnl': trim_float(net_pnl, 6),
        'balance_after_trade': trim_float(estimated_available, 6),
        'max_favourable_excursion': trim_float(position.get('max_favourable_excursion', 0.0), 6),
        'max_adverse_excursion': trim_float(position.get('max_adverse_excursion', 0.0), 6),
    }
    record_trade(REAL_NS, trade)
    stats = get_stats(REAL_NS)
    stats['total_pnl'] = float(stats.get('total_pnl', 0.0)) + net_pnl
    stats['daily_pnl'] = float(stats.get('daily_pnl', 0.0)) + net_pnl
    stats['trades_today'] = int(stats.get('trades_today', 0)) + 1
    stats['last_close_at'] = time.time()
    if net_pnl < 0:
        stats['consecutive_losses'] = int(stats.get('consecutive_losses', 0)) + 1
        stats['last_loss_at'] = time.time()
    else:
        stats['consecutive_losses'] = 0
    if exit_reason == 'TIME_STOP':
        stats['time_stops'] = int(stats.get('time_stops', 0)) + 1
    if exit_reason == 'EMERGENCY_EXIT':
        stats['emergency_exits'] = int(stats.get('emergency_exits', 0)) + 1
    save_stats(REAL_NS, stats)
    save_position_for_coin(REAL_NS, coin, None)
    save_open_order_for_coin(REAL_NS, coin, None)
    sync_real_account_state()
    push_text_log(REAL_NS, f'REAL exit {exit_reason} for {coin} filled at {weighted_px:.5f}. Net PnL {net_pnl:.4f} USDC.')
    return True


def submit_real_maker_exit_multi(position, snapshot, exit_reason):
    coin = position['coin']
    bundle = get_real_clients()
    exchange = bundle['exchange']
    submitted_price = float(snapshot['best_ask'])
    response = exchange.order(coin, False, float(position['size']), submitted_price, {'limit': {'tif': 'Alo'}}, reduce_only=True)
    status = ((response or {}).get('response') or {}).get('data', {}).get('statuses', [{}])[0]
    if 'error' in status:
        raise RuntimeError(status['error'])
    order = {
        'coin': coin,
        'phase': 'exit',
        'side': 'sell',
        'maker_or_taker': 'maker',
        'order_type': 'limit',
        'submitted_price': submitted_price,
        'submitted_at': time.time(),
        'submitted_at_ms': int(time.time() * 1000),
        'timeout_seconds': int(get_config(REAL_NS)['entry_timeout_seconds']),
        'size': float(position['size']),
        'notional': float(position['size']) * submitted_price,
        'status': 'OPEN',
        'exit_reason': exit_reason,
    }
    if 'resting' in status:
        order['exchange_oid'] = status['resting']['oid']
        save_open_order_for_coin(REAL_NS, coin, order)
        set_engine_status(REAL_NS, f'{exit_reason}_ORDER_WORKING:{coin}')
        push_text_log(REAL_NS, f'REAL maker exit posted for {coin} {exit_reason} at {submitted_price:.5f}.')
        return True
    fills = fetch_real_fills_since(order['submitted_at_ms'])
    handle_real_exit_fill_multi(position, fills, exit_reason, 'post_only_limit', 'maker', submitted_price)
    return True


def execute_real_taker_exit_multi(position, exit_reason, snapshot=None):
    if not position:
        return False
    coin = position['coin']
    bundle = get_real_clients()
    exchange = bundle['exchange']
    snapshot = snapshot or fetch_coin_book_snapshot(coin)
    submitted_price = snapshot.get('best_bid', position['filled_price'])
    started_ms = int(time.time() * 1000)
    exchange.market_close(coin, sz=float(position['size']), px=submitted_price, slippage=REAL_MARKET_ORDER_TOLERANCE)
    time.sleep(0.6)
    fills = fetch_real_fills_since(started_ms)
    if not handle_real_exit_fill_multi(position, fills, exit_reason, 'market_exit', 'taker', submitted_price):
        push_text_log(REAL_NS, f'REAL taker exit requested for {coin} {exit_reason}, but no fill was detected yet.')
    return True


def manage_open_orders(namespace):
    open_orders = list(get_open_orders(namespace).values())
    if not open_orders:
        return
    if namespace == REAL_NS:
        sync_real_account_state()
    for order in open_orders:
        coin = order['coin']
        age = time.time() - float(order['submitted_at'])
        timeout_seconds = float(order.get('timeout_seconds', 10))
        if namespace == SANDBOX_NS:
            metrics = market_universe.get_metrics_for_coin(coin)
            if not metrics:
                continue
            current_mid = float(metrics['mid'])
            if order['phase'] == 'entry' and get_bot_state(SANDBOX_NS) != 'RUNNING':
                cancel_sandbox_entry_multi(coin, 'bot no longer RUNNING')
                continue
            if order['phase'] == 'entry' and (order.get('maker_or_taker') == 'taker' or current_mid <= float(order['submitted_price'])):
                fill_sandbox_entry_multi(order, current_mid)
                continue
            if order['phase'] == 'exit' and current_mid >= float(order['submitted_price']):
                position = get_position_for_coin(SANDBOX_NS, coin)
                if position:
                    fill_sandbox_exit_multi(position, order['exit_reason'], 'maker', current_mid, submitted_price=float(order['submitted_price']))
                continue
            if age >= timeout_seconds:
                if order['phase'] == 'entry':
                    cancel_sandbox_entry_multi(coin, f'not filled within {timeout_seconds:.0f} seconds')
                else:
                    save_open_order_for_coin(SANDBOX_NS, coin, None)
                    push_text_log(SANDBOX_NS, f"Sandbox maker exit {order['exit_reason']} for {coin} expired after {timeout_seconds:.0f}s.")
            continue

        stats = get_stats(REAL_NS)
        working_ids = {int(item['oid']) for item in stats.get('real_open_orders', []) if item.get('coin') == coin}
        oid = int(order.get('exchange_oid', 0)) if order.get('exchange_oid') else 0
        if oid and oid in working_ids and age < timeout_seconds:
            continue
        if oid and oid in working_ids and age >= timeout_seconds:
            cancel_real_order_multi(order, 'timeout exceeded')
            continue
        fills = fetch_real_fills_since(order['submitted_at_ms'])
        if order['phase'] == 'entry':
            if handle_real_entry_fill_multi(order, fills):
                continue
            save_open_order_for_coin(REAL_NS, coin, None)
            push_text_log(REAL_NS, f'REAL entry order for {coin} disappeared without detected fill; treating as cancelled.')
            continue
        position = get_position_for_coin(REAL_NS, coin)
        if position and handle_real_exit_fill_multi(position, fills, order.get('exit_reason', 'UNKNOWN'), 'post_only_limit', 'maker', float(order['submitted_price'])):
            continue
        save_open_order_for_coin(REAL_NS, coin, None)
        push_text_log(REAL_NS, f"REAL maker exit {order.get('exit_reason', 'UNKNOWN')} for {coin} no longer working and no fill was detected.")


def maybe_place_exit_order_multi(namespace, position, exit_reason, maker_or_taker, snapshot):
    coin = position['coin']
    open_order = get_open_order_for_coin(namespace, coin)
    if open_order and open_order.get('phase') == 'exit':
        return False
    if namespace == SANDBOX_NS:
        if maker_or_taker == 'taker':
            fill_sandbox_exit_multi(position, exit_reason, 'taker', float(snapshot['mid']), submitted_price=float(snapshot.get('best_bid', snapshot['mid'])))
            return True
        order = {
            'coin': coin,
            'phase': 'exit',
            'side': 'sell',
            'maker_or_taker': 'maker',
            'order_type': 'limit',
            'submitted_price': float(snapshot.get('best_ask', snapshot['mid'])),
            'submitted_at': time.time(),
            'submitted_at_ms': int(time.time() * 1000),
            'timeout_seconds': int(get_config(SANDBOX_NS)['entry_timeout_seconds']),
            'size': float(position['size']),
            'notional': float(position['size']) * float(snapshot.get('best_ask', snapshot['mid'])),
            'status': 'OPEN',
            'exit_reason': exit_reason,
        }
        save_open_order_for_coin(SANDBOX_NS, coin, order)
        set_engine_status(SANDBOX_NS, f'{exit_reason}_ORDER_WORKING:{coin}')
        push_text_log(SANDBOX_NS, f"Sandbox maker exit posted for {coin} {exit_reason} at {order['submitted_price']:.5f}.")
        return True
    if maker_or_taker == 'taker':
        return execute_real_taker_exit_multi(position, exit_reason, snapshot)
    return submit_real_maker_exit_multi(position, snapshot, exit_reason)


def manage_positions(namespace):
    positions = list(get_positions(namespace).values())
    if not positions:
        return
    config = get_config(namespace)
    total_positions = 0
    for position in positions:
        coin = position['coin']
        market_metrics = market_universe.get_metrics_for_coin(coin)
        if not market_metrics:
            continue
        current_mid = float(market_metrics['mid'])
        position = update_position_extremes(position, current_mid)
        save_position_for_coin(namespace, coin, position)
        total_positions += 1
        if get_bot_state(namespace) == 'KILLED':
            continue
        change_pct = pct_change(float(position['filled_price']), current_mid)
        seconds_open = time.time() - float(position['entry_time'])
        if change_pct <= -abs(float(config['emergency_exit_drop_pct'])) and seconds_open <= int(config['emergency_window_seconds']):
            maybe_place_exit_order_multi(namespace, position, 'EMERGENCY_EXIT', 'taker', {'coin': coin, 'mid': current_mid, 'best_bid': current_mid, 'best_ask': current_mid})
            continue
        if change_pct <= -abs(float(config['stop_loss_pct'])):
            maybe_place_exit_order_multi(namespace, position, 'STOP_LOSS', 'taker', {'coin': coin, 'mid': current_mid, 'best_bid': current_mid, 'best_ask': current_mid})
            continue
        if change_pct >= float(config['take_profit_pct']):
            maybe_place_exit_order_multi(namespace, position, 'TAKE_PROFIT', 'taker', fetch_coin_book_snapshot(coin))
            continue
        if seconds_open >= int(config['time_stop_seconds']):
            maybe_place_exit_order_multi(namespace, position, 'TIME_STOP', 'maker', fetch_coin_book_snapshot(coin))
            continue
    if total_positions:
        set_engine_status(namespace, f'MANAGING_{total_positions}_POSITIONS')


def attempt_entries(namespace):
    config = get_config(namespace)
    positions = get_positions(namespace)
    open_orders = get_open_orders(namespace)
    active_slots = len(positions) + count_entry_orders(namespace)
    slots_left = max(0, int(config.get('max_open_positions', 1)) - active_slots)
    if slots_left <= 0:
        return
    primary_basis = '30s' if namespace == SANDBOX_NS else '5m'
    candidates = market_universe.get_hot_perps(limit=12, primary_basis=primary_basis).get('leaders', [])
    for candidate in candidates:
        if slots_left <= 0:
            return
        coin = candidate['coin']
        if coin in positions or coin in open_orders:
            continue
        snapshot, metrics = build_coin_snapshot_and_metrics(coin, require_book=True)
        if not snapshot or not metrics:
            continue
        passed, reason, checks = evaluate_entry_candidate(namespace, coin, snapshot, metrics)
        record_signal(namespace, snapshot, metrics, passed, reason, checks)
        if not passed:
            continue
        if namespace == SANDBOX_NS:
            if place_sandbox_entry_multi(coin, snapshot):
                slots_left -= 1
        else:
            if submit_real_entry_multi(coin, snapshot):
                slots_left -= 1


def compute_open_position_views(namespace):
    config = get_config(namespace)
    views = []
    total_live_pnl = 0.0
    for coin, position in get_positions(namespace).items():
        market_metrics = market_universe.get_metrics_for_coin(coin)
        current_mid = float(market_metrics['mid']) if market_metrics else float(position.get('filled_price', 0.0))
        view = compute_position_view(position, current_mid)
        view['targets'] = build_position_targets(position, config)
        view['progress_pct'] = trim_float(compute_progress_pct(view, config), 2)
        views.append(view)
        total_live_pnl += float(view.get('gross_unrealized_pnl', 0.0))
    views.sort(key=lambda item: item.get('gross_unrealized_pnl_pct', 0.0), reverse=True)
    return views, total_live_pnl


def build_market_focus():
    hot = market_universe.get_hot_perps(limit=1).get('leaders', [])
    if not hot:
        return {
            'coin': 'N/A',
            'mid': 0.0,
            'best_bid': 0.0,
            'best_ask': 0.0,
            'spread_pct': 0.0,
            'return_30s': 0.0,
            'return_1m': 0.0,
            'return_2m': 0.0,
            'return_5m': 0.0,
            'return_15m': 0.0,
            'return_60m': 0.0,
            'bounce_from_2m_low': 0.0,
            'book_imbalance': 0.5,
            'book_imbalance_10s_ago': 0.5,
            'market_data_status': 'BOOTING',
            'market_data_source': 'allMids',
            'market_data_error': market_universe.last_error,
            'market_data_age': 0.0,
        }
    coin = hot[0]['coin']
    snapshot, metrics = build_coin_snapshot_and_metrics(coin, require_book=False)
    return {
        'coin': coin,
        'mid': trim_float(metrics['mid'], 6),
        'best_bid': trim_float(metrics['best_bid'], 6),
        'best_ask': trim_float(metrics['best_ask'], 6),
        'spread_pct': trim_float(metrics['spread_pct'], 6),
        'return_2m': trim_float(metrics['return_2m'], 6),
        'return_5m': trim_float(metrics['return_5m'], 6),
        'return_15m': trim_float(metrics['return_15m'], 6),
        'return_60m': trim_float(metrics['return_60m'], 6),
        'bounce_from_2m_low': trim_float(metrics['bounce_from_2m_low'], 6),
        'book_imbalance': trim_float(metrics['book_imbalance'], 6),
        'book_imbalance_10s_ago': trim_float(metrics['book_imbalance_10s_ago'], 6),
        'market_data_status': 'LIVE' if metrics['market_data_age'] <= MARKET_STALE_AFTER_SECONDS else 'STALE',
        'market_data_source': 'allMids',
        'market_data_error': market_universe.last_error,
        'market_data_age': trim_float(metrics['market_data_age'], 2),
    }


def build_health_payload(namespace):
    market_universe.ensure_running()
    with market_universe.lock:
        universe_last_message_at = market_universe.last_message_at
        universe_last_error = market_universe.last_error
        universe_last_open_at = market_universe.last_open_at
        universe_last_close_at = market_universe.last_close_at
        universe_last_close_code = market_universe.last_close_code
        universe_last_close_reason = market_universe.last_close_reason
        universe_ws_alive = bool(market_universe.ws_thread and market_universe.ws_thread.is_alive())
        universe_loop_alive = bool(market_universe.loop_thread and market_universe.loop_thread.is_alive())
    payload = {
        'database': 'OK',
        'hyperliquid_sdk': 'OK' if not HYPERLIQUID_IMPORT_ERROR else 'ERROR',
        'hyperliquid_api': 'N/A',
        'market_data': 'LIVE' if universe_last_message_at and (time.time() - universe_last_message_at) <= MARKET_STALE_AFTER_SECONDS else 'STALE',
        'market_data_source': 'allMids',
        'market_loop_alive': universe_loop_alive,
        'ws_thread_alive': universe_ws_alive,
        'ws_reconnect_count': 0,
        'ws_last_open_at': universe_last_open_at,
        'ws_last_message_at': universe_last_message_at,
        'ws_last_close_at': universe_last_close_at,
        'ws_last_close_code': universe_last_close_code,
        'ws_last_close_reason': universe_last_close_reason,
        'ws_last_error': universe_last_error,
        'market_loop_last_heartbeat_at': universe_last_message_at,
        'market_loop_last_error': universe_last_error,
        'engine': get_engine_status(namespace),
        'errors': [],
    }
    try:
        redis.ping()
    except Exception as exc:
        payload['database'] = 'ERROR'
        payload['errors'].append(str(exc))
    if HYPERLIQUID_IMPORT_ERROR:
        payload['errors'].append(HYPERLIQUID_IMPORT_ERROR)
    if universe_last_error:
        payload['errors'].append(universe_last_error)
    if namespace == REAL_NS:
        if not HYPERLIQUID_API_KEY or not HYPERLIQUID_API_SECRET:
            payload['hyperliquid_api'] = 'MISSING_KEYS'
        else:
            stats = get_stats(REAL_NS)
            payload['hyperliquid_api'] = 'READY' if not stats.get('last_real_sync_error') else 'ERROR'
            if stats.get('last_real_sync_error'):
                payload['errors'].append(stats['last_real_sync_error'])
    return payload


def build_state_payload(namespace):
    market_universe.ensure_running()
    if namespace == REAL_NS:
        sync_real_account_state()
    balances = get_balances(namespace)
    stats = get_stats(namespace)
    open_positions, total_live_pnl = compute_open_position_views(namespace)
    if namespace == SANDBOX_NS:
        balances['equity'] = float(balances.get('available', 0.0)) + total_live_pnl + sum(float(view.get('notional', 0.0)) for view in open_positions)
        save_balances(namespace, balances)
    config = get_config(namespace)
    focus_market = build_market_focus()
    hot_primary_basis = '30s' if namespace == SANDBOX_NS else '5m'
    hot_perps = market_universe.get_hot_perps(limit=10, primary_basis=hot_primary_basis)
    open_orders = get_open_orders(namespace)
    active_position = open_positions[0] if open_positions else {'active': False}
    payload = {
        'environment': namespace.upper(),
        'bot_state': get_bot_state(namespace),
        'engine_status': get_engine_status(namespace),
        'real_warning': namespace == REAL_NS,
        'requires_confirmation': namespace == REAL_NS,
        'market': focus_market,
        'market_diagnostics': {
            'status': 'LIVE' if focus_market['market_data_age'] <= MARKET_STALE_AFTER_SECONDS else 'STALE',
            'error': market_universe.last_error,
            'reconnect_count': 0,
            'ws_thread_alive': bool(market_universe.ws_thread and market_universe.ws_thread.is_alive()),
            'loop_thread_alive': bool(market_universe.loop_thread and market_universe.loop_thread.is_alive()),
            'last_open_at': market_universe.last_open_at,
            'last_open_at_label': format_timestamp(market_universe.last_open_at),
            'last_message_at': market_universe.last_message_at,
            'last_message_at_label': format_timestamp(market_universe.last_message_at),
            'last_close_at': market_universe.last_close_at,
            'last_close_at_label': format_timestamp(market_universe.last_close_at),
            'last_close_code': market_universe.last_close_code,
            'last_close_reason': market_universe.last_close_reason,
            'last_ws_error': market_universe.last_error,
            'last_loop_heartbeat_at': market_universe.last_message_at,
            'last_loop_heartbeat_at_label': format_timestamp(market_universe.last_message_at),
            'last_loop_error': market_universe.last_error,
        },
        'balances': {
            'available': trim_float(balances.get('available', 0.0), 6),
            'equity': trim_float(balances.get('equity', 0.0), 6),
        },
        'active_position': active_position,
        'positions': open_positions,
        'portfolio': {
            'open_positions_count': len(open_positions),
            'max_open_positions': int(config.get('max_open_positions', 1)),
            'total_live_pnl': trim_float(total_live_pnl, 6),
            'slots_used': len(open_positions) + count_entry_orders(namespace),
        },
        'open_orders': {
            'count': get_effective_open_order_count(namespace),
            'items': list(open_orders.values()),
        },
        'config': config,
        'stats': {
            'daily_pnl': trim_float(stats.get('daily_pnl', 0.0), 6),
            'total_pnl': trim_float(stats.get('total_pnl', 0.0), 6),
            'consecutive_losses': int(stats.get('consecutive_losses', 0)),
            'trades_today': int(stats.get('trades_today', 0)),
            'trades_last_hour': get_hourly_trade_count(namespace),
            'cancelled_entries': int(stats.get('cancelled_entries', 0)),
            'time_stops': int(stats.get('time_stops', 0)),
            'emergency_exits': int(stats.get('emergency_exits', 0)),
            'last_loss_at': stats.get('last_loss_at', 0.0),
            'last_loss_at_label': format_timestamp(stats.get('last_loss_at', 0.0)),
            'unsupported_external_position': stats.get('unsupported_external_position', ''),
        },
        'last_signal': get_last_signal(namespace),
        'last_trade': get_last_trade(namespace),
        'hot_perps': hot_perps['leaders'],
        'hot_perps_error': hot_perps['last_error'],
        'reason_last_trade_not_taken': get_reason_not_taken(namespace),
        'logs': load_action_logs(namespace),
    }
    if namespace == REAL_NS:
        payload['real_account'] = {
            'api_key_present': bool(HYPERLIQUID_API_KEY),
            'api_secret_present': bool(HYPERLIQUID_API_SECRET),
            'open_orders': get_stats(REAL_NS).get('real_open_orders', []),
        }
    return payload


def trade_loop(namespace):
    market_universe.ensure_running()
    while True:
        try:
            lock_key = ns_key(namespace, 'loop_lock')
            acquired = redis.set(lock_key, INSTANCE_ID, nx=True, ex=15)
            if not acquired and redis.get(lock_key) != INSTANCE_ID:
                time.sleep(TRADING_LOOP_INTERVAL)
                continue
            redis.set(lock_key, INSTANCE_ID, ex=15)
            if namespace == REAL_NS:
                sync_real_account_state()
            manage_open_orders(namespace)
            manage_positions(namespace)
            if get_bot_state(namespace) != 'RUNNING':
                set_engine_status(namespace, 'PAUSED_WAITING_FOR_START' if get_bot_state(namespace) == 'PAUSED' else 'KILLED')
                time.sleep(TRADING_LOOP_INTERVAL)
                continue
            attempt_entries(namespace)
            if not get_positions(namespace) and not get_open_orders(namespace):
                set_engine_status(namespace, 'RUNNING_SCANNING_MARKET')
        except Exception as exc:
            set_engine_status(namespace, f'LOOP_ERROR: {exc}')
            push_text_log(namespace, f'Loop error: {exc}')
        time.sleep(TRADING_LOOP_INTERVAL)

def parse_config_updates(namespace, incoming):
    config = get_config(namespace)
    for key, value in incoming.items():
        if key not in EDITABLE_CONFIG_FIELDS:
            continue
        if key == 'require_imbalance_improvement':
            config[key] = bool(value)
            continue
        if key in {'max_trades_per_hour', 'consecutive_loss_limit', 'cooldown_after_loss_seconds', 'entry_cooldown_seconds', 'time_stop_seconds', 'emergency_window_seconds', 'entry_timeout_seconds', 'max_open_positions'}:
            config[key] = max(0, int(float(value)))
            continue
        config[key] = float(value)
    if namespace == REAL_NS:
        config.pop('maker_entry_slippage_pct', None)
        config.pop('maker_exit_slippage_pct', None)
        config.pop('taker_exit_slippage_pct', None)
        config['simulate_slippage'] = False
    if namespace == SANDBOX_NS:
        config['trade_notional_usdc'] = min(config['trade_notional_usdc'], config['max_notional_usdc'])
    return config


@app.before_request
def require_login():
    allowed_routes = {
        'login',
        'logout',
        'static',
        'index',
        'sandbox_index',
        'trade_history_page',
        'sandbox_trade_history_page',
        'results_page',
        'sandbox_results_page',
        'health_check',
        'sandbox_health_check',
        'get_trades',
        'sandbox_get_trades',
        'get_results',
        'sandbox_get_results',
        'get_state',
        'sandbox_get_state',
        'real_start',
        'sandbox_start',
        'real_pause',
        'sandbox_pause',
        'real_kill',
        'sandbox_kill',
        'real_manual_close',
        'sandbox_manual_close',
        'sandbox_reset',
        'save_real_config',
        'save_sandbox_config',
        'clear_real_logs',
        'clear_sandbox_logs',
    }
    if request.endpoint not in allowed_routes and not session.get('logged_in'):
        return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('index'))
        error = 'Invalid credentials.'
    return render_template('login.html', error=error)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/')
def index():
    return render_template('index.html', api_base='/api', page_label='REAL', results_href='/results', trades_href='/trades')


@app.route('/sandbox')
def sandbox_index():
    return render_template('index.html', api_base='/sandbox/api', page_label='SANDBOX', results_href='/sandbox/results', trades_href='/sandbox/trades')


@app.route('/trades')
def trade_history_page():
    return render_template('trade_history.html', api_base='/api', page_label='REAL', dashboard_href='/', results_href='/results')


@app.route('/sandbox/trades')
def sandbox_trade_history_page():
    return render_template('trade_history.html', api_base='/sandbox/api', page_label='SANDBOX', dashboard_href='/sandbox', results_href='/sandbox/results')


@app.route('/results')
def results_page():
    return render_template('results.html', api_base='/api', page_label='REAL', dashboard_href='/', trades_href='/trades')


@app.route('/sandbox/results')
def sandbox_results_page():
    return render_template('results.html', api_base='/sandbox/api', page_label='SANDBOX', dashboard_href='/sandbox', trades_href='/sandbox/trades')


@app.route('/api/health')
def health_check():
    return jsonify(build_health_payload(REAL_NS))


@app.route('/sandbox/api/health')
def sandbox_health_check():
    return jsonify(build_health_payload(SANDBOX_NS))


@app.route('/api/state')
def get_state():
    return jsonify(build_state_payload(REAL_NS))


@app.route('/sandbox/api/state')
def sandbox_get_state():
    return jsonify(build_state_payload(SANDBOX_NS))


@app.route('/api/trades')
def get_trades():
    return jsonify({'trades': load_json_log(REAL_NS, 'trade_log', TRADE_LOG_LIMIT)})


@app.route('/sandbox/api/trades')
def sandbox_get_trades():
    return jsonify({'trades': load_json_log(SANDBOX_NS, 'trade_log', TRADE_LOG_LIMIT)})


@app.route('/api/declined_trades')
def get_declined_trades():
    signals = load_json_log(REAL_NS, 'signal_log', SIGNAL_LOG_LIMIT)
    declined = [signal for signal in signals if not signal.get('entry_conditions_passed')]
    return jsonify({'declined_trades': declined})


@app.route('/sandbox/api/declined_trades')
def sandbox_get_declined_trades():
    signals = load_json_log(SANDBOX_NS, 'signal_log', SIGNAL_LOG_LIMIT)
    declined = [signal for signal in signals if not signal.get('entry_conditions_passed')]
    return jsonify({'declined_trades': declined})


@app.route('/api/results')
def get_results():
    return jsonify(compute_results_summary(REAL_NS))


@app.route('/sandbox/api/results')
def sandbox_get_results():
    return jsonify(compute_results_summary(SANDBOX_NS))


@app.route('/api/start', methods=['POST'])
def real_start():
    data = request.json or {}
    confirm_text = str(data.get('confirm_text', '')).strip()
    if confirm_text != REAL_START_CONFIRM_TEXT:
        return jsonify({'status': 'error', 'message': f'Type {REAL_START_CONFIRM_TEXT} to start REAL mode.'}), 400
    try:
        get_real_clients()
        sync_real_account_state()
    except Exception as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 400
    set_bot_state(REAL_NS, 'RUNNING')
    set_engine_status(REAL_NS, 'RUNNING_WAITING_FOR_SIGNAL')
    push_text_log(REAL_NS, 'REAL bot started by operator after confirmation.')
    return jsonify({'status': 'success'})


@app.route('/sandbox/api/start', methods=['POST'])
def sandbox_start():
    set_bot_state(SANDBOX_NS, 'RUNNING')
    set_engine_status(SANDBOX_NS, 'RUNNING_WAITING_FOR_SIGNAL')
    push_text_log(SANDBOX_NS, 'SANDBOX bot started by operator.')
    return jsonify({'status': 'success'})


@app.route('/api/pause', methods=['POST'])
def real_pause():
    set_bot_state(REAL_NS, 'PAUSED')
    set_engine_status(REAL_NS, 'PAUSED_BY_OPERATOR')
    for order in list(get_open_orders(REAL_NS).values()):
        if order.get('phase') == 'entry':
            cancel_real_order_multi(order, 'operator paused bot')
    push_text_log(REAL_NS, 'REAL bot paused by operator. Existing positions remain managed.')
    return jsonify({'status': 'success'})


@app.route('/sandbox/api/pause', methods=['POST'])
def sandbox_pause():
    set_bot_state(SANDBOX_NS, 'PAUSED')
    set_engine_status(SANDBOX_NS, 'PAUSED_BY_OPERATOR')
    for coin, order in list(get_open_orders(SANDBOX_NS).items()):
        if order.get('phase') == 'entry':
            cancel_sandbox_entry_multi(coin, 'operator paused bot')
    push_text_log(SANDBOX_NS, 'SANDBOX bot paused by operator. Existing positions remain managed.')
    return jsonify({'status': 'success'})


@app.route('/api/kill', methods=['POST'])
def real_kill():
    data = request.json or {}
    close_position = bool(data.get('close_position', False))
    set_bot_state(REAL_NS, 'KILLED')
    try:
        sync_real_account_state()
        for order in get_stats(REAL_NS).get('real_open_orders', []):
            if order.get('coin'):
                get_real_clients()['exchange'].cancel(order['coin'], int(order['oid']))
        save_open_orders(REAL_NS, {})
        if close_position:
            for position in list(get_positions(REAL_NS).values()):
                execute_real_taker_exit_multi(position, 'KILL_SWITCH_CLOSE')
        set_engine_status(REAL_NS, 'KILLED')
        push_text_log(REAL_NS, f'REAL kill switch engaged. close_position={close_position}.')
        return jsonify({'status': 'success'})
    except Exception as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 500


@app.route('/sandbox/api/kill', methods=['POST'])
def sandbox_kill():
    set_bot_state(SANDBOX_NS, 'KILLED')
    save_open_orders(SANDBOX_NS, {})
    for position in list(get_positions(SANDBOX_NS).values()):
        market_metrics = market_universe.get_metrics_for_coin(position['coin'])
        current_mid = float(market_metrics['mid']) if market_metrics else float(position['filled_price'])
        fill_sandbox_exit_multi(position, 'KILL_SWITCH_CLOSE', 'taker', current_mid, submitted_price=current_mid)
    set_engine_status(SANDBOX_NS, 'KILLED')
    push_text_log(SANDBOX_NS, 'SANDBOX kill switch engaged.')
    return jsonify({'status': 'success'})


@app.route('/api/manual_close', methods=['POST'])
def real_manual_close():
    try:
        positions = list(get_positions(REAL_NS).values())
        if not positions:
            return jsonify({'status': 'error', 'message': 'No REAL position is open.'}), 400
        for position in positions:
            execute_real_taker_exit_multi(position, 'MANUAL_CLOSE')
        return jsonify({'status': 'success'})
    except Exception as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 500


@app.route('/sandbox/api/manual_close', methods=['POST'])
def sandbox_manual_close():
    positions = list(get_positions(SANDBOX_NS).values())
    if not positions:
        return jsonify({'status': 'error', 'message': 'No SANDBOX position is open.'}), 400
    for position in positions:
        market_metrics = market_universe.get_metrics_for_coin(position['coin'])
        current_mid = float(market_metrics['mid']) if market_metrics else float(position['filled_price'])
        fill_sandbox_exit_multi(position, 'MANUAL_CLOSE', 'taker', current_mid, submitted_price=current_mid)
    return jsonify({'status': 'success'})


@app.route('/sandbox/api/reset', methods=['POST'])
def sandbox_reset():
    if get_positions(SANDBOX_NS) or get_open_orders(SANDBOX_NS):
        return jsonify({'status': 'error', 'message': 'Pause and clear positions/orders before resetting SANDBOX.'}), 400
    reset_sandbox_state(clear_logs=False)
    return jsonify({'status': 'success'})


@app.route('/api/config', methods=['POST'])
def save_real_config():
    try:
        config = parse_config_updates(REAL_NS, request.json or {})
        save_config(REAL_NS, config)
        push_text_log(REAL_NS, 'REAL config updated by operator.')
        return jsonify({'status': 'success', 'config': config})
    except Exception as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 400


@app.route('/sandbox/api/config', methods=['POST'])
def save_sandbox_config():
    try:
        config = parse_config_updates(SANDBOX_NS, request.json or {})
        save_config(SANDBOX_NS, config)
        push_text_log(SANDBOX_NS, 'SANDBOX config updated by operator.')
        return jsonify({'status': 'success', 'config': config})
    except Exception as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 400


@app.route('/api/logs/clear', methods=['POST'])
def clear_real_logs():
    redis.delete(ns_key(REAL_NS, 'action_logs'))
    push_text_log(REAL_NS, 'REAL action log cleared by operator.')
    return jsonify({'status': 'success'})


@app.route('/sandbox/api/logs/clear', methods=['POST'])
def clear_sandbox_logs():
    redis.delete(ns_key(SANDBOX_NS, 'action_logs'))
    push_text_log(SANDBOX_NS, 'SANDBOX action log cleared by operator.')
    return jsonify({'status': 'success'})


Thread(target=market_data.loop, daemon=True).start()
Thread(target=trade_loop, args=(REAL_NS,), daemon=True).start()
Thread(target=trade_loop, args=(SANDBOX_NS,), daemon=True).start()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
