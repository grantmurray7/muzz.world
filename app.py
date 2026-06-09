import os
import time
import uuid
import math
import json
import requests
import queue
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from email.utils import formatdate
from html import escape
from threading import Thread, Lock
from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from binance import Client, ThreadedWebsocketManager
from psycopg2.extras import Json
from psycopg2.pool import ThreadedConnectionPool

app = Flask(__name__)

# --- Environment Variables & Config ---
app.secret_key = os.environ.get("FLASK_SECRET_KEY", str(uuid.uuid4()))
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "DefaultPassword123!")
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET")
RENDER_INTERNAL_DATABASE = os.environ.get("RENDER_INTERNAL_DATABASE") or os.environ.get("DATABASE_URL")


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
            (key,)
        )

    @staticmethod
    def _expires_at(ex_seconds):
        if ex_seconds is None:
            return None
        return datetime.now(timezone.utc) + timedelta(seconds=int(ex_seconds))

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
        return items[start:end + 1]

    def get(self, key):
        with self.connection() as conn:
            with conn.cursor() as cur:
                self._purge_expired_key(cur, key)
                cur.execute('SELECT value FROM state_kv WHERE key = %s', (key,))
                row = cur.fetchone()
                return None if row is None else row[0]

    def set(self, key, value, nx=False, ex=None):
        expires_at = self._expires_at(ex)
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
                        (key, str(value), expires_at)
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
                    (key, str(value), expires_at)
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
                    (key,)
                )
                return int(cur.fetchone()[0])

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
            (key, Json(items))
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
price_state_lock = Lock()
log_queue = queue.Queue()

SYMBOL = 'SOLUSDT'
SAMPLE_INTERVAL = 1
SCALP_WINDOW_SECONDS = 90
DROP_TRIGGER_PCT = -0.25
BOUNCE_TRIGGER_PCT = 0.10
AUTO_BUY_ALLOCATION = 0.95
SELL_TARGET_PCT = 0.35
SELL_TARGET_MULTIPLIER = 1 + (SELL_TARGET_PCT / 100)
HARD_STOP_PCT = -0.15
PRICE_HISTORY_LIMIT = 1500
MAX_TRADE_USDT = 50.00
TRADE_HISTORY_LIMIT = 200
LOG_HISTORY_LIMIT = 250
RSS_STATUS_INTERVAL = 30
WEBSOCKET_STALE_AFTER = 20
SANDBOX_START_USDT = 14000.0
MIN_POSITION_SOL = 0.01
INSTANCE_ID = str(uuid.uuid4())[:8]

GLOBAL_NS = 'global'
LIVE_NS = 'live'
SANDBOX_NS = 'sandbox'


@app.before_request
def require_login():
    allowed_routes = {
        'login', 'logout', 'static',
        'index', 'sandbox_index', 'trade_history_page', 'sandbox_trade_history_page',
        'health_check', 'sandbox_health_check', 'status_feed',
        'execute_manual_buy', 'sandbox_execute_manual_buy',
        'get_trades', 'sandbox_get_trades',
        'get_state', 'sandbox_get_state',
        'toggle_bot', 'sandbox_toggle_bot',
        'manual_set_position', 'sandbox_manual_set_position',
        'clear_logs', 'sandbox_clear_logs',
        'liquidate_to_usdt', 'sandbox_liquidate_to_usdt',
    }
    if request.endpoint not in allowed_routes and not session.get('logged_in'):
        return redirect(url_for('login'))


def ns_key(namespace, key):
    return f'{namespace}:{key}'


def log_activity(msg, namespace=LIVE_NS, skip_db=False):
    timestamp = time.strftime('%H:%M:%S')
    tag = 'SANDBOX' if namespace == SANDBOX_NS else INSTANCE_ID
    log_line = f'[{timestamp}] [{tag}] {msg}'
    print(log_line)
    if not skip_db:
        log_queue.put((namespace, log_line))


def redis_log_worker():
    while True:
        namespace, log_line = log_queue.get()
        try:
            redis.lpush(ns_key(namespace, 'bot_logs'), log_line)
            redis.ltrim(ns_key(namespace, 'bot_logs'), 0, LOG_HISTORY_LIMIT - 1)
        except Exception as e:
            print(f'Logging fail to database: {e}')
        finally:
            log_queue.task_done()


Thread(target=redis_log_worker, daemon=True).start()


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


def ns_lpush(namespace, key, value):
    redis.lpush(ns_key(namespace, key), value)


def ns_ltrim(namespace, key, start, end):
    redis.ltrim(ns_key(namespace, key), start, end)


def ns_lrange(namespace, key, start, end):
    return redis.lrange(ns_key(namespace, key), start, end)


def read_float_state(namespace, key, default=0.0):
    try:
        return float(ns_get(namespace, key, default) or default)
    except Exception:
        return default


def read_int_state(namespace, key, default=0):
    try:
        return int(ns_get(namespace, key, default) or default)
    except Exception:
        return default


def read_text_state(namespace, key, default=''):
    try:
        return str(ns_get(namespace, key, default) or default)
    except Exception:
        return default


def set_if_missing(namespace, key, value):
    if ns_get(namespace, key) is None:
        ns_set(namespace, key, value)


def init_live_state():
    set_if_missing(LIVE_NS, 'bot_running', 'false')
    set_if_missing(LIVE_NS, 'position_active', 'false')
    set_if_missing(LIVE_NS, 'balance_usdt', 0)
    set_if_missing(LIVE_NS, 'balance_sol', 0)
    set_if_missing(LIVE_NS, 'engine_status', 'BOOTING')
    set_if_missing(LIVE_NS, 'strategy_mode', 'BOOTING')
    set_if_missing(LIVE_NS, 'strategy_setup', 'Connecting live services')


def bootstrap_sandbox():
    if ns_get(SANDBOX_NS, 'initialized') == 'true':
        set_if_missing(SANDBOX_NS, 'bot_running', 'false')
        set_if_missing(SANDBOX_NS, 'position_active', 'false')
        set_if_missing(SANDBOX_NS, 'balance_usdt', SANDBOX_START_USDT)
        set_if_missing(SANDBOX_NS, 'balance_sol', 0)
        set_if_missing(SANDBOX_NS, 'engine_status', 'PAUSED')
        set_if_missing(SANDBOX_NS, 'strategy_mode', 'PAUSED')
        set_if_missing(SANDBOX_NS, 'strategy_setup', 'Sandbox paused by default')
        return
    ns_set(SANDBOX_NS, 'initialized', 'true')
    ns_set(SANDBOX_NS, 'bot_running', 'false')
    ns_set(SANDBOX_NS, 'position_active', 'false')
    ns_set(SANDBOX_NS, 'balance_usdt', SANDBOX_START_USDT)
    ns_set(SANDBOX_NS, 'balance_sol', 0)
    ns_set(SANDBOX_NS, 'engine_status', 'PAUSED')
    set_strategy_snapshot(SANDBOX_NS, mode='PAUSED', setup='Sandbox paused by default')


def record_trade(namespace, side, quantity, price, source, note=''):
    try:
        trade_number = ns_incr(namespace, 'trade_count')
        trade = {
            'id': trade_number,
            'ts': time.time(),
            'time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': SYMBOL,
            'side': side,
            'quantity': round(float(quantity), 4),
            'price': round(float(price), 4),
            'notional_usdt': round(float(quantity) * float(price), 2),
            'source': source,
            'note': note,
            'environment': namespace.upper()
        }
        ns_lpush(namespace, 'trade_history', json.dumps(trade))
        ns_ltrim(namespace, 'trade_history', 0, TRADE_HISTORY_LIMIT - 1)
        return trade
    except Exception as e:
        log_activity(f'Trade history write failed: {e}', namespace=namespace)
        return None


def load_trade_history(namespace, limit=50):
    raw_trades = ns_lrange(namespace, 'trade_history', 0, max(0, limit - 1))
    trades = []
    for raw_trade in raw_trades:
        try:
            trades.append(json.loads(raw_trade))
        except Exception:
            continue
    return trades


def get_binance_client():
    return Client(BINANCE_API_KEY, BINANCE_API_SECRET)


def record_price_sample(price, sample_ts=None):
    sample_ts = sample_ts or time.time()
    sample = json.dumps({'ts': sample_ts, 'price': price})
    redis.lpush(ns_key(GLOBAL_NS, 'price_history'), sample)
    redis.ltrim(ns_key(GLOBAL_NS, 'price_history'), 0, PRICE_HISTORY_LIMIT - 1)


def update_live_price(price, sample_ts=None):
    sample_ts = sample_ts or time.time()
    with price_state_lock:
        redis.set(ns_key(GLOBAL_NS, 'current_sol_price'), str(price))
        redis.set(ns_key(GLOBAL_NS, 'websocket_last_seen'), str(sample_ts))
        redis.set(ns_key(GLOBAL_NS, 'websocket_status'), 'LIVE')
        record_price_sample(price, sample_ts=sample_ts)


def handle_price_socket_message(msg):
    try:
        if not isinstance(msg, dict):
            return
        if msg.get('e') == 'error':
            error_type = msg.get('type', 'socket_error')
            error_msg = msg.get('m', 'unknown websocket error')
            redis.set(ns_key(GLOBAL_NS, 'websocket_status'), f'ERROR: {error_type}')
            log_activity(f'Price websocket error: {error_type} | {error_msg}', namespace=LIVE_NS)
            log_activity(f'Price websocket error: {error_type} | {error_msg}', namespace=SANDBOX_NS)
            return
        raw_price = msg.get('c') or msg.get('p')
        if raw_price is None:
            return
        price = float(raw_price)
        event_ts = time.time()
        if msg.get('E'):
            event_ts = float(msg['E']) / 1000.0
        update_live_price(price, sample_ts=event_ts)
    except Exception as e:
        redis.set(ns_key(GLOBAL_NS, 'websocket_status'), 'ERROR: callback')
        log_activity(f'Price websocket callback failure: {e}', namespace=LIVE_NS)


def run_price_stream():
    while True:
        twm = None
        try:
            redis.set(ns_key(GLOBAL_NS, 'websocket_status'), 'CONNECTING')
            twm = ThreadedWebsocketManager()
            twm.start()
            twm.start_symbol_ticker_socket(callback=handle_price_socket_message, symbol=SYMBOL)
            log_activity(f'Live price websocket started for {SYMBOL}.', namespace=LIVE_NS)
            log_activity(f'Live price websocket started for {SYMBOL}.', namespace=SANDBOX_NS)
            twm.join()
            redis.set(ns_key(GLOBAL_NS, 'websocket_status'), 'STOPPED')
        except Exception as e:
            redis.set(ns_key(GLOBAL_NS, 'websocket_status'), 'ERROR: bootstrap')
            log_activity(f'Price websocket bootstrap failed: {e}', namespace=LIVE_NS)
            log_activity(f'Price websocket bootstrap failed: {e}', namespace=SANDBOX_NS)
        finally:
            try:
                if twm:
                    twm.stop()
            except Exception:
                pass
        time.sleep(5)


def get_live_price(max_age=WEBSOCKET_STALE_AFTER):
    try:
        current_price = float(redis.get(ns_key(GLOBAL_NS, 'current_sol_price')) or 0.0)
    except Exception:
        current_price = 0.0
    try:
        last_seen = float(redis.get(ns_key(GLOBAL_NS, 'websocket_last_seen')) or 0.0)
    except Exception:
        last_seen = 0.0
    if current_price <= 0 or last_seen <= 0:
        raise RuntimeError('websocket price not ready yet')
    age = time.time() - last_seen
    if age > max_age:
        raise RuntimeError(f'websocket price stale ({age:.1f}s old)')
    return current_price


def load_price_samples():
    raw_samples = redis.lrange(ns_key(GLOBAL_NS, 'price_history'), 0, PRICE_HISTORY_LIMIT - 1)
    parsed_samples = []
    for raw_sample in reversed(raw_samples):
        try:
            sample = json.loads(raw_sample)
            parsed_samples.append({'ts': float(sample['ts']), 'price': float(sample['price'])})
        except Exception:
            continue
    return parsed_samples


def get_recent_samples(samples, seconds, now=None):
    now = now or time.time()
    cutoff = now - seconds
    return [sample for sample in samples if sample.get('ts', 0.0) >= cutoff]


def pct_change(from_price, to_price):
    if from_price <= 0:
        return 0.0
    return ((to_price - from_price) / from_price) * 100


def set_strategy_snapshot(namespace, **fields):
    for key, value in fields.items():
        ns_set(namespace, f'strategy_{key}', value)


def clear_position_tracking(namespace):
    ns_delete(namespace, 'position_opened_at')


def reset_position_state(namespace, setup='Position reset to cash mode'):
    ns_set(namespace, 'position_active', 'false')
    ns_delete(namespace, 'purchase_price', 'bot_tracked_qty')
    clear_position_tracking(namespace)
    set_strategy_snapshot(namespace, mode='BUY', setup=setup)


def sanitize_position_state(namespace, actual_sol_balance, source_label):
    position_active = ns_get(namespace, 'position_active') == 'true'
    purchase_price = read_float_state(namespace, 'purchase_price')
    tracked_qty = read_float_state(namespace, 'bot_tracked_qty')

    if actual_sol_balance <= MIN_POSITION_SOL:
        if position_active or tracked_qty > 0 or purchase_price > 0:
            reset_position_state(namespace, setup='No sellable SOL detected')
            log_activity(f'{source_label}: cleared stale position state because sellable SOL is below minimum.', namespace=namespace)
        return 0.0, False

    repaired = False
    if not position_active:
        ns_set(namespace, 'position_active', 'true')
        repaired = True
    if tracked_qty <= MIN_POSITION_SOL or tracked_qty > actual_sol_balance:
        ns_set(namespace, 'bot_tracked_qty', round(actual_sol_balance, 4))
        repaired = True
    if repaired:
        log_activity(f'{source_label}: repaired tracked position size to {actual_sol_balance:.4f} SOL.', namespace=namespace)

    return math.floor(actual_sol_balance * 100) / 100.0, True


def build_status_feed_item(namespace, now=None):
    now = now or time.time()
    bucket_ts = int(now // RSS_STATUS_INTERVAL) * RSS_STATUS_INTERVAL
    trade_count = read_int_state(namespace, 'trade_count')
    bot_running = ns_get(namespace, 'bot_running') == 'true'
    position_active = ns_get(namespace, 'position_active') == 'true'
    engine_status = read_text_state(namespace, 'engine_status', 'Initializing...')
    current_price = read_float_state(namespace, 'current_sol_price')
    usdt_bal = read_float_state(namespace, 'balance_usdt')
    sol_bal = read_float_state(namespace, 'balance_sol')
    purchase_price = read_float_state(namespace, 'purchase_price')
    running_label = 'RUNNING' if bot_running else 'PAUSED'

    if position_active and purchase_price > 0:
        target_price = purchase_price * SELL_TARGET_MULTIPLIER
        profit_pct = pct_change(purchase_price, current_price)
        progress_pct = max(0.0, min(100.0, (profit_pct / SELL_TARGET_PCT) * 100))
        remaining_pct = max(0.0, SELL_TARGET_PCT - profit_pct)
        remaining_usd = max(0.0, target_price - current_price)
        title = '\n'.join([
            f'{running_label} | SELL | SOL ${current_price:.2f}',
            f'Progress: {profit_pct:+.2f}% / +{SELL_TARGET_PCT:.2f}% ({progress_pct:.0f}%)',
            f'Entry ${purchase_price:.2f} -> Target ${target_price:.2f}',
            f'To go: {remaining_pct:.2f}% / ${remaining_usd:.2f} | Trades: {trade_count}'
        ])
    else:
        title = '\n'.join([
            f'{running_label} | BUY | SOL ${current_price:.2f}',
            'Scalp setup: 90s flush then bounce',
            f'Arm {DROP_TRIGGER_PCT:.2f}% | Buy bounce +{BOUNCE_TRIGGER_PCT:.2f}% | Trades: {trade_count}',
            f'USDT ${usdt_bal:.2f}'
        ])
    description_parts = [
        title,
        f'Status: {engine_status}',
        f'SOL held: {sol_bal:.4f}',
        f'USDT: ${usdt_bal:.2f}'
    ]
    latest_logs = ns_lrange(namespace, 'bot_logs', 0, 0)
    if latest_logs:
        description_parts.append(f'Latest log: {latest_logs[0]}')
    return {
        'bucket_ts': bucket_ts,
        'pub_date': formatdate(bucket_ts, usegmt=True),
        'title': title,
        'description': '\n'.join(description_parts),
        'guid': f'muzz-world-status-{namespace}-{bucket_ts}'
    }


def reconcile_live_state(client):
    try:
        usdt_bal = float(client.get_asset_balance(asset='USDT')['free'])
        sol_bal = float(client.get_asset_balance(asset='SOL')['free'])
        ns_set(LIVE_NS, 'balance_usdt', usdt_bal)
        ns_set(LIVE_NS, 'balance_sol', sol_bal)
        open_orders = client.get_open_orders(symbol=SYMBOL)
        if open_orders:
            log_activity('CRITICAL: Open working orders found on exchange. Forcing lockout.', namespace=LIVE_NS)
            ns_set(LIVE_NS, 'engine_status', 'ERROR_LOCKOUT_OPEN_ORDERS')
            return False
        my_trades = client.get_my_trades(symbol=SYMBOL, limit=20)
        if sol_bal >= 0.05:
            accumulated_qty = 0.0
            weighted_cost = 0.0
            for trade in reversed(my_trades):
                if trade['isBuyer']:
                    qty = float(trade['qty'])
                    price = float(trade['price'])
                    accumulated_qty += qty
                    weighted_cost += (price * qty)
                    if accumulated_qty >= (sol_bal * 0.99):
                        break
            if accumulated_qty > 0:
                avg_entry_price = weighted_cost / accumulated_qty
                ns_set(LIVE_NS, 'position_active', 'true')
                ns_set(LIVE_NS, 'purchase_price', avg_entry_price)
                ns_set(LIVE_NS, 'bot_tracked_qty', sol_bal)
                if not ns_get(LIVE_NS, 'position_opened_at'):
                    ns_set(LIVE_NS, 'position_opened_at', time.time())
                return True
        reset_position_state(LIVE_NS, setup='Live account synced and scanning')
        return True
    except Exception as e:
        log_activity(f'Reconciliation Engine Failure: {e}', namespace=LIVE_NS)
        ns_set(LIVE_NS, 'engine_status', 'RECONCILE_ERROR')
        return False


def execute_paper_buy(namespace, price, note='90s flush bounce scalp'):
    usdt_bal = read_float_state(namespace, 'balance_usdt')
    usdt_alloc = usdt_bal * AUTO_BUY_ALLOCATION
    if usdt_alloc < 5.0:
        log_activity('Paper buy aborted: available balance under operational thresholds.', namespace=namespace)
        return
    sol_quantity = math.floor((usdt_alloc / price) * 100) / 100.0
    if sol_quantity <= MIN_POSITION_SOL:
        log_activity('Paper buy aborted: calculated order size too low.', namespace=namespace)
        return
    ns_set(namespace, 'balance_usdt', round(usdt_bal - (sol_quantity * price), 4))
    ns_set(namespace, 'balance_sol', round(read_float_state(namespace, 'balance_sol') + sol_quantity, 4))
    ns_set(namespace, 'position_active', 'true')
    ns_set(namespace, 'purchase_price', price)
    ns_set(namespace, 'bot_tracked_qty', sol_quantity)
    ns_set(namespace, 'position_opened_at', time.time())
    record_trade(namespace, 'BUY', sol_quantity, price, 'PAPER', note)
    log_activity(f'PAPER BUY: {sol_quantity} SOL at ${price:.2f} | {note}', namespace=namespace)


def execute_paper_sell(namespace, price, note):
    sol_bal = read_float_state(namespace, 'balance_sol')
    sol_to_liquidate, valid_position = sanitize_position_state(namespace, sol_bal, 'Paper sell path')
    if not valid_position or sol_to_liquidate <= MIN_POSITION_SOL:
        log_activity('Paper sell aborted: no valid sellable SOL remained after repair.', namespace=namespace)
        return
    ns_set(namespace, 'balance_sol', round(sol_bal - sol_to_liquidate, 4))
    ns_set(namespace, 'balance_usdt', round(read_float_state(namespace, 'balance_usdt') + (sol_to_liquidate * price), 4))
    record_trade(namespace, 'SELL', sol_to_liquidate, price, 'PAPER', note)
    reset_position_state(namespace, setup='Watching for next paper entry')
    log_activity(f'PAPER SELL: {sol_to_liquidate} SOL at ${price:.2f} | {note}', namespace=namespace)


def run_namespaced_trader(namespace, paper=False):
    client = None if paper else get_binance_client()
    if paper:
        bootstrap_sandbox()
    else:
        init_live_state()

    reconciled = paper

    while True:
        try:
            if not paper and not reconciled:
                if reconcile_live_state(client):
                    reconciled = True
                    if ns_get(namespace, 'position_active') == 'true':
                        ns_set(namespace, 'engine_status', 'READY_WITH_POSITION')
                        set_strategy_snapshot(namespace, mode='SELL', setup='Live position synced from exchange')
                    else:
                        ns_set(namespace, 'engine_status', 'READY_IN_CASH')
                        set_strategy_snapshot(namespace, mode='BUY', setup='Live account synced and scanning')
                else:
                    ns_set(namespace, 'engine_status', 'RETRYING_LIVE_SYNC')
                    set_strategy_snapshot(namespace, mode='WAITING', setup='Retrying live account sync')
                    time.sleep(5)
                    continue

            current_status = read_text_state(namespace, 'engine_status')
            if 'LOCKOUT' in current_status:
                time.sleep(SAMPLE_INTERVAL)
                continue

            lock_key = ns_key(namespace, 'trading_execution_lock')
            lock_acquired = redis.set(lock_key, INSTANCE_ID, nx=True, ex=15)
            current_lock = redis.get(lock_key)
            if not lock_acquired and current_lock != INSTANCE_ID:
                time.sleep(SAMPLE_INTERVAL)
                continue
            redis.set(lock_key, INSTANCE_ID, ex=15)

            try:
                current_price = get_live_price()
                ns_set(namespace, 'current_sol_price', current_price)
            except Exception:
                ns_set(namespace, 'engine_status', 'WAITING_FOR_LIVE_PRICE')
                set_strategy_snapshot(namespace, mode='WAITING', setup='Waiting for live websocket price')
                time.sleep(SAMPLE_INTERVAL)
                continue

            if ns_get(namespace, 'bot_running') != 'true':
                ns_set(namespace, 'engine_status', 'PAUSED')
                set_strategy_snapshot(namespace, mode='PAUSED', setup='Trading paused by operator')
                time.sleep(SAMPLE_INTERVAL)
                continue

            position_active = ns_get(namespace, 'position_active') == 'true'
            purchase_price = read_float_state(namespace, 'purchase_price')

            if not paper:
                live_sol_balance = float(client.get_asset_balance(asset='SOL')['free'])
                ns_set(namespace, 'balance_sol', live_sol_balance)
                sanitize_position_state(namespace, live_sol_balance, 'Live sync guard')
                position_active = ns_get(namespace, 'position_active') == 'true'
                purchase_price = read_float_state(namespace, 'purchase_price')
            else:
                sandbox_sol_balance = read_float_state(namespace, 'balance_sol')
                sanitize_position_state(namespace, sandbox_sol_balance, 'Sandbox sync guard')
                position_active = ns_get(namespace, 'position_active') == 'true'
                purchase_price = read_float_state(namespace, 'purchase_price')

            if not position_active:
                now = time.time()
                samples = get_recent_samples(load_price_samples(), SCALP_WINDOW_SECONDS, now=now)
                if len(samples) < 10:
                    ns_set(namespace, 'engine_status', 'WARMING_SCALP_WINDOW')
                    set_strategy_snapshot(
                        namespace,
                        mode='BUY',
                        setup='Warming scalp window',
                        recent_high='0',
                        local_low='0',
                        drop_pct='0',
                        bounce_pct='0'
                    )
                    time.sleep(SAMPLE_INTERVAL)
                    continue

                recent_high = max(sample['price'] for sample in samples)
                high_indexes = [idx for idx, sample in enumerate(samples) if sample['price'] == recent_high]
                high_index = high_indexes[-1] if high_indexes else 0
                post_high_samples = samples[high_index:]
                local_low = min(sample['price'] for sample in post_high_samples)
                drop_pct = pct_change(recent_high, current_price)
                bounce_pct = pct_change(local_low, current_price)
                armed = drop_pct <= DROP_TRIGGER_PCT
                setup = 'Drop armed' if armed else 'Waiting for flush'
                if armed and bounce_pct >= BOUNCE_TRIGGER_PCT and current_price > local_low:
                    setup = 'Bounce confirmed'

                set_strategy_snapshot(
                    namespace,
                    mode='BUY',
                    setup=setup,
                    recent_high=f'{recent_high:.6f}',
                    local_low=f'{local_low:.6f}',
                    drop_pct=f'{drop_pct:.4f}',
                    bounce_pct=f'{bounce_pct:.4f}'
                )
                ns_set(namespace, 'engine_status', 'SCALP_DROP_ARMED' if armed else 'SCANNING_FOR_SCALP_ENTRY')

                if armed and bounce_pct >= BOUNCE_TRIGGER_PCT and current_price > local_low:
                    buy_reason = (
                        f'Drop {drop_pct:+.2f}% from 90s high ${recent_high:.2f}; '
                        f'bounce {bounce_pct:+.2f}% from local low ${local_low:.2f}'
                    )
                    if paper:
                        execute_paper_buy(namespace, current_price, buy_reason)
                    else:
                        usdt_bal = float(client.get_asset_balance(asset='USDT')['free'])
                        ns_set(namespace, 'balance_usdt', usdt_bal)
                        usdt_alloc = usdt_bal * AUTO_BUY_ALLOCATION
                        if usdt_alloc >= 5.0:
                            sol_quantity = math.floor((usdt_alloc / current_price) * 100) / 100.0
                            if sol_quantity > MIN_POSITION_SOL:
                                log_activity(
                                    f'AUTO BUY: {sol_quantity} SOL at ${current_price:.2f} | {buy_reason}',
                                    namespace=namespace
                                )
                                client.create_order(
                                    symbol=SYMBOL,
                                    side=Client.SIDE_BUY,
                                    type=Client.ORDER_TYPE_MARKET,
                                    quantity=sol_quantity
                                )
                                record_trade(namespace, 'BUY', sol_quantity, current_price, 'AUTO', buy_reason)
                                ns_set(namespace, 'position_opened_at', time.time())
                                time.sleep(1)
                                reconciled = reconcile_live_state(client)
                            else:
                                log_activity('Auto buy aborted: calculated order size too low.', namespace=namespace)
                        else:
                            log_activity('Auto buy aborted: available balance under operational thresholds.', namespace=namespace)
            else:
                target_price = purchase_price * SELL_TARGET_MULTIPLIER
                stop_price = purchase_price * (1 + (HARD_STOP_PCT / 100))
                profit_pct = pct_change(purchase_price, current_price)
                opened_at = read_float_state(namespace, 'position_opened_at', time.time())
                seconds_open = max(0, int(time.time() - opened_at))
                set_strategy_snapshot(
                    namespace,
                    mode='SELL',
                    setup='Managing open scalp',
                    target_price=f'{target_price:.6f}',
                    stop_price=f'{stop_price:.6f}',
                    profit_pct=f'{profit_pct:.4f}',
                    seconds_open=str(seconds_open)
                )
                ns_set(namespace, 'engine_status', 'MANAGING_SCALP_POSITION')

                sell_note = None
                if current_price >= target_price:
                    sell_note = f'Target hit at +{SELL_TARGET_PCT:.2f}%'
                elif current_price <= stop_price:
                    sell_note = f'Hard stop hit at {HARD_STOP_PCT:.2f}%'

                if sell_note:
                    if paper:
                        execute_paper_sell(namespace, current_price, sell_note)
                    else:
                        sol_bal = float(client.get_asset_balance(asset='SOL')['free'])
                        ns_set(namespace, 'balance_sol', sol_bal)
                        sol_to_liquidate, valid_position = sanitize_position_state(namespace, sol_bal, 'Live sell path')
                        if valid_position and sol_to_liquidate > MIN_POSITION_SOL:
                            log_activity(
                                f'AUTO SELL: {sol_to_liquidate} SOL at ${current_price:.2f} | {sell_note}',
                                namespace=namespace
                            )
                            client.create_order(
                                symbol=SYMBOL,
                                side=Client.SIDE_SELL,
                                type=Client.ORDER_TYPE_MARKET,
                                quantity=sol_to_liquidate
                            )
                            record_trade(namespace, 'SELL', sol_to_liquidate, current_price, 'AUTO', sell_note)
                            time.sleep(1)
                            reconciled = reconcile_live_state(client)
                        else:
                            log_activity('Auto sell aborted: no valid sellable SOL remained after repair.', namespace=namespace)
        except Exception as e:
            log_activity(f'Process Loop Error: {e}', namespace=namespace)
            ns_set(namespace, 'engine_status', 'Loop Error')
            set_strategy_snapshot(namespace, mode='ERROR', setup=str(e))
        time.sleep(SAMPLE_INTERVAL)


init_live_state()
bootstrap_sandbox()
Thread(target=run_price_stream, daemon=True).start()
Thread(target=run_namespaced_trader, args=(LIVE_NS, False), daemon=True).start()
Thread(target=run_namespaced_trader, args=(SANDBOX_NS, True), daemon=True).start()


def build_state_payload(namespace):
    logs = ns_lrange(namespace, 'bot_logs', 0, LOG_HISTORY_LIMIT - 1)
    usdt_bal = read_float_state(namespace, 'balance_usdt')
    sol_bal = read_float_state(namespace, 'balance_sol')
    current_price = read_float_state(namespace, 'current_sol_price')
    trade_count = read_int_state(namespace, 'trade_count')
    purchase_price = read_float_state(namespace, 'purchase_price')
    target_price = purchase_price * SELL_TARGET_MULTIPLIER if purchase_price > 0 else 0.0
    stop_price = purchase_price * (1 + (HARD_STOP_PCT / 100)) if purchase_price > 0 else 0.0
    profit_pct = pct_change(purchase_price, current_price) if purchase_price > 0 else 0.0
    seconds_open = 0
    if purchase_price > 0:
        seconds_open = max(0, int(time.time() - read_float_state(namespace, 'position_opened_at', time.time())))
    websocket_last_seen = read_float_state(GLOBAL_NS, 'websocket_last_seen')
    market_data_age = max(0.0, time.time() - websocket_last_seen) if websocket_last_seen > 0 else 0.0
    total_usd = usdt_bal + (sol_bal * current_price)
    total_gbp = total_usd * 0.78
    try:
        fiat_res = requests.get('https://open.er-api.com/v6/latest/USD', timeout=2)
        if fiat_res.status_code == 200:
            rates = fiat_res.json().get('rates', {})
            total_gbp = total_usd * rates.get('GBP', 0.78)
    except Exception:
        pass
    return {
        'bot_running': ns_get(namespace, 'bot_running') == 'true',
        'position_active': ns_get(namespace, 'position_active') == 'true',
        'purchase_price': str(purchase_price),
        'current_sol_price': str(current_price),
        'balance_usdt': str(usdt_bal),
        'balance_sol': str(sol_bal),
        'total_portfolio_usd': str(round(total_usd, 2)),
        'total_portfolio_gbp': str(round(total_gbp, 2)),
        'trade_count': trade_count,
        'engine_status': read_text_state(namespace, 'engine_status', 'Initializing...'),
        'websocket_status': read_text_state(GLOBAL_NS, 'websocket_status', 'DISCONNECTED'),
        'market_data_age': round(market_data_age, 1),
        'strategy': {
            'mode': read_text_state(namespace, 'strategy_mode'),
            'setup': read_text_state(namespace, 'strategy_setup'),
            'recent_high': read_float_state(namespace, 'strategy_recent_high'),
            'local_low': read_float_state(namespace, 'strategy_local_low'),
            'drop_pct': read_float_state(namespace, 'strategy_drop_pct'),
            'bounce_pct': read_float_state(namespace, 'strategy_bounce_pct'),
            'target_price': target_price,
            'stop_price': stop_price,
            'profit_pct': profit_pct,
            'seconds_open': seconds_open
        },
        'logs': logs
    }


def build_health_payload(namespace):
    health = {'database': 'FAIL', 'proxy_server': 'DIRECT (NO PROXY)', 'binance_api': 'FAIL', 'market_data': 'FAIL', 'errors': []}
    try:
        redis.ping()
        health['database'] = 'OK'
    except Exception as e:
        health['errors'].append(f'Database Error: {str(e)}')
    websocket_status = read_text_state(GLOBAL_NS, 'websocket_status', 'DISCONNECTED')
    websocket_last_seen = read_float_state(GLOBAL_NS, 'websocket_last_seen')
    if websocket_last_seen > 0:
        age = max(0.0, time.time() - websocket_last_seen)
        if age <= WEBSOCKET_STALE_AFTER:
            health['market_data'] = f'OK (WS {age:.1f}s)'
        else:
            health['market_data'] = f'STALE (WS {age:.1f}s)'
    else:
        health['market_data'] = f'WAITING ({websocket_status})'
    if namespace == SANDBOX_NS:
        health['binance_api'] = 'PAPER MODE'
    elif BINANCE_API_KEY and BINANCE_API_SECRET:
        try:
            test_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
            if test_client.get_server_time():
                health['binance_api'] = 'OK'
        except Exception as e:
            health['errors'].append(f'Binance API Handshake Failure: {str(e)}')
    else:
        health['binance_api'] = 'KEYS MISSING'
    return health


def render_dashboard(namespace, page_label=''):
    return render_template(
        'index.html',
        page_label=page_label,
        api_base='/sandbox/api' if namespace == SANDBOX_NS else '/api',
        trades_href='/sandbox/trades' if namespace == SANDBOX_NS else '/trades'
    )


def render_trade_history(namespace, page_label=''):
    return render_template(
        'trade_history.html',
        page_label=page_label,
        api_base='/sandbox/api' if namespace == SANDBOX_NS else '/api',
        dashboard_href='/sandbox' if namespace == SANDBOX_NS else '/'
    )


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('index'))
        error = 'Invalid Credentials.'
    return render_template('login.html', error=error)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/')
def index():
    return render_dashboard(LIVE_NS)


@app.route('/sandbox')
def sandbox_index():
    return render_dashboard(SANDBOX_NS, page_label='SANDBOX')


@app.route('/trades')
def trade_history_page():
    return render_trade_history(LIVE_NS)


@app.route('/sandbox/trades')
def sandbox_trade_history_page():
    return render_trade_history(SANDBOX_NS, page_label='SANDBOX')


@app.route('/feed.xml', methods=['GET'])
def status_feed():
    try:
        item = build_status_feed_item(LIVE_NS)
        now = time.time()
        feed_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>muzz.world bot status</title>
    <link>https://muzz.world/</link>
    <description>SOL trading bot status heartbeat every {RSS_STATUS_INTERVAL} seconds.</description>
    <language>en-gb</language>
    <ttl>1</ttl>
    <lastBuildDate>{formatdate(now, usegmt=True)}</lastBuildDate>
    <item>
      <title>{escape(item['title'])}</title>
      <link>https://muzz.world/</link>
      <guid isPermaLink="false">{escape(item['guid'])}</guid>
      <pubDate>{item['pub_date']}</pubDate>
      <description>{escape(item['description'])}</description>
    </item>
  </channel>
</rss>'''
        response = app.response_class(feed_xml, mimetype='application/rss+xml')
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        return response
    except Exception as e:
        return app.response_class(
            f'<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>muzz.world bot status</title><item><title>Feed error</title><description>{escape(str(e))}</description></item></channel></rss>',
            mimetype='application/rss+xml',
            status=500
        )


@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify(build_health_payload(LIVE_NS))


@app.route('/sandbox/api/health', methods=['GET'])
def sandbox_health_check():
    return jsonify(build_health_payload(SANDBOX_NS))


@app.route('/api/trades', methods=['GET'])
def get_trades():
    return jsonify({'trade_count': read_int_state(LIVE_NS, 'trade_count'), 'trades': load_trade_history(LIVE_NS, 100)})


@app.route('/sandbox/api/trades', methods=['GET'])
def sandbox_get_trades():
    return jsonify({'trade_count': read_int_state(SANDBOX_NS, 'trade_count'), 'trades': load_trade_history(SANDBOX_NS, 100)})


@app.route('/api/state', methods=['GET'])
def get_state():
    return jsonify(build_state_payload(LIVE_NS))


@app.route('/sandbox/api/state', methods=['GET'])
def sandbox_get_state():
    return jsonify(build_state_payload(SANDBOX_NS))


@app.route('/api/toggle', methods=['POST'])
def toggle_bot():
    action = (request.json or {}).get('run', False)
    ns_set(LIVE_NS, 'bot_running', 'true' if action else 'false')
    log_activity(f"Trading State Update: ENGINE {'STARTED' if action else 'PAUSED'}", namespace=LIVE_NS)
    return jsonify({'status': 'success', 'bot_running': action})


@app.route('/sandbox/api/toggle', methods=['POST'])
def sandbox_toggle_bot():
    action = (request.json or {}).get('run', False)
    ns_set(SANDBOX_NS, 'bot_running', 'true' if action else 'false')
    log_activity(f"Sandbox State Update: ENGINE {'STARTED' if action else 'PAUSED'}", namespace=SANDBOX_NS)
    return jsonify({'status': 'success', 'bot_running': action})


@app.route('/api/manual_set', methods=['POST'])
def manual_set_position():
    try:
        data = request.json or {}
        price = data.get('price')
        if price and float(price) > 0:
            ns_set(LIVE_NS, 'position_active', 'true')
            ns_set(LIVE_NS, 'purchase_price', round(float(price), 2))
            ns_set(LIVE_NS, 'position_opened_at', time.time())
            log_activity(f'Manual Track Override: Position set ACTIVE at Entry ${price}', namespace=LIVE_NS)
        else:
            reset_position_state(LIVE_NS, setup='Manual reset to cash mode')
            log_activity('Manual Track Override: Position cleared to INACTIVE', namespace=LIVE_NS)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/sandbox/api/manual_set', methods=['POST'])
def sandbox_manual_set_position():
    try:
        data = request.json or {}
        price = data.get('price')
        if price and float(price) > 0:
            ns_set(SANDBOX_NS, 'position_active', 'true')
            ns_set(SANDBOX_NS, 'purchase_price', round(float(price), 2))
            ns_set(SANDBOX_NS, 'position_opened_at', time.time())
            log_activity(f'Sandbox manual track: position set ACTIVE at Entry ${price}', namespace=SANDBOX_NS)
        else:
            reset_position_state(SANDBOX_NS, setup='Sandbox manual reset to cash mode')
            log_activity('Sandbox manual track: position cleared to INACTIVE', namespace=SANDBOX_NS)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/logs/clear', methods=['POST'])
def clear_logs():
    redis.delete(ns_key(LIVE_NS, 'bot_logs'))
    log_activity('Terminal log wiped by operator.', namespace=LIVE_NS, skip_db=True)
    return jsonify({'status': 'success'})


@app.route('/sandbox/api/logs/clear', methods=['POST'])
def sandbox_clear_logs():
    redis.delete(ns_key(SANDBOX_NS, 'bot_logs'))
    log_activity('Sandbox terminal log wiped by operator.', namespace=SANDBOX_NS, skip_db=True)
    return jsonify({'status': 'success'})


@app.route('/api/manual_buy', methods=['POST'])
def execute_manual_buy():
    try:
        data = request.json or {}
        usdt_amount = min(float(data.get('usdt_amount', 0)), MAX_TRADE_USDT)
        client = get_binance_client()
        current_price = get_live_price()
        sol_quantity = math.floor((usdt_amount / current_price) * 100) / 100.0
        if sol_quantity > MIN_POSITION_SOL:
            log_activity(f'MANUAL BUY: {sol_quantity} SOL at ${current_price:.2f}', namespace=LIVE_NS)
            client.create_order(symbol=SYMBOL, side=Client.SIDE_BUY, type=Client.ORDER_TYPE_MARKET, quantity=sol_quantity)
            record_trade(LIVE_NS, 'BUY', sol_quantity, current_price, 'MANUAL', 'Manual market buy')
            ns_set(LIVE_NS, 'position_opened_at', time.time())
            time.sleep(1)
            reconcile_live_state(client)
            return jsonify({'status': 'success', 'message': f'Successfully purchased {sol_quantity} SOL.'})
        return jsonify({'status': 'error', 'message': 'Calculated order size too low.'}), 400
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/sandbox/api/manual_buy', methods=['POST'])
def sandbox_execute_manual_buy():
    try:
        data = request.json or {}
        usdt_amount = float(data.get('usdt_amount', 0))
        current_price = get_live_price()
        usdt_bal = read_float_state(SANDBOX_NS, 'balance_usdt')
        deploy = min(usdt_amount, usdt_bal)
        sol_quantity = math.floor((deploy / current_price) * 100) / 100.0
        if sol_quantity > MIN_POSITION_SOL:
            ns_set(SANDBOX_NS, 'balance_usdt', round(usdt_bal - (sol_quantity * current_price), 4))
            ns_set(SANDBOX_NS, 'balance_sol', round(read_float_state(SANDBOX_NS, 'balance_sol') + sol_quantity, 4))
            ns_set(SANDBOX_NS, 'position_active', 'true')
            ns_set(SANDBOX_NS, 'purchase_price', current_price)
            ns_set(SANDBOX_NS, 'bot_tracked_qty', sol_quantity)
            ns_set(SANDBOX_NS, 'position_opened_at', time.time())
            record_trade(SANDBOX_NS, 'BUY', sol_quantity, current_price, 'MANUAL_PAPER', 'Manual paper buy')
            log_activity(f'SANDBOX MANUAL BUY: {sol_quantity} SOL at ${current_price:.2f}', namespace=SANDBOX_NS)
            return jsonify({'status': 'success', 'message': f'Paper bought {sol_quantity} SOL.'})
        return jsonify({'status': 'error', 'message': 'Calculated order size too low.'}), 400
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/liquidate', methods=['POST'])
def liquidate_to_usdt():
    try:
        client = get_binance_client()
        current_price = get_live_price()
        sol_bal = float(client.get_asset_balance(asset='SOL')['free'])
        ns_set(LIVE_NS, 'balance_sol', sol_bal)
        sol_to_liquidate, valid_position = sanitize_position_state(LIVE_NS, sol_bal, 'Manual live liquidation')
        if valid_position and sol_to_liquidate > MIN_POSITION_SOL:
            log_activity(f'MANUAL SELL: {sol_to_liquidate} SOL at ${current_price:.2f} | Manual liquidation', namespace=LIVE_NS)
            client.create_order(symbol=SYMBOL, side=Client.SIDE_SELL, type=Client.ORDER_TYPE_MARKET, quantity=sol_to_liquidate)
            record_trade(LIVE_NS, 'SELL', sol_to_liquidate, current_price, 'MANUAL', 'Manual liquidation')
            reset_position_state(LIVE_NS, setup='Manual liquidation complete')
            return jsonify({'status': 'success', 'message': f'Liquidated {sol_to_liquidate} SOL.'})
        return jsonify({'status': 'error', 'message': 'Insufficient SOL to dispatch order.'}), 400
    except Exception as e:
        log_activity(f'Manual Liquidation Error: {e}', namespace=LIVE_NS)
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/sandbox/api/liquidate', methods=['POST'])
def sandbox_liquidate_to_usdt():
    try:
        current_price = get_live_price()
        execute_paper_sell(SANDBOX_NS, current_price, 'Manual paper liquidation')
        return jsonify({'status': 'success', 'message': 'Sandbox position liquidated.'})
    except Exception as e:
        log_activity(f'Sandbox Manual Liquidation Error: {e}', namespace=SANDBOX_NS)
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
