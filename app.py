import os
import time
import uuid
import math
import json
import requests
import queue
from threading import Thread
from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from binance.client import Client
from upstash_redis import Redis

app = Flask(__name__)

# --- Environment Variables & Config ---
app.secret_key = os.environ.get("FLASK_SECRET_KEY", str(uuid.uuid4()))
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "DefaultPassword123!")
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET")

# Initialize Upstash Redis Client
redis = Redis.from_env()
log_queue = queue.Queue()

# --- Hard Safety Bounds ---
SYMBOL = 'SOLUSDT'
SAMPLE_INTERVAL = 10
SIGNAL_EVALUATION_INTERVAL = 10
LOOKBACK_SECONDS = 120
UPLIFT_THRESHOLD_PCT = 0.02
AUTO_BUY_ALLOCATION = 0.95
SELL_TARGET_MULTIPLIER = 1.006
PRICE_HISTORY_LIMIT = int(LOOKBACK_SECONDS / SAMPLE_INTERVAL) + 5
MAX_TRADE_USDT = 50.00  # Manual intervention ceiling
INSTANCE_ID = str(uuid.uuid4())[:8]

# --- Authentication Guard ---
@app.before_request
def require_login():
    allowed_routes = ['login', 'static', 'api/health', 'api/manual_buy']
    if request.endpoint not in allowed_routes and not session.get('logged_in'):
        return redirect(url_for('login'))

def log_activity(msg, skip_db=False):
    timestamp = time.strftime('%H:%M:%S')
    log_line = f"[{timestamp}] [{INSTANCE_ID}] {msg}"
    print(log_line)
    if not skip_db:
        log_queue.put(log_line)

def redis_log_worker():
    while True:
        log_line = log_queue.get()
        try:
            redis.lpush('bot_logs', log_line)
            redis.ltrim('bot_logs', 0, 99)
        except Exception as e:
            print(f"Logging fail to Redis: {e}")
        finally:
            log_queue.task_done()

Thread(target=redis_log_worker, daemon=True).start()

def get_binance_proxy_url():
    proxy_url = os.environ.get("BINANCE_PROXY")
    if not proxy_url:
        return None

    if proxy_url.startswith("socks5://"):
        proxy_url = proxy_url.replace("socks5://", "socks5h://")
    elif proxy_url.startswith("http://"):
        proxy_url = proxy_url.replace("http://", "socks5h://")

    return proxy_url

def get_binance_requests_params():
    proxy_url = get_binance_proxy_url()
    if not proxy_url:
        return {}

    return {'proxies': {'http': proxy_url, 'https': proxy_url}}

def get_binance_client():
    return Client(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params=get_binance_requests_params())

def record_price_sample(price):
    sample = json.dumps({'ts': time.time(), 'price': price})
    redis.lpush('price_history', sample)
    redis.ltrim('price_history', 0, PRICE_HISTORY_LIMIT - 1)

def load_price_samples():
    raw_samples = redis.lrange('price_history', 0, PRICE_HISTORY_LIMIT - 1)
    parsed_samples = []

    for raw_sample in reversed(raw_samples):
        try:
            sample = json.loads(raw_sample)
            parsed_samples.append({'ts': float(sample['ts']), 'price': float(sample['price'])})
        except Exception:
            try:
                parsed_samples.append({'ts': 0.0, 'price': float(raw_sample)})
            except Exception:
                continue

    return parsed_samples

def find_anchor_price(samples, anchor_ts):
    anchor_price = None

    for sample in samples:
        if sample['ts'] <= anchor_ts:
            anchor_price = sample['price']
        else:
            break

    return anchor_price

def pct_change(from_price, to_price):
    if from_price <= 0:
        return 0.0
    return ((to_price - from_price) / from_price) * 100

# --- Truth Reconciliation Engine ---
def reconcile_state_against_binance(client):
    log_activity("Running core truth reconciliation step...")
    try:
        redis.delete('price_history')
        
        usdt_bal = float(client.get_asset_balance(asset='USDT')['free'])
        sol_bal = float(client.get_asset_balance(asset='SOL')['free'])
        
        redis.set('balance_usdt', str(usdt_bal))
        redis.set('balance_sol', str(sol_bal))
        
        open_orders = client.get_open_orders(symbol=SYMBOL)
        if open_orders:
            log_activity("CRITICAL: Open working orders found on exchange! Forcing lockout.")
            redis.set('engine_status', 'ERROR_LOCKOUT_OPEN_ORDERS')
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
                log_activity(f"Reconciliation: Active position verified. Reconstructed Cost Basis: ${avg_entry_price:.2f}, Size: {sol_bal}")
                redis.set('position_active', 'true')
                redis.set('purchase_price', str(avg_entry_price))
                redis.set('bot_tracked_qty', str(sol_bal))
                return True
                
        log_activity("Reconciliation: No active bot holdings detected. Set to Cash Mode.")
        redis.set('position_active', 'false')
        redis.delete('purchase_price')
        redis.delete('bot_tracked_qty')
        return True
        
    except Exception as e:
        log_activity(f"Reconciliation Engine Failure: {e}")
        redis.set('engine_status', "Reconcile Error")
        return False

# --- Autonomous Strategy Background Loop ---
def run_trading_bot():
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        print("API keys missing from environment profiles.")
        return

    client = get_binance_client()
    
    if not reconcile_state_against_binance(client):
        log_activity("Initial truth sync failed. Thread loop aborted for account safety.")
        return

    last_logged_thought = ""
    last_signal_check = 0.0

    while True:
        try:
            current_status = redis.get('engine_status') or ""
            if "LOCKOUT" in current_status:
                time.sleep(SAMPLE_INTERVAL)
                continue

            lock_acquired = redis.set('trading_execution_lock', INSTANCE_ID, nx=True, ex=15)
            if not lock_acquired and redis.get('trading_execution_lock') != INSTANCE_ID:
                time.sleep(SAMPLE_INTERVAL)
                continue
                
            redis.set('trading_execution_lock', INSTANCE_ID, ex=15)
            ticker = client.get_symbol_ticker(symbol=SYMBOL)
            current_price = float(ticker['price'])
            redis.set('current_sol_price', str(current_price))
            record_price_sample(current_price)

            if redis.get('bot_running') != 'true':
                redis.set('engine_status', 'PAUSED')
                log_activity("Trading paused. Loop idling.")
                last_logged_thought = "Trading paused. Loop idling."
                time.sleep(SAMPLE_INTERVAL)
                continue

            position_active = redis.get('position_active') == 'true'
            purchase_price = float(redis.get('purchase_price') or 0.0)
            bot_tracked_qty = float(redis.get('bot_tracked_qty') or 0.0)

            if not position_active:
                samples = load_price_samples()
                now = time.time()
                anchor_price = find_anchor_price(samples, now - LOOKBACK_SECONDS)

                if (now - last_signal_check) < SIGNAL_EVALUATION_INTERVAL:
                    time.sleep(SAMPLE_INTERVAL)
                    continue

                last_signal_check = now
                buy_triggered = False
                thought_msg = f"BUY mode | Spot: ${current_price:.2f}"

                if len(samples) < 4 or anchor_price is None:
                    warm_seconds = min(len(samples) * SAMPLE_INTERVAL, LOOKBACK_SECONDS)
                    thought_msg += f" | Warming 2m price cache ({warm_seconds}/{LOOKBACK_SECONDS}s via 10s polls)..."
                    log_activity(thought_msg)
                    redis.set('engine_status', 'WARMING_2M_CACHE')
                    continue

                latest_prices = [sample['price'] for sample in samples[-4:]]
                uplifts = [
                    pct_change(latest_prices[0], latest_prices[1]),
                    pct_change(latest_prices[1], latest_prices[2]),
                    pct_change(latest_prices[2], latest_prices[3])
                ]
                anchor_delta = pct_change(anchor_price, current_price)
                thought_msg += (
                    f" | 2m Anchor: ${anchor_price:.2f} ({anchor_delta:+.2f}%)"
                    f" | Uplifts: [{uplifts[0]:+.2f}%, {uplifts[1]:+.2f}%, {uplifts[2]:+.2f}%]"
                )

                if all(step >= UPLIFT_THRESHOLD_PCT for step in uplifts) and current_price < anchor_price:
                    buy_triggered = True
                    log_activity(
                        "BUY ALERT: Three consecutive +0.02% uplifts confirmed while price remains below the 2-minute anchor."
                    )
                else:
                    thought_msg += " | Waiting for rebound under anchor."
                
                log_activity(thought_msg)
                last_logged_thought = thought_msg
                redis.set('engine_status', 'MONITORING_RECENT_DIP_REBOUND_REST_10S')

                if buy_triggered:
                    usdt_bal = float(client.get_asset_balance(asset='USDT')['free'])
                    usdt_alloc = usdt_bal * AUTO_BUY_ALLOCATION
                    
                    if usdt_alloc >= 5.0:
                        sol_quantity = math.floor((usdt_alloc / current_price) * 100) / 100.0
                        log_activity(f"EXECUTION: Dispatching Market BUY for {sol_quantity} SOL...")
                        client.create_order(symbol=SYMBOL, side=Client.SIDE_BUY, type=Client.ORDER_TYPE_MARKET, quantity=sol_quantity)
                        time.sleep(1) 
                        reconcile_state_against_binance(client)
                    else:
                        log_activity("Transaction aborted: Available balance under operational thresholds.")

            else:
                target_price = purchase_price * SELL_TARGET_MULTIPLIER
                profit_pct = ((current_price - purchase_price) / purchase_price) * 100
                thought_msg = f"SELL mode | Spot: ${current_price:.2f} | Target: ${target_price:.2f} ({profit_pct:+.2f}% / +0.60%) | Holding."
                
                log_activity(thought_msg)
                last_logged_thought = thought_msg
                redis.set('engine_status', 'HOLDING_FOR_+0.60%_EXIT_REST_10S')

                if current_price >= target_price:
                    sol_bal = float(client.get_asset_balance(asset='SOL')['free'])
                    sol_to_liquidate = math.floor(min(sol_bal, bot_tracked_qty) * 100) / 100.0
                    
                    if sol_to_liquidate > 0.01:
                        log_activity(f"EXECUTION: Target Hit. Market Selling bot-tracked size of {sol_to_liquidate} SOL...")
                        client.create_order(symbol=SYMBOL, side=Client.SIDE_SELL, type=Client.ORDER_TYPE_MARKET, quantity=sol_to_liquidate)
                        time.sleep(1)
                        reconcile_state_against_binance(client)
                    else:
                        log_activity("Sell error: Bot-tracked position parameters empty or invalid.")

        except Exception as e:
            log_activity(f"Process Loop Error: {e}")
            redis.set('engine_status', "Loop Error")

        time.sleep(SAMPLE_INTERVAL)

Thread(target=run_trading_bot, daemon=True).start()

# --- INSTANT INTERVENTION ENDPOINTS ---
@app.route('/api/manual_buy', methods=['POST'])
def execute_manual_buy():
    if not session.get('logged_in'):
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    try:
        data = request.json or {}
        usdt_amount = min(float(data.get('usdt_amount', 0)), MAX_TRADE_USDT)

        client = get_binance_client()
        ticker = client.get_symbol_ticker(symbol=SYMBOL)
        current_price = float(ticker['price'])
        
        sol_quantity = math.floor((usdt_amount / current_price) * 100) / 100.0
        
        if sol_quantity > 0.01:
            log_activity(f"MANUAL INTERVENTION: Executing forced Market BUY for {sol_quantity} SOL...")
            client.create_order(symbol=SYMBOL, side=Client.SIDE_BUY, type=Client.ORDER_TYPE_MARKET, quantity=sol_quantity)
            time.sleep(1)
            reconcile_state_against_binance(client)
            return jsonify({'status': 'success', 'message': f'Successfully purchased {sol_quantity} SOL.'})
        return jsonify({'status': 'error', 'message': 'Calculated order size too low.'}), 400
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    health = {'upstash_redis': 'FAIL', 'proxy_server': 'FAIL', 'binance_api': 'FAIL', 'market_data': 'FAIL', 'errors': []}
    try:
        redis.ping()
        health['upstash_redis'] = 'OK'
    except Exception as e:
        health['errors'].append(f"Redis Error: {str(e)}")

    proxy_url = os.environ.get("BINANCE_PROXY")
    requests_params = get_binance_requests_params()
    proxies = requests_params.get('proxies')
    if proxy_url:
        try:
            res = requests.get('https://api.ipify.org?format=json', proxies=proxies, timeout=5)
            if res.status_code == 200:
                health['proxy_server'] = f"OK ({res.json().get('ip')})"
        except Exception as e:
            health['errors'].append(f"SOCKS5 Routing Failure: {str(e)}")
    else:
        health['proxy_server'] = 'NOT SET'

    if BINANCE_API_KEY and BINANCE_API_SECRET:
        try:
            test_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params=requests_params)
            if test_client.get_server_time():
                health['binance_api'] = 'OK'
        except Exception as e:
            health['errors'].append(f"Binance API Handshake Failure: {str(e)}")
    else:
        health['binance_api'] = 'KEYS MISSING'
    health['market_data'] = f"OK (REST {SAMPLE_INTERVAL}s)"

    return jsonify(health)

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('index'))
        else:
            error = "Invalid Credentials."
    return render_template('login.html', error=error)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/state', methods=['GET'])
def get_state():
    try:
        logs = redis.lrange('bot_logs', 0, 20)
        usdt_bal = float(redis.get('balance_usdt') or 0.0)
        sol_bal = float(redis.get('balance_sol') or 0.0)
        current_price = float(redis.get('current_sol_price') or 0.0)
        
        total_usd = usdt_bal + (sol_bal * current_price)
        total_gbp = total_usd * 0.78  
        try:
            fiat_res = requests.get('https://open.er-api.com/v6/latest/USD', timeout=2)
            if fiat_res.status_code == 200:
                rates = fiat_res.json().get('rates', {})
                total_gbp = total_usd * rates.get('GBP', 0.78)
        except Exception:
            pass

        return jsonify({
            'bot_running': redis.get('bot_running') == 'true',
            'position_active': redis.get('position_active') == 'true',
            'purchase_price': redis.get('purchase_price') or '0.00',
            'current_sol_price': str(current_price),
            'balance_usdt': str(usdt_bal),
            'balance_sol': str(sol_bal),
            'total_portfolio_usd': str(round(total_usd, 2)),
            'total_portfolio_gbp': str(round(total_gbp, 2)),
            'engine_status': redis.get('engine_status') or 'Initializing...',
            'logs': logs
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/toggle', methods=['POST'])
def toggle_bot():
    try:
        data = request.json
        action = data.get('run', False)
        redis.set('bot_running', 'true' if action else 'false')
        log_activity(f"Trading State Update: ENGINE {'STARTED' if action else 'PAUSED'}")
        return jsonify({'status': 'success', 'bot_running': action})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/manual_set', methods=['POST'])
def manual_set_position():
    try:
        data = request.json
        price = data.get('price')
        if price and float(price) > 0:
            redis.set('position_active', 'true')
            redis.set('purchase_price', str(round(float(price), 2)))
            log_activity(f"Manual Track Override: Position set ACTIVE at Entry ${price}")
            return jsonify({'status': 'success'})
        else:
            redis.set('position_active', 'false')
            redis.delete('purchase_price')
            log_activity("Manual Track Override: Position cleared to INACTIVE")
            return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/logs/clear', methods=['POST'])
def clear_logs():
    try:
        redis.delete('bot_logs')
        log_activity("Terminal log wiped by operator.")
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/liquidate', methods=['POST'])
def liquidate_to_usdt():
    try:
        client = get_binance_client()
        sol_bal = float(client.get_asset_balance(asset='SOL')['free'])
        bot_tracked_qty = float(redis.get('bot_tracked_qty') or 0.0)

        sol_to_liquidate = math.floor(min(sol_bal, bot_tracked_qty if bot_tracked_qty > 0 else sol_bal) * 100) / 100.0
        
        if sol_to_liquidate > 0.01:
            log_activity(f"MANUAL OVERRIDE LIQUIDATION: Market selling {sol_to_liquidate} SOL.")
            client.create_order(symbol=SYMBOL, side=Client.SIDE_SELL, type=Client.ORDER_TYPE_MARKET, quantity=sol_to_liquidate)
            
            redis.set('position_active', 'false')
            redis.delete('purchase_price')
            redis.delete('bot_tracked_qty')
            return jsonify({'status': 'success', 'message': f'Liquidated {sol_to_liquidate} SOL.'})
        else:
            return jsonify({'status': 'error', 'message': 'Insufficient SOL to dispatch order.'}), 400
            
    except Exception as e:
        log_activity(f"Manual Liquidation Error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
