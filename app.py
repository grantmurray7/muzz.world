import os
import time
import uuid
import math
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
INTERVAL = 10
MAX_TRADE_USDT = 50.00  # Hard ceiling per single trade allocation
INSTANCE_ID = str(uuid.uuid4())[:8]

# --- Authentication Guard ---
@app.before_request
def require_login():
    allowed_routes = ['login', 'static', 'api/health']
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

# --- Truth Reconciliation Engine ---
def reconcile_state_against_binance(client):
    """Forces reality check against Binance data before executing logic."""
    log_activity("Running core truth reconciliation step...")
    try:
        # 1. Fetch exact wallet values
        usdt_bal = float(client.get_asset_balance(asset='USDT')['free'])
        sol_bal = float(client.get_asset_balance(asset='SOL')['free'])
        
        redis.set('balance_usdt', str(usdt_bal))
        redis.set('balance_sol', str(sol_bal))
        
        # 2. Check for active open working orders
        open_orders = client.get_open_orders(symbol=SYMBOL)
        if open_orders:
            log_activity(f"CRITICAL: Open working orders found on exchange! Forcing error lockout state.")
            redis.set('engine_state', 'ERROR_LOCKOUT')
            return False

        # 3. Pull last trade execution to capture true mathematical entry basis
        my_trades = client.get_my_trades(symbol=SYMBOL, limit=1)
        
        if my_trades:
            last_trade = my_trades[0]
            is_buyer = last_trade['isBuyer']
            exec_price = float(last_trade['price'])
            exec_qty = float(last_trade['qty'])
            
            # If our last action was a BUY and we still hold at least that much SOL
            if is_buyer and sol_bal >= (exec_qty * 0.99):
                log_activity(f"Reconciliation: Active position verified. Entry basis match: ${exec_price}")
                redis.set('position_active', 'true')
                redis.set('purchase_price', str(exec_price))
                redis.set('bot_tracked_qty', str(exec_qty))
                return True
                
        # Default fallback to cash tracking mode
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

    proxy_url = os.environ.get("BINANCE_PROXY")
    requests_params = {}
    if proxy_url:
        if proxy_url.startswith("socks5://"):
            proxy_url = proxy_url.replace("socks5://", "socks5h://")
        elif proxy_url.startswith("http://"):
            proxy_url = proxy_url.replace("http://", "socks5h://")
        requests_params['proxies'] = {'http': proxy_url, 'https': proxy_url}

    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params=requests_params)
    
    # Run absolute validation on startup/redeploy instance instantiation
    if not reconcile_state_against_binance(client):
        log_activity("Initial truth sync failed. Thread loop aborted for account safety.")
        return

    last_logged_thought = ""

    while True:
        try:
            # --- LOCKING SYSTEM: Prevent multiple container workers executing trades simultaneously ---
            # Set a 15-second expiration lock unique to this thread instance
            lock_acquired = redis.set('trading_execution_lock', INSTANCE_ID, nx=True, ex=15)
            if not lock_acquired and redis.get('trading_execution_lock') != INSTANCE_ID:
                print(f"[{time.strftime('%H:%M:%S')}] [{INSTANCE_ID}] Standby: Secondary engine worker detected. Idling execution loop.")
                time.sleep(INTERVAL)
                continue
                
            # Refresh lock lease to secure execution runtime window
            redis.set('trading_execution_lock', INSTANCE_ID, ex=15)

            if redis.get('bot_running') != 'true':
                log_activity("Trading paused. Loop idling.", skip_db=True)
                time.sleep(INTERVAL)
                continue

            ticker = client.get_symbol_ticker(symbol=SYMBOL)
            current_price = float(ticker['price'])
            redis.set('current_sol_price', str(current_price))

            position_active = redis.get('position_active') == 'true'
            purchase_price = float(redis.get('purchase_price') or 0.0)
            bot_tracked_qty = float(redis.get('bot_tracked_qty') or 0.0)

            raw_history = redis.lrange('price_history', 0, 2)
            price_history = [float(p) for p in raw_history]

            if not position_active:
                buy_triggered = False
                thought_msg = f"BUY mode | Spot: ${current_price:.2f}"

                if len(price_history) >= 1:
                    last_price = price_history[0]
                    drop_pct = ((last_price - current_price) / last_price) * 100
                    thought_msg += f" | Last: ${last_price:.2f} ({drop_pct:+.2f}%)"
                    
                    if drop_pct >= 1.0:
                        buy_triggered = True
                        log_activity(f"BUY ALERT: Flash 1% drop detected ({last_price} -> {current_price})")

                if len(price_history) >= 3 and not buy_triggered:
                    step1 = price_history[0]
                    step2 = price_history[1]
                    step3 = price_history[2]
                    
                    d1 = ((step3 - step2) / step3) * 100
                    d2 = ((step2 - step1) / step2) * 100
                    d3 = ((step1 - current_price) / step1) * 100
                    
                    thought_msg += f" | Cascade Dips: [{d1:+.2f}%, {d2:+.2f}%, {d3:+.2f}%]"
                    
                    if d1 >= 0.20 and d2 >= 0.20 and d3 >= 0.20:
                        buy_triggered = True
                        log_activity(f"BUY ALERT: 3-Step Cascade Confirmed (All steps >= 0.20%)")

                if not buy_triggered:
                    thought_msg += " | Conditions split. Holding."
                
                skip_write = (thought_msg == last_logged_thought)
                log_activity(thought_msg, skip_db=skip_write)
                last_logged_thought = thought_msg

                if buy_triggered:
                    usdt_bal = float(client.get_asset_balance(asset='USDT')['free'])
                    # Strict safety parameter checks: Max configuration floor caps
                    usdt_alloc = min(usdt_bal * 0.95, MAX_TRADE_USDT)
                    
                    if usdt_alloc >= 5.0:
                        sol_quantity = math.floor((usdt_alloc / current_price) * 100) / 100.0
                        log_activity(f"EXECUTION: Dispatching Market BUY for {sol_quantity} SOL...")
                        
                        # Dispatch order and immediately extract exact fill weights
                        order = client.create_order(symbol=SYMBOL, side=Client.SIDE_BUY, type=Client.ORDER_TYPE_MARKET, quantity=sol_quantity)
                        time.sleep(1) # Quick wait for fill execution clarity
                        
                        # Re-verify fill states completely
                        reconcile_state_against_binance(client)
                    else:
                        log_activity("Transaction aborted: Available balance under operational thresholds.")

                redis.lpush('price_history', str(current_price))
                redis.ltrim('price_history', 0, 2)

            else:
                target_price = purchase_price * 1.0121
                profit_pct = ((current_price - purchase_price) / purchase_price) * 100
                thought_msg = f"SELL mode | Spot: ${current_price:.2f} | Target: ${target_price:.2f} ({profit_pct:+.2f}% / +1.21%) | Holding."
                
                skip_write = (thought_msg == last_logged_thought)
                log_activity(thought_msg, skip_db=skip_write)
                last_logged_thought = thought_msg

                if current_price >= target_price:
                    sol_bal = float(client.get_asset_balance(asset='SOL')['free'])
                    # CRITICAL FIX: Only liquidate the specific quantity the bot bought, never your global wallet balance
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

        time.sleep(INTERVAL)

Thread(target=run_trading_bot, daemon=True).start()

# --- DIAGNOSTIC HEALTH CHECK ENDPOINT ---
@app.route('/api/health', methods=['GET'])
def health_check():
    health = {'upstash_redis': 'FAIL', 'proxy_server': 'FAIL', 'binance_api': 'FAIL', 'errors': []}
    try:
        redis.ping()
        health['upstash_redis'] = 'OK'
    except Exception as e:
        health['errors'].append(f"Redis Error: {str(e)}")

    proxy_url = os.environ.get("BINANCE_PROXY")
    if proxy_url:
        if proxy_url.startswith("socks5://"):
            proxy_url = proxy_url.replace("socks5://", "socks5h://")
        elif proxy_url.startswith("http://"):
            proxy_url = proxy_url.replace("http://", "socks5h://")
        proxies = {'http': proxy_url, 'https': proxy_url}
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
            test_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params={'proxies': proxies} if proxy_url else {})
            if test_client.get_server_time():
                health['binance_api'] = 'OK'
        except Exception as e:
            health['errors'].append(f"Binance API Handshake Failure: {str(e)}")
    else:
        health['binance_api'] = 'KEYS MISSING'

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

@app.route('/api/liquidate', methods=['POST'])
def liquidate_to_usdt():
    try:
        proxy_url = os.environ.get("BINANCE_PROXY")
        requests_params = {}
        if proxy_url:
            if proxy_url.startswith("socks5://"):
                proxy_url = proxy_url.replace("socks5://", "socks5h://")
            elif proxy_url.startswith("http://"):
                proxy_url = proxy_url.replace("http://", "socks5h://")
            requests_params['proxies'] = {'http': proxy_url, 'https': proxy_url}

        client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params=requests_params)
        sol_bal = float(client.get_asset_balance(asset='SOL')['free'])
        bot_tracked_qty = float(redis.get('bot_tracked_qty') or 0.0)

        # Safety catch: limit liquidation to bot-owned size unless forced
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
