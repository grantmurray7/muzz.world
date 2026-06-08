import os
import time
import uuid
import base64
import requests
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

# Initialize Upstash Redis Client synchronously from environment variables
redis = Redis.from_env()

# Trading Target Setup
SYMBOL = 'SOLUSDT'
INTERVAL = 10  # strict 10-second ticker execution

# --- Authentication Guard ---
@app.before_request
def require_login():
    allowed_routes = ['login', 'static', 'api/health']
    if request.endpoint not in allowed_routes and not session.get('logged_in'):
        return redirect(url_for('login'))

# --- System Logging Utility ---
def log_activity(msg):
    timestamp = time.strftime('%H:%M:%S')
    log_line = f"[{timestamp}] {msg}"
    print(log_line)
    try:
        redis.lpush('bot_logs', log_line)
        redis.ltrim('bot_logs', 0, 99)
    except Exception as e:
        print(f"Logging fail to Redis: {e}")

# --- Autonomous Strategy Background Loop ---
def run_trading_bot():
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        print("API keys not set. The background execution loop has halted.")
        return

    proxy_url = os.environ.get("BINANCE_PROXY")
    
    requests_params = {}
    if proxy_url:
        requests_params['proxies'] = {
            'http': proxy_url,
            'https': proxy_url
        }
        # Force manual authentication injection to fix 407 errors
        try:
            if "@" in proxy_url:
                creds = proxy_url.split("//")[1].split("@")[0]
                encoded_creds = base64.b64encode(creds.encode()).decode()
                requests_params['headers'] = {
                    'Proxy-Authorization': f'Basic {encoded_creds}'
                }
            print(f"Proxy config detected with header injection: {proxy_url}")
        except Exception as e:
            print(f"Failed parsing proxy credentials: {e}")
    else:
        print("WARNING: No BINANCE_PROXY environment variable set.")

    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params=requests_params)
    log_activity("Core engine initiated with proxy configurations. Monitoring SOL/USDT at 10s intervals.")

    while True:
        try:
            bot_running = redis.get('bot_running') == 'true'
            
            try:
                usdt_bal = client.get_asset_balance(asset='USDT')['free']
                sol_bal = client.get_asset_balance(asset='SOL')['free']
                redis.set('balance_usdt', str(usdt_bal))
                redis.set('balance_sol', str(sol_bal))
                redis.set('engine_status', 'Connected & Syncing')
            except Exception as e:
                log_activity(f"Failed pulling asset metrics from Binance: {e}")
                redis.set('engine_status', f"Binance Connect Error")
                time.sleep(INTERVAL)
                continue

            if not bot_running:
                time.sleep(INTERVAL)
                continue

            ticker = client.get_symbol_ticker(symbol=SYMBOL)
            current_price = float(ticker['price'])
            redis.set('current_sol_price', str(current_price))

            position_active = redis.get('position_active') == 'true'
            purchase_price = redis.get('purchase_price')
            purchase_price = float(purchase_price) if purchase_price else 0.0

            raw_history = redis.lrange('price_history', 0, 2)
            price_history = [float(p) for p in raw_history]

            if not position_active:
                buy_triggered = False

                if len(price_history) >= 1:
                    last_price = price_history[0]
                    if (last_price - current_price) / last_price >= 0.01:
                        buy_triggered = True
                        log_activity(f"BUY ALERT: Flash 1% drop detected ({last_price} -> {current_price})")

                if len(price_history) >= 3 and not buy_triggered:
                    step1 = price_history[0]
                    step2 = price_history[1]
                    step3 = price_history[2]
                    
                    if (step3 - step2) / step3 >= 0.0005:
                        if (step2 - step1) / step2 >= 0.0005:
                            if (step1 - current_price) / step1 >= 0.0005:
                                buy_triggered = True
                                log_activity(f"BUY ALERT: 3 consecutive descending steps")

                if buy_triggered:
                    usdt_alloc = float(usdt_bal) * 0.95
                    if usdt_alloc >= 5.0:
                        sol_quantity = round(usdt_alloc / current_price, 2)
                        log_activity(f"Order dispatch: Purchasing SOL...")
                        client.create_order(symbol=SYMBOL, side=Client.SIDE_BUY, type=Client.ORDER_TYPE_MARKET, quantity=sol_quantity)
                        redis.set('position_active', 'true')
                        redis.set('purchase_price', str(current_price))
                    else:
                        log_activity(f"Transaction aborted: Balance below $5 limit.")

                redis.lpush('price_history', str(current_price))
                redis.ltrim('price_history', 0, 2)

            else:
                target_price = purchase_price * 1.0121
                if current_price >= target_price:
                    sol_to_liquidate = round(float(sol_bal), 2)
                    if sol_to_liquidate > 0.01:
                        log_activity(f"Target Hit. Executing Liquidating Sell...")
                        client.create_order(symbol=SYMBOL, side=Client.SIDE_SELL, type=Client.ORDER_TYPE_MARKET, quantity=sol_to_liquidate)
                        redis.set('position_active', 'false')
                        redis.delete('purchase_price')
                    else:
                        log_activity("Sell error: Balances too low.")

        except Exception as e:
            log_activity(f"Process Loop Error: {e}")
            redis.set('engine_status', f"Loop Error")

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
    proxies = {'http': proxy_url, 'https': proxy_url} if proxy_url else None
    
    headers = {}
    if proxy_url and "@" in proxy_url:
        creds = proxy_url.split("//")[1].split("@")[0]
        encoded_creds = base64.b64encode(creds.encode()).decode()
        headers['Proxy-Authorization'] = f'Basic {encoded_creds}'

    if proxy_url:
        try:
            res = requests.get('https://api.ipify.org?format=json', proxies=proxies, headers=headers, timeout=5)
            if res.status_code == 200:
                health['proxy_server'] = f"OK ({res.json().get('ip')})"
        except Exception as e:
            health['errors'].append(f"Proxy Failure: {str(e)}")
    else:
        health['proxy_server'] = 'NOT SET'

    if BINANCE_API_KEY and BINANCE_API_SECRET:
        try:
            test_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params={'proxies': proxies, 'headers': headers} if proxy_url else {})
            if test_client.get_server_time():
                health['binance_api'] = 'OK'
        except Exception as e:
            health['errors'].append(f"Binance Failure: {str(e)}")
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
        return jsonify({
            'bot_running': redis.get('bot_running') == 'true',
            'position_active': redis.get('position_active') == 'true',
            'purchase_price': redis.get('purchase_price') or '0.00',
            'current_sol_price': redis.get('current_sol_price') or '0.00',
            'balance_usdt': redis.get('balance_usdt') or '0.00',
            'balance_sol': redis.get('balance_sol') or '0.00',
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
        log_activity(f"System State Altered: SYSTEM {'ON' if action else 'PAUSED'}.")
        return jsonify({'status': 'success', 'bot_running': action})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
