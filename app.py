import os
import time
import uuid
import math
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
        if proxy_url.startswith("socks5://"):
            proxy_url = proxy_url.replace("socks5://", "socks5h://")
        elif proxy_url.startswith("http://"):
            proxy_url = proxy_url.replace("http://", "socks5h://")

        requests_params['proxies'] = {
            'http': proxy_url,
            'https': proxy_url
        }
        print(f"SOCKS5 Configuration loaded. Tunneling traffic through: {proxy_url}")
    else:
        print("WARNING: No BINANCE_PROXY environment variable set.")

    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params=requests_params)
    log_activity("Core engine initiated with SOCKS5 configuration. Monitoring SOL/USDT at 10s intervals.")

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
                log_activity(f"Binance Connect Error: {e}")
                redis.set('engine_status', f"Binance Connect Error")
                time.sleep(INTERVAL)
                continue

            if not bot_running:
                log_activity("Trading paused. Loop idling.")
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
                    
                    if (step3 - step2) / step3 >= 0.0005:
                        if (step2 - step1) / step2 >= 0.0005:
                            if (step1 - current_price) / step1 >= 0.0005:
                                buy_triggered = True
                                log_activity(f"BUY ALERT: 3 consecutive descending steps")

                if not buy_triggered:
                    thought_msg += " | Conditions split. Holding."
                
                log_activity(thought_msg)

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
                profit_pct = ((current_price - purchase_price) / purchase_price) * 100
                
                log_activity(f"SELL mode | Spot: ${current_price:.2f} | Target: ${target_price:.2f} ({profit_pct:+.2f}% / +1.21%) | Holding.")

                if current_price >= target_price:
                    sol_to_liquidate = math.floor(float(sol_bal) * 100) / 100.0
                    if sol_to_liquidate > 0.01:
                        log_activity(f"Target Hit. Executing Liquidating Sell of {sol_to_liquidate} SOL...")
                        client.create_order(symbol=SYMBOL, side=Client.SIDE_SELL, type=Client.ORDER_TYPE_MARKET, quantity=sol_to_liquidate)
                        redis.set('position_active', 'false')
                        redis.delete('purchase_price')
                    else:
                        log_activity("Sell error: Balances too low to pass market minimums.")

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

        sol_to_liquidate = math.floor(sol_bal * 100) / 100.0
        
        if sol_to_liquidate > 0.01:
            log_activity(f"MANUAL OVERRIDE LIQUIDATION: Market selling {sol_to_liquidate} SOL.")
            client.create_order(symbol=SYMBOL, side=Client.SIDE_SELL, type=Client.ORDER_TYPE_MARKET, quantity=sol_to_liquidate)
            
            redis.set('position_active', 'false')
            redis.delete('purchase_price')
            return jsonify({'status': 'success', 'message': f'Liquidated {sol_to_liquidate} SOL.'})
        else:
            return jsonify({'status': 'error', 'message': 'Insufficient SOL to dispatch order.'}), 400
            
    except Exception as e:
        log_activity(f"Manual Liquidation Error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
