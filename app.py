import os
import time
import uuid
import logging
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
    allowed_routes = ['login', 'static']
    if request.endpoint not in allowed_routes and not session.get('logged_in'):
        return redirect(url_for('login'))

# --- System Logging Utility ---
def log_activity(msg):
    """Saves timestamps and logs straight to Redis for frontend retrieval."""
    timestamp = time.strftime('%H:%M:%S')
    log_line = f"[{timestamp}] {msg}"
    print(log_line)  # Output to Render console logs
    try:
        redis.lpush('bot_logs', log_line)
        redis.ltrim('bot_logs', 0, 99)  # Retain only the last 100 entries
    except Exception as e:
        print(f"Logging fail to Redis: {e}")

# --- Autonomous Strategy Background Loop ---
def run_trading_bot():
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        print("API keys not set. The background execution loop has halted.")
        return

    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    log_activity("Core engine initiated. Monitoring SOL/USDT at 10s intervals.")

    while True:
        try:
            # Check Master Toggle status
            bot_running = redis.get('bot_running') == 'true'
            
            # Fetch Current Balance values across assets
            try:
                usdt_bal = client.get_asset_balance(asset='USDT')['free']
                sol_bal = client.get_asset_balance(asset='SOL')['free']
                redis.set('balance_usdt', str(usdt_bal))
                redis.set('balance_sol', str(sol_bal))
            except Exception as e:
                log_activity(f"Failed pulling asset metrics: {e}")
                time.sleep(INTERVAL)
                continue

            if not bot_running:
                time.sleep(INTERVAL)
                continue

            # Fetch Market Ticker Price
            ticker = client.get_symbol_ticker(symbol=SYMBOL)
            current_price = float(ticker['price'])
            redis.set('current_sol_price', str(current_price))

            # Retrieve Trading State Data from Upstash
            position_active = redis.get('position_active') == 'true'
            purchase_price = redis.get('purchase_price')
            purchase_price = float(purchase_price) if purchase_price else 0.0

            raw_history = redis.lrange('price_history', 0, 2)
            price_history = [float(p) for p in raw_history]

            if not position_active:
                # --- BUY PROTOCOL EVALUATION ---
                buy_triggered = False

                if len(price_history) >= 1:
                    # Metric 1: One-step flash drop of 1.0% or greater
                    last_price = price_history[0]
                    if (last_price - current_price) / last_price >= 0.01:
                        buy_triggered = True
                        log_activity(f"BUY ALERT: Flash 1% drop detected ({last_price} -> {current_price})")

                if len(price_history) >= 3 and not buy_triggered:
                    # Metric 2: 3 consecutive descending iterations >= 0.05% drop per step
                    step1 = price_history[0]
                    step2 = price_history[1]
                    step3 = price_history[2]
                    
                    if (step3 - step2) / step3 >= 0.0005:
                        if (step2 - step1) / step2 >= 0.0005:
                            if (step1 - current_price) / step1 >= 0.0005:
                                buy_triggered = True
                                log_activity(f"BUY ALERT: 3 consecutive verified steps descending ({step3} > {step2} > {step1} > {current_price})")

                if buy_triggered:
                    usdt_alloc = float(usdt_bal) * 0.95
                    
                    if usdt_alloc >= 5.0: # Binance operational floor limit check
                        sol_quantity = usdt_alloc / current_price
                        sol_quantity = round(sol_quantity, 2) # Truncate down to exact valid lot step size

                        log_activity(f"Order dispatch: Purchasing SOL with 95% stake (${round(usdt_alloc, 2)} USDT)...")
                        
                        order = client.create_order(
                            symbol=SYMBOL,
                            side=Client.SIDE_BUY,
                            type=Client.ORDER_TYPE_MARKET,
                            quantity=sol_quantity
                        )
                        
                        redis.set('position_active', 'true')
                        redis.set('purchase_price', str(current_price))
                        log_activity(f"EXECUTION SUCCESS: Bought SOL at execution price: {current_price}")
                    else:
                        log_activity(f"Transaction aborted: 95% allocated allocation (${round(usdt_alloc, 2)}) sits below Binance $5 absolute threshold.")

                # Persist Price History Data Arrays to Upstash
                redis.lpush('price_history', str(current_price))
                redis.ltrim('price_history', 0, 2)

            else:
                # --- ACTIONABLE TARGET SELL EVALUATION ---
                # 1.21% gross markup targets 1.0% exact clean net return post maximum standard roundtrip fees.
                target_price = purchase_price * 1.0121
                
                if current_price >= target_price:
                    sol_to_liquidate = round(float(sol_bal), 2)
                    
                    if sol_to_liquidate > 0.01:
                        log_activity(f"Target Hit ({current_price} >= {round(target_price, 2)}). Executing Liquidating Sell for {sol_to_liquidate} SOL...")
                        
                        order = client.create_order(
                            symbol=SYMBOL,
                            side=Client.SIDE_SELL,
                            type=Client.ORDER_TYPE_MARKET,
                            quantity=sol_to_liquidate
                        )
                        
                        redis.set('position_active', 'false')
                        redis.delete('purchase_price')
                        log_activity(f"CYCLE COMPLETE: Liquidated positions at {current_price}. 1% target locked.")
                    else:
                        log_activity("Sell condition flagged error: Available asset balances are too low to form valid transactions.")

        except Exception as e:
            log_activity(f"Process Loop Error Event: {e}")

        time.sleep(INTERVAL)

# Safe launch thread
Thread(target=run_trading_bot, daemon=True).start()

# --- WEB UI SYSTEM ROUTING ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('index'))
        else:
            error = "Access Denied: Invalid Master Key Credentials."
    return render_template('login.html', error=error)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/state', methods=['GET'])
def get_state():
    """State transmission endpoint for dynamic async frontend rendering."""
    try:
        logs = redis.lrange('bot_logs', 0, 20)
        decoded_logs = [log for log in logs]
        
        return jsonify({
            'bot_running': redis.get('bot_running') == 'true',
            'position_active': redis.get('position_active') == 'true',
            'purchase_price': redis.get('purchase_price') or '0.00',
            'current_sol_price': redis.get('current_sol_price') or '0.00',
            'balance_usdt': redis.get('balance_usdt') or '0.00',
            'balance_sol': redis.get('balance_sol') or '0.00',
            'logs': decoded_logs
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/toggle', methods=['POST'])
def toggle_bot():
    """System ignition master switch trigger control."""
    try:
        data = request.json
        action = data.get('run', False)
        if action:
            redis.set('bot_running', 'true')
            log_activity("System State Altered: SYSTEM ON.")
        else:
            redis.set('bot_running', 'false')
            log_activity("System State Altered: SYSTEM PAUSED.")
        return jsonify({'status': 'success', 'bot_running': action})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
