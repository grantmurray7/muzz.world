import os
import time
import uuid
import math
import json
import requests
import queue
from email.utils import formatdate
from html import escape
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
BUY_DIP_FROM_ANCHOR_USD = 0.10
BUY_REBOUND_STEP_USD = 0.01
AUTO_BUY_ALLOCATION = 0.95
SELL_TARGET_MULTIPLIER = 1.0035
PRICE_HISTORY_LIMIT = int(LOOKBACK_SECONDS / SAMPLE_INTERVAL) + 5
MAX_TRADE_USDT = 50.00  # Manual intervention ceiling
TRADE_HISTORY_LIMIT = 200
LOG_HISTORY_LIMIT = 250
RSS_STATUS_INTERVAL = 30
INSTANCE_ID = str(uuid.uuid4())[:8]

# --- Authentication Guard ---
@app.before_request
def require_login():
    allowed_routes = ['login', 'logout', 'static', 'api/health', 'api/manual_buy', 'status_feed']
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
            redis.ltrim('bot_logs', 0, LOG_HISTORY_LIMIT - 1)
        except Exception as e:
            print(f"Logging fail to Redis: {e}")
        finally:
            log_queue.task_done()

Thread(target=redis_log_worker, daemon=True).start()

def record_trade(side, quantity, price, source, note=''):
    try:
        trade_number = int(redis.incr('trade_count'))
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
            'note': note
        }
        redis.lpush('trade_history', json.dumps(trade))
        redis.ltrim('trade_history', 0, TRADE_HISTORY_LIMIT - 1)
        return trade
    except Exception as e:
        log_activity(f"Trade history write failed: {e}")
        return None

# ... existing helper functions unchanged ...

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

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/trades')
def trade_history_page():
    return render_template('trade_history.html')

# ... rest of file unchanged ...
