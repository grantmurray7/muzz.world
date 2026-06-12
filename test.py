#!/usr/bin/env python3
import csv
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request


SETTINGS_PATH = Path(__file__).with_name("settings.txt")
OUTPUT_CSV_PATH = Path(__file__).with_name("ai_key_test_results.csv")
OPENAI_BALANCE_URL = "https://api.openai.com/dashboard/billing/credit_grants"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
FEAR_GREED_URL = "https://api.alternative.me/fng/?limit=1&format=json"
DEFAULT_MAX_OUTPUT_TOKENS = 1500
FEAR_GREED_LONG_THRESHOLD = 30
FEAR_GREED_SHORT_THRESHOLD = 70
BASE_PROMPT = """I am trading on the Hyperliquid BTC Perpetual market using Taker orders and my rates a 0.015% and 0.015% each way, so looking to clear 0.03% on any trade to make profit.

Based on the fresh market snapshot below, choose the single best directional trade for the next 15 minutes. Prefer LONG or SHORT whenever one direction appears to have a positive expected edge over the next 15 minutes.

Treat the provided Hyperliquid BTC perpetual snapshot as the source of truth for price, momentum, volatility, and order book state. Do not invent prices or levels not present in the snapshot.

Prioritize immediate BTC price action and market structure, especially the last 1h, 15m, and 5m behavior.

Field definitions:
- `ret_pct`: percentage return over that lookback window.
- `rng_pct`: percentage high-low range over that lookback window.
- `pos`: normalized position in range from 0 to 1, where 0 is near the window low and 1 is near the window high.
- `book.bid5` and `book.ask5`: summed top-5 book depth on bids and asks.
- `book.imb`: bid share of top-5 depth. `imb > 0.5` means bid-heavy / buy-side stronger. `imb < 0.5` means ask-heavy / sell-side stronger.

Use NO_TRADE only when:
- Neither LONG nor SHORT appears likely to achieve +0.03% net profit.
- The directional edge is too small to overcome costs.
- Abnormal volatility or mixed structure makes short-term direction genuinely unclear.

Do not default to NO_TRADE simply because confidence is below 100%. If one direction has a measurable advantage, choose it.

Output rules:
- Return raw JSON only. No markdown. No code fences. No prose before or after the JSON.
- Return exactly one JSON object on a single line.
- `signal` must be exactly one of `LONG`, `SHORT`, `NO_TRADE`.
- `why` should be a detailed rationale that uses the snapshot fields directly. Prefer roughly 3-8 sentences, and include the most important supporting and conflicting signals.
- `sources` must be an empty array `[]` unless you actually used a fresh external source.

Return valid JSON only with this exact shape:
{"signal":"LONG|SHORT|NO_TRADE","why":"detailed rationale based on the snapshot fields","sources":["up to 3 short source strings, freshest first"]}"""

PRICING = {
    "openai": {
        "gpt-4.1": (2.00, 8.00),
        "gpt-4.1-mini": (0.40, 1.60),
        "gpt-4.1-nano": (0.10, 0.40),
    },
    "grok": {
        "grok-4-fast": (0.20, 0.50),
        "grok-4.3": (1.25, 2.50),
    },
    "gemini": {},
}


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def load_settings(path):
    settings = {}
    path = Path(path)
    if not path.exists():
        return settings
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        settings[key.strip()] = value.strip()
    return settings


def get_max_output_tokens(settings):
    raw = str(settings.get("AI_MAX_OUTPUT_TOKENS", DEFAULT_MAX_OUTPUT_TOKENS)).strip()
    try:
        value = int(raw)
    except Exception:
        value = DEFAULT_MAX_OUTPUT_TOKENS
    return max(200, value)


def read_json_response(url, payload=None, headers=None, timeout=90):
    request_headers = {"Content-Type": "application/json"}
    if headers:
        request_headers.update(headers)
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib_request.Request(url, data=body, headers=request_headers, method="POST" if body else "GET")
    try:
        with urllib_request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw), raw
    except urllib_error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {raw}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Network error: {exc}") from exc


def clamp(value, low, high):
    return max(low, min(high, value))


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def round_or_none(value, digits=4):
    if value is None:
        return None
    return round(float(value), digits)


def pct_change(from_price, to_price):
    if from_price <= 0:
        return 0.0
    return ((to_price - from_price) / from_price) * 100.0


def compute_window_metrics(candles, count):
    if not candles:
        return {"ret_pct": 0.0, "range_pct": 0.0, "high": 0.0, "low": 0.0}
    window = candles[-count:] if len(candles) >= count else candles[:]
    if not window:
        return {"ret_pct": 0.0, "range_pct": 0.0, "high": 0.0, "low": 0.0}
    first_close = safe_float(window[0]["c"])
    last_close = safe_float(window[-1]["c"])
    high = max(safe_float(item["h"]) for item in window)
    low = min(safe_float(item["l"]) for item in window)
    range_pct = pct_change(low, high) if low > 0 else 0.0
    return {
        "ret_pct": pct_change(first_close, last_close),
        "range_pct": range_pct,
        "high": high,
        "low": low,
    }


def price_position(current_price, low, high):
    if high <= low:
        return 0.5
    return clamp((current_price - low) / (high - low), 0.0, 1.0)


def fetch_hyperliquid_snapshot():
    now_ms = int(time.time() * 1000)
    mids, _ = read_json_response(
        HYPERLIQUID_INFO_URL,
        payload={"type": "allMids"},
        timeout=15,
    )
    book, _ = read_json_response(
        HYPERLIQUID_INFO_URL,
        payload={"type": "l2Book", "coin": "BTC"},
        timeout=15,
    )
    candles_1m, _ = read_json_response(
        HYPERLIQUID_INFO_URL,
        payload={
            "type": "candleSnapshot",
            "req": {
                "coin": "BTC",
                "interval": "1m",
                "startTime": now_ms - (65 * 60 * 1000),
                "endTime": now_ms,
            },
        },
        timeout=20,
    )
    candles_1h, _ = read_json_response(
        HYPERLIQUID_INFO_URL,
        payload={
            "type": "candleSnapshot",
            "req": {
                "coin": "BTC",
                "interval": "1h",
                "startTime": now_ms - (26 * 60 * 60 * 1000),
                "endTime": now_ms,
            },
        },
        timeout=20,
    )

    levels = book.get("levels") or [[], []]
    bids = levels[0] if len(levels) > 0 else []
    asks = levels[1] if len(levels) > 1 else []
    best_bid = safe_float(bids[0]["px"]) if bids else 0.0
    best_ask = safe_float(asks[0]["px"]) if asks else 0.0
    mid = safe_float(mids.get("BTC")) if isinstance(mids, dict) else 0.0
    if mid <= 0 and best_bid > 0 and best_ask > 0:
        mid = (best_bid + best_ask) / 2.0
    spread_bps = (((best_ask - best_bid) / mid) * 10000.0) if mid > 0 and best_ask >= best_bid else 0.0
    bid5 = sum(safe_float(level.get("sz")) for level in bids[:5])
    ask5 = sum(safe_float(level.get("sz")) for level in asks[:5])
    imbalance = (bid5 / (bid5 + ask5)) if (bid5 + ask5) > 0 else 0.5

    metrics_1m = compute_window_metrics(candles_1m, 1)
    metrics_5m = compute_window_metrics(candles_1m, 5)
    metrics_15m = compute_window_metrics(candles_1m, 15)
    metrics_1h = compute_window_metrics(candles_1m, 60)
    metrics_4h = compute_window_metrics(candles_1h, 4)
    metrics_day = compute_window_metrics(candles_1h, 24)

    snapshot = {
        "ts_utc": iso_now(),
        "source": "Hyperliquid BTC perp",
        "px": {
            "mid": round_or_none(mid, 2),
            "bid": round_or_none(best_bid, 2),
            "ask": round_or_none(best_ask, 2),
            "spr_bps": round_or_none(spread_bps, 3),
        },
        "ret_pct": {
            "1m": round_or_none(metrics_1m["ret_pct"]),
            "5m": round_or_none(metrics_5m["ret_pct"]),
            "15m": round_or_none(metrics_15m["ret_pct"]),
            "1h": round_or_none(metrics_1h["ret_pct"]),
            "4h": round_or_none(metrics_4h["ret_pct"]),
        },
        "rng_pct": {
            "1m": round_or_none(metrics_1m["range_pct"]),
            "5m": round_or_none(metrics_5m["range_pct"]),
            "15m": round_or_none(metrics_15m["range_pct"]),
            "1h": round_or_none(metrics_1h["range_pct"]),
            "4h": round_or_none(metrics_4h["range_pct"]),
        },
        "pos": {
            "1h": round_or_none(price_position(mid, metrics_1h["low"], metrics_1h["high"]), 3),
            "4h": round_or_none(price_position(mid, metrics_4h["low"], metrics_4h["high"]), 3),
            "1d": round_or_none(price_position(mid, metrics_day["low"], metrics_day["high"]), 3),
        },
        "book": {
            "bid5": round_or_none(bid5, 4),
            "ask5": round_or_none(ask5, 4),
            "imb": round_or_none(imbalance, 3),
        },
        "levels": {
            "h1_high": round_or_none(metrics_1h["high"], 2),
            "h1_low": round_or_none(metrics_1h["low"], 2),
            "d1_high": round_or_none(metrics_day["high"], 2),
            "d1_low": round_or_none(metrics_day["low"], 2),
        },
    }
    return snapshot


def build_prompt(snapshot):
    return (
        BASE_PROMPT
        + "\n\nFresh market snapshot:\n"
        + json.dumps(snapshot, separators=(",", ":"), ensure_ascii=True)
    )


def fetch_openai_balance(api_key):
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        data, _ = read_json_response(OPENAI_BALANCE_URL, headers=headers, timeout=30)
    except Exception as exc:
        return "", str(exc)
    balance = data.get("total_available")
    if balance is None:
        return "", "balance field missing"
    try:
        return f"{float(balance):.6f}", ""
    except Exception:
        return str(balance), ""


def extract_output_text(response_data):
    top_level = response_data.get("output_text")
    if isinstance(top_level, str) and top_level.strip():
        return top_level.strip()
    parts = []
    for item in response_data.get("output", []) or []:
        for content in item.get("content", []) or []:
            content_type = content.get("type")
            if content_type in {"output_text", "text"}:
                text = content.get("text", "")
                if text:
                    parts.append(str(text))
    return "\n".join(parts).strip()


def estimate_token_cost(provider, model, input_tokens, output_tokens):
    model_pricing = PRICING.get(provider, {}).get((model or "").strip())
    if not model_pricing:
        return ""
    input_price, output_price = model_pricing
    total_cost = ((input_tokens / 1_000_000.0) * input_price) + (
        (output_tokens / 1_000_000.0) * output_price
    )
    return f"{total_cost:.8f}"


def append_csv_row(row):
    file_exists = OUTPUT_CSV_PATH.exists()
    with OUTPUT_CSV_PATH.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "timestamp_utc",
                "provider",
                "model",
                "signal",
                "balance",
                "response_time_s",
                "input_tokens",
                "output_tokens",
                "total_tokens",
                "estimated_token_cost_usd",
                "fear_greed_value",
                "fear_greed_classification",
                "answer_preview",
                "error",
            ],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def reset_csv():
    with OUTPUT_CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "timestamp_utc",
                "provider",
                "model",
                "signal",
                "balance",
                "response_time_s",
                "input_tokens",
                "output_tokens",
                "total_tokens",
                "estimated_token_cost_usd",
                "fear_greed_value",
                "fear_greed_classification",
                "answer_preview",
                "error",
            ],
        )
        writer.writeheader()


def build_row(provider, model, balance, elapsed, input_tokens, output_tokens, total_tokens, answer, error_text, fear_greed):
    return {
        "timestamp_utc": iso_now(),
        "provider": provider,
        "model": model,
        "signal": extract_signal(answer),
        "balance": balance,
        "response_time_s": f"{elapsed:.3f}",
        "input_tokens": input_tokens or "",
        "output_tokens": output_tokens or "",
        "total_tokens": total_tokens or "",
        "estimated_token_cost_usd": estimate_token_cost(provider, model, int(input_tokens or 0), int(output_tokens or 0)),
        "fear_greed_value": fear_greed.get("value", ""),
        "fear_greed_classification": fear_greed.get("classification", ""),
        "answer_preview": answer,
        "error": error_text,
    }


def extract_signal(answer):
    text = str(answer or "").strip()
    if not text:
        return ""
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()
    if "{" in text and "}" in text:
        candidate = text[text.find("{") : text.rfind("}") + 1]
        try:
            parsed = json.loads(candidate)
            signal = str(parsed.get("signal", "")).strip().upper()
            if signal in {"LONG", "SHORT", "NO_TRADE"}:
                return signal
        except Exception:
            pass
    match = re.search(r'"signal"\s*:\s*"?(LONG|SHORT|NO_TRADE)"?', text, flags=re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return ""


def fear_greed_to_signal(value_text):
    try:
        value = int(str(value_text).strip())
    except Exception:
        return ""
    if value <= FEAR_GREED_LONG_THRESHOLD:
        return "LONG"
    if value >= FEAR_GREED_SHORT_THRESHOLD:
        return "SHORT"
    return "NO_TRADE"


def build_fear_greed_row(fear_greed):
    signal = fear_greed_to_signal(fear_greed.get("value", ""))
    value = fear_greed.get("value", "")
    classification = fear_greed.get("classification", "")
    summary = (
        f'{{"signal":"{signal}","why":"Fear & Greed daily sentiment score {value} ({classification}). '
        f'Contrarian mapping: <={FEAR_GREED_LONG_THRESHOLD} => LONG, >={FEAR_GREED_SHORT_THRESHOLD} => SHORT, '
        f'otherwise NO_TRADE.","sources":[]}}'
    )
    return {
        "timestamp_utc": iso_now(),
        "provider": "fear_greed",
        "model": "alternative_me",
        "signal": signal,
        "balance": "n/a",
        "response_time_s": "",
        "input_tokens": "",
        "output_tokens": "",
        "total_tokens": "",
        "estimated_token_cost_usd": "",
        "fear_greed_value": value,
        "fear_greed_classification": classification,
        "answer_preview": summary,
        "error": "",
    }


def test_openai(settings, prompt_text, max_output_tokens, fear_greed):
    api_key = settings.get("OPENAI_API_KEY", "").strip()
    model = settings.get("OPENAI_MODEL", "gpt-4.1").strip() or "gpt-4.1"
    if not api_key:
        return None
    balance, _ = fetch_openai_balance(api_key)
    payload = {
        "model": model,
        "input": prompt_text,
        "max_output_tokens": max_output_tokens,
    }
    started = time.perf_counter()
    error_text = ""
    response_data = None
    try:
        response_data, _ = read_json_response(
            "https://api.openai.com/v1/responses",
            payload=payload,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=120,
        )
    except Exception as exc:
        error_text = str(exc)
    elapsed = time.perf_counter() - started
    usage = (response_data or {}).get("usage") or {}
    return build_row(
        "openai",
        model,
        balance or "n/a",
        elapsed,
        usage.get("input_tokens", ""),
        usage.get("output_tokens", ""),
        usage.get("total_tokens", ""),
        extract_output_text(response_data or {}),
        error_text,
        fear_greed,
    )


def test_gemini(settings, prompt_text, max_output_tokens, fear_greed):
    api_key = settings.get("GEMINI_API_KEY", "").strip()
    model = settings.get("GEMINI_MODEL", "gemini-3.5-flash").strip() or "gemini-3.5-flash"
    if not api_key:
        return None
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        + urllib_parse.quote(model, safe="")
        + ":generateContent?key="
        + urllib_parse.quote(api_key, safe="")
    )
    payload = {
        "contents": [{"parts": [{"text": prompt_text}]}],
        "generationConfig": {"maxOutputTokens": max_output_tokens},
    }
    started = time.perf_counter()
    error_text = ""
    data = None
    try:
        data, _ = read_json_response(url, payload=payload, timeout=120)
    except Exception as exc:
        error_text = str(exc)
    elapsed = time.perf_counter() - started
    usage = (data or {}).get("usageMetadata") or {}
    parts = []
    for candidate in (data or {}).get("candidates", []) or []:
        content = candidate.get("content") or {}
        for part in content.get("parts", []) or []:
            text = part.get("text", "")
            if text:
                parts.append(str(text))
    return build_row(
        "gemini",
        model,
        "n/a",
        elapsed,
        usage.get("promptTokenCount", ""),
        usage.get("candidatesTokenCount", ""),
        usage.get("totalTokenCount", ""),
        "\n".join(parts).strip(),
        error_text,
        fear_greed,
    )


def test_grok(settings, prompt_text, max_output_tokens, fear_greed):
    api_key = settings.get("GROK_API_KEY", "").strip()
    model = settings.get("GROK_MODEL", "grok-4-fast").strip() or "grok-4-fast"
    if not api_key:
        return None
    payload = {
        "model": model,
        "input": prompt_text,
        "max_output_tokens": max_output_tokens,
    }
    started = time.perf_counter()
    error_text = ""
    response_data = None
    try:
        response_data, _ = read_json_response(
            "https://api.x.ai/v1/responses",
            payload=payload,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=120,
        )
    except Exception as exc:
        error_text = str(exc)
    elapsed = time.perf_counter() - started
    usage = (response_data or {}).get("usage") or {}
    input_tokens = usage.get("input_tokens", usage.get("prompt_tokens", ""))
    output_tokens = usage.get("output_tokens", usage.get("completion_tokens", ""))
    total_tokens = usage.get("total_tokens", "")
    return build_row(
        "grok",
        model,
        "n/a",
        elapsed,
        input_tokens,
        output_tokens,
        total_tokens,
        extract_output_text(response_data or {}),
        error_text,
        fear_greed,
    )


def fetch_fear_greed():
    try:
        data, _ = read_json_response(FEAR_GREED_URL, timeout=15)
    except Exception:
        return {"value": "", "classification": ""}
    rows = data.get("data") or []
    if not rows:
        return {"value": "", "classification": ""}
    item = rows[0] or {}
    return {
        "value": str(item.get("value", "")).strip(),
        "classification": str(item.get("value_classification", "")).strip(),
    }


def main():
    settings = load_settings(SETTINGS_PATH)
    if not settings:
        print("settings.txt not found beside this script", file=sys.stderr)
        return 1
    max_output_tokens = get_max_output_tokens(settings)
    reset_csv()
    try:
        snapshot = fetch_hyperliquid_snapshot()
    except Exception as exc:
        print(f"Failed to fetch Hyperliquid snapshot: {exc}", file=sys.stderr)
        return 1
    fear_greed = fetch_fear_greed()
    snapshot["sentiment"] = {
        "fear_greed_value": fear_greed.get("value", ""),
        "fear_greed_classification": fear_greed.get("classification", ""),
        "fear_greed_signal": fear_greed_to_signal(fear_greed.get("value", "")),
    }
    prompt_text = build_prompt(snapshot)
    print("Testing Gemini, OpenAI, and Grok from settings.txt")
    print(f"Hyperliquid mid: {snapshot['px']['mid']} | 5m={snapshot['ret_pct']['5m']}% | 15m={snapshot['ret_pct']['15m']}% | 1h={snapshot['ret_pct']['1h']}%")
    print(
        f"Fear & Greed: {fear_greed.get('value', 'n/a')} {fear_greed.get('classification', '')} | "
        f"signal={fear_greed_to_signal(fear_greed.get('value', '')) or 'n/a'} "
        f"(<= {FEAR_GREED_LONG_THRESHOLD} LONG, >= {FEAR_GREED_SHORT_THRESHOLD} SHORT)"
    )
    print(f"Max output tokens: {max_output_tokens}")
    print(f"CSV log: {OUTPUT_CSV_PATH}")
    rows = []
    fear_greed_row = build_fear_greed_row(fear_greed)
    rows.append(fear_greed_row)
    append_csv_row(fear_greed_row)
    for tester in (test_gemini, test_openai, test_grok):
        row = tester(settings, prompt_text, max_output_tokens, fear_greed)
        if not row:
            continue
        rows.append(row)
        append_csv_row(row)
        print(
            f"{row['provider']:10} | {row['signal'] or 'n/a':9} | {row['error'] or 'ok'} | "
            f"rt={row['response_time_s']}s | "
            f"in={row['input_tokens'] or 'n/a'} | "
            f"out={row['output_tokens'] or 'n/a'} | "
            f"cost={row['estimated_token_cost_usd'] or 'n/a'}"
        )
    if not rows:
        print("No supported API keys found in settings.txt", file=sys.stderr)
        return 1
    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
