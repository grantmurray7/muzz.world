#!/usr/bin/env python3
import csv
import json
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
PROMPT = """I am trading on the Hyperliquid BTC Perpetual market using Taker orders and my rates a 0.015% and 0.015% each way, so looking to clear 0.03% on any trade to make profit.

Based on fresh market data, recent news, price action, momentum, volatility, and market structure, choose the single best directional trade for the next 15 minutes. Prefer LONG or SHORT whenever one direction appears to have a positive expected edge over the next 15 minutes.

Prioritize BTC price action and immediate market structure e.g. last 1h BTC price action, last 15m and 5m momentum.
Prioritize current BTC price action, momentum, and market structure over commentary. Only use recent, high-quality news sources. Ignore stale articles, evergreen explainers, and low-quality blog spam. Prefer sources from the last 6 hours unless an older event is still clearly driving BTC today.

Use NO_TRADE only when:
- Neither LONG nor SHORT appears likely to achieve +0.03% net profit.
- The directional edge is too small to overcome costs.
- News risk, event risk, or abnormal volatility makes short-term direction genuinely unclear.

Do not default to NO_TRADE simply because confidence is below 100%. If one direction has a measurable advantage, choose it.

Return valid JSON only with this exact shape:
{"signal":"LONG|SHORT|NO_TRADE","why":"1-3 short sentences","sources":["up to 3 short source strings, freshest first"]}"""

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
                "balance",
                "response_time_s",
                "input_tokens",
                "output_tokens",
                "total_tokens",
                "estimated_token_cost_usd",
                "answer_preview",
                "error",
            ],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def build_row(provider, model, balance, elapsed, input_tokens, output_tokens, total_tokens, answer, error_text):
    return {
        "timestamp_utc": iso_now(),
        "provider": provider,
        "model": model,
        "balance": balance,
        "response_time_s": f"{elapsed:.3f}",
        "input_tokens": input_tokens or "",
        "output_tokens": output_tokens or "",
        "total_tokens": total_tokens or "",
        "estimated_token_cost_usd": estimate_token_cost(provider, model, int(input_tokens or 0), int(output_tokens or 0)),
        "answer_preview": (answer[:217] + "...") if len(answer) > 220 else answer,
        "error": error_text,
    }


def test_openai(settings):
    api_key = settings.get("OPENAI_API_KEY", "").strip()
    model = settings.get("OPENAI_MODEL", "gpt-4.1").strip() or "gpt-4.1"
    if not api_key:
        return None
    balance, _ = fetch_openai_balance(api_key)
    payload = {
        "model": model,
        "input": PROMPT,
        "max_output_tokens": 500,
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
    )


def test_gemini(settings):
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
    payload = {"contents": [{"parts": [{"text": PROMPT}]}]}
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
    )


def test_grok(settings):
    api_key = settings.get("GROK_API_KEY", "").strip()
    model = settings.get("GROK_MODEL", "grok-4-fast").strip() or "grok-4-fast"
    if not api_key:
        return None
    payload = {
        "model": model,
        "input": PROMPT,
        "max_output_tokens": 500,
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
    )


def main():
    settings = load_settings(SETTINGS_PATH)
    if not settings:
        print("settings.txt not found beside this script", file=sys.stderr)
        return 1
    print("Testing Gemini, OpenAI, and Grok from settings.txt")
    print(f"CSV log: {OUTPUT_CSV_PATH}")
    rows = []
    for tester in (test_gemini, test_openai, test_grok):
        row = tester(settings)
        if not row:
            continue
        rows.append(row)
        append_csv_row(row)
        print(
            f"{row['provider']:7} | {row['error'] or 'ok'} | "
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
