#!/usr/bin/env python3
import csv
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request

from lets_fuck import OPENAI_MODEL_DEFAULT, PROMPT, SETTINGS_PATH, load_settings


RESPONSES_URL = "https://api.openai.com/v1/responses"
BALANCE_URL = "https://api.openai.com/dashboard/billing/credit_grants"
OUTPUT_CSV_PATH = Path(__file__).with_name("openai_key_test_results.csv")

# Token-only estimate. This does not include any extra tool/search charges.
MODEL_PRICING_PER_1M = {
    "gpt-4.1": {"input": 2.00, "cached_input": 0.50, "output": 8.00},
    "gpt-4.1-mini": {"input": 0.40, "cached_input": 0.10, "output": 1.60},
    "gpt-4.1-nano": {"input": 0.10, "cached_input": 0.025, "output": 0.40},
    "gpt-4o": {"input": 2.50, "cached_input": 1.25, "output": 10.00},
    "gpt-4o-mini": {"input": 0.15, "cached_input": 0.075, "output": 0.60},
    "gpt-5.4": {"input": 2.50, "cached_input": 0.25, "output": 15.00},
    "gpt-5.4-mini": {"input": 0.75, "cached_input": 0.075, "output": 4.50},
    "gpt-5.4-nano": {"input": 0.20, "cached_input": 0.02, "output": 1.25},
}


def iso_now():
    return datetime.now(timezone.utc).isoformat()


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


def discover_openai_keys(settings):
    found = []
    for key_name in sorted(settings):
        if re.fullmatch(r"OPENAI_API_KEY(?:_\d+|\d+)?", key_name):
            value = settings.get(key_name, "").strip()
            if value:
                found.append((key_name, value))
    return found


def fetch_balance(api_key):
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        data, _ = read_json_response(BALANCE_URL, headers=headers, timeout=30)
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


def estimate_token_cost(model, usage):
    pricing = MODEL_PRICING_PER_1M.get((model or "").strip())
    if not pricing:
        return ""
    usage = usage or {}
    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    details = usage.get("input_tokens_details") or {}
    cached_tokens = int(details.get("cached_tokens") or 0)
    uncached_tokens = max(0, input_tokens - cached_tokens)
    total_cost = (
        (uncached_tokens / 1_000_000.0) * pricing["input"]
        + (cached_tokens / 1_000_000.0) * pricing["cached_input"]
        + (output_tokens / 1_000_000.0) * pricing["output"]
    )
    return f"{total_cost:.8f}"


def append_csv_row(row):
    file_exists = OUTPUT_CSV_PATH.exists()
    with OUTPUT_CSV_PATH.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "timestamp_utc",
                "key_name",
                "model",
                "balance_usd",
                "balance_error",
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


def run_one_key(key_name, api_key, model):
    balance_usd, balance_error = fetch_balance(api_key)
    payload = {
        "model": model,
        "input": PROMPT,
        "tools": [{"type": "web_search_preview"}],
        "max_output_tokens": 10000,
    }
    started = time.perf_counter()
    response_data = None
    error_text = ""
    try:
        response_data, _ = read_json_response(
            RESPONSES_URL,
            payload=payload,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=120,
        )
    except Exception as exc:
        error_text = str(exc)
    elapsed = time.perf_counter() - started
    usage = (response_data or {}).get("usage") or {}
    answer_preview = extract_output_text(response_data or {})
    if len(answer_preview) > 220:
        answer_preview = answer_preview[:217] + "..."
    row = {
        "timestamp_utc": iso_now(),
        "key_name": key_name,
        "model": model,
        "balance_usd": balance_usd,
        "balance_error": balance_error,
        "response_time_s": f"{elapsed:.3f}",
        "input_tokens": usage.get("input_tokens", ""),
        "output_tokens": usage.get("output_tokens", ""),
        "total_tokens": usage.get("total_tokens", ""),
        "estimated_token_cost_usd": estimate_token_cost(model, usage),
        "answer_preview": answer_preview,
        "error": error_text,
    }
    append_csv_row(row)
    return row


def main():
    settings = load_settings(SETTINGS_PATH)
    key_entries = discover_openai_keys(settings)
    if not key_entries:
        print(
            "No OpenAI keys found. Add settings like OPENAI_API_KEY, OPENAI_API_KEY_2, OPENAI_API_KEY_3 to settings.txt.",
            file=sys.stderr,
        )
        return 1
    model = settings.get("OPENAI_MODEL", OPENAI_MODEL_DEFAULT).strip() or OPENAI_MODEL_DEFAULT
    print(f"Testing {len(key_entries)} OpenAI key(s) with model {model}")
    print(f"Prompt source: lets_fuck.PROMPT")
    print(f"CSV log: {OUTPUT_CSV_PATH}")
    for key_name, api_key in key_entries:
        print(f"\n[{key_name}] running...")
        row = run_one_key(key_name, api_key, model)
        status = row["error"] or "ok"
        print(
            f"[{key_name}] {status} | balance={row['balance_usd'] or 'n/a'} "
            f"| rt={row['response_time_s']}s | in={row['input_tokens'] or 'n/a'} "
            f"| out={row['output_tokens'] or 'n/a'} | cost={row['estimated_token_cost_usd'] or 'n/a'}"
        )
    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
