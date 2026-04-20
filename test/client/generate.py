#!/usr/bin/env python3
"""Load-test anchor with synthetic NDJSON events.

Stdlib only — no pip, no venv. Just `python3 test/generate.py`.

Schema matches `test/create_table.sql` (table `demo.events`, partitioned by
`ts`, identifier field `event_id`):
    ts, event_id, event_time, event_date, user_id, event_type, amount,
    session_id, user_agent, country,
    properties: {browser, os, device},
    tags: [string],
    metrics: [{name, value}]

All events in a batch share today's `ts`, so each POST lands in exactly one
partition.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

EVENT_TYPES = ["click", "view", "purchase", "signup", "logout"]
COUNTRIES = ["US", "GB", "DE", "FR", "JP", "BR", "IN", "LT"]
BROWSERS = ["chrome", "firefox", "safari", "edge"]
OSES = ["macos", "windows", "linux", "ios", "android"]
DEVICES = ["desktop", "mobile", "tablet"]
TAG_POOL = ["vip", "beta", "new", "loyal", "promo", "mobile_app", "first_time"]


def today_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def gen_event(ts: str) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "ts": ts,
        "event_id": f"evt-{random.randint(10**9, 10**10 - 1)}",
        "event_time": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z",
        "event_date": now.date().isoformat(),
        "user_id": f"user-{random.randint(1, 10_000)}",
        "event_type": random.choice(EVENT_TYPES),
        "amount": round(random.uniform(0, 500), 2),
        "session_id": f"sess-{random.randint(10**6, 10**7 - 1)}",
        "user_agent": f"Mozilla/5.0 ({random.choice(OSES)})",
        "country": random.choice(COUNTRIES),
        "properties": {
            "browser": random.choice(BROWSERS),
            "os": random.choice(OSES),
            "device": random.choice(DEVICES),
        },
        "tags": random.sample(TAG_POOL, k=random.randint(0, 3)),
        "metrics": [
            {"name": "latency_ms", "value": round(random.uniform(1, 500), 2)},
            {"name": "cpu_pct", "value": round(random.uniform(0, 100), 2)},
        ],
    }


def build_batch(n: int, ts: str) -> bytes:
    return b"\n".join(json.dumps(gen_event(ts)).encode() for _ in range(n))


def post_batch(url: str, body: bytes, timeout: float) -> tuple[int, str]:
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/x-ndjson"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as e:
        return 0, f"connection error: {e.reason}"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--url", default="http://localhost:8080")
    p.add_argument("--namespace", default="demo")
    p.add_argument("--table", default="events")
    p.add_argument("--batch-size", type=int, default=100,
                   help="records per POST (default: 100)")
    p.add_argument("--interval", type=float, default=0.1,
                   help="seconds between batches; 0 = as fast as possible (default: 0.1)")
    p.add_argument("--batches", type=int, default=0,
                   help="stop after N batches; 0 = run until Ctrl-C (default: 0)")
    p.add_argument("--timeout", type=float, default=30.0,
                   help="per-request HTTP timeout in seconds (default: 30)")
    p.add_argument("--dry-run", action="store_true",
                   help="print one sample record to stdout and exit")
    args = p.parse_args()

    ts = today_ts()

    if args.dry_run:
        print(json.dumps(gen_event(ts), indent=2))
        return 0

    endpoint = f"{args.url}/api/schema/{args.namespace}/table/{args.table}/push"
    print(f"POST -> {endpoint}")
    print(
        f"batch_size={args.batch_size} interval={args.interval}s "
        f"batches={'∞' if args.batches == 0 else args.batches} ts={ts}"
    )

    sent = 0
    ok_records = 0
    fail_batches = 0
    start = time.monotonic()
    i = 0
    try:
        while args.batches == 0 or i < args.batches:
            i += 1
            body = build_batch(args.batch_size, ts)
            t0 = time.monotonic()
            code, text = post_batch(endpoint, body, args.timeout)
            dt_ms = (time.monotonic() - t0) * 1000
            sent += args.batch_size
            snippet = text.strip().replace("\n", " ")[:160]
            if 200 <= code < 300:
                ok_records += args.batch_size
                print(f"[{i:>5}] {code} {dt_ms:>7.1f}ms ok_total={ok_records} | {snippet}")
            else:
                fail_batches += 1
                print(
                    f"[{i:>5}] {code} {dt_ms:>7.1f}ms FAIL | {snippet}",
                    file=sys.stderr,
                )
            if args.interval > 0:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)

    elapsed = time.monotonic() - start
    rate = sent / elapsed if elapsed > 0 else 0.0
    print(
        f"\nsummary: sent={sent} ok={ok_records} fail_batches={fail_batches} "
        f"elapsed={elapsed:.1f}s rate={rate:.1f} rec/s"
    )
    return 0 if fail_batches == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
