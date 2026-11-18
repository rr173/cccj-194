#!/usr/bin/env python3
"""Generate deterministic JSONL intake files at any scale.

Usage:
    python3 scripts/gen_input.py --profile normalize --rows 100000 --seed 7 --out data.jsonl
    python3 scripts/gen_input.py --profile crossfield --rows 1000 --seed 3 --out data.jsonl

Options:
    --malformed-every N   emit a broken (non-JSON) line every N rows (default 0)
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta


def _rand_dt(rng):
    base = datetime(2020, 1, 1)
    return base + timedelta(
        days=rng.randint(0, 2200), hours=rng.randint(0, 23), minutes=rng.randint(0, 59)
    )


def gen_normalize_row(rng):
    u = rng.random()
    dt = _rand_dt(rng)
    if u < 0.50:
        fmt = rng.choice(
            ["%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%SZ", "%d/%m/%Y", "%Y-%m-%d"]
        )
        raw_ts = dt.strftime(fmt)
    elif u < 0.65:
        raw_ts = None
    elif u < 0.85:
        raw_ts = rng.choice(["not-a-date", "2024-13-40", "32/01/2024", "", "2024-02-30"])
    else:
        raw_ts = "  " + dt.strftime("%Y-%m-%d %H:%M") + "  "

    u = rng.random()
    if u < 0.55:
        style = rng.random()
        if style < 0.4:
            raw_amount = f"{rng.randint(0, 9999)}.{rng.randint(0, 99):02d}"
        elif style < 0.6:
            raw_amount = f"{rng.randint(1, 999)},{rng.randint(0, 999):03d}.{rng.randint(0, 9)}"
        elif style < 0.75:
            raw_amount = rng.choice(["€", "$", "¥"]) + f"{rng.randint(0, 500)}.{rng.randint(0, 9)}"
        elif style < 0.9:
            raw_amount = f"({rng.randint(1, 500)}.{rng.randint(0, 99):02d})"
        else:
            raw_amount = f"-{rng.randint(1, 999)}"
    elif u < 0.70:
        raw_amount = None
    elif u < 0.88:
        raw_amount = rng.choice(["abc", "12.3.4", "€", "--5", "()"])
    else:
        raw_amount = "  42.5 "

    return {"raw_ts": raw_ts, "raw_amount": raw_amount}


def gen_crossfield_row(rng):
    u = rng.random()
    if u < 0.70:
        qty = rng.randint(1, 9)
    elif u < 0.78:
        qty = 0
    elif u < 0.83:
        qty = -3
    elif u < 0.90:
        qty = None
    else:
        qty = 100

    u = rng.random()
    if u < 0.80:
        currency = rng.choice(["USD", "EUR", "CNY"])
    elif u < 0.90:
        currency = "XXX"
    else:
        currency = ""

    u = rng.random()
    if u < 0.65:
        unit_price = f"{rng.randint(1, 200)}.{rng.randint(0, 99):02d}"
    elif u < 0.80:
        unit_price = None
    else:
        unit_price = rng.choice(["abc", "1.2.3", ""])

    start = _rand_dt(rng)
    u = rng.random()
    if u < 0.60:
        end = start + timedelta(seconds=rng.randint(0, 86400))
        start_ts = start.strftime("%Y-%m-%d %H:%M")
        end_ts = end.strftime("%Y-%m-%d %H:%M")
    elif u < 0.72:
        end = start - timedelta(seconds=rng.randint(1, 3600))
        start_ts = start.strftime("%Y-%m-%d %H:%M")
        end_ts = end.strftime("%Y-%m-%d %H:%M")
    elif u < 0.82:
        start_ts = start.strftime("%Y-%m-%d %H:%M")
        end_ts = None
    elif u < 0.90:
        start_ts = None
        end_ts = start.strftime("%Y-%m-%d %H:%M")
    else:
        start_ts = "not-a-ts"
        end_ts = start.strftime("%Y-%m-%d %H:%M")

    return {
        "qty": qty,
        "unit_price": unit_price,
        "currency": currency,
        "start_ts": start_ts,
        "end_ts": end_ts,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=["normalize", "crossfield"], required=True)
    ap.add_argument("--rows", type=int, required=True)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", required=True)
    ap.add_argument("--malformed-every", type=int, default=0)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    gen = gen_normalize_row if args.profile == "normalize" else gen_crossfield_row
    with open(args.out, "w", encoding="utf-8") as fh:
        for i in range(args.rows):
            if args.malformed_every and i and i % args.malformed_every == 0:
                fh.write('{"raw_ts": "2024-01-01", BROKEN\n')
                continue
            fh.write(json.dumps(gen(rng), ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
