#!/usr/bin/env python3
"""Live test of scrape_v2 full-schema extraction against N real hospitals.

Picks hospitals with .json-format MRF URLs from dim/urls, streams each through
stream_v2, writes data-v2/{CCN}.jsonl, prints per-hospital + field coverage stats.

Run: .venv/bin/python scripts/scrape_v2_test.py --n 100
"""
import json
import time
from collections import Counter
from pathlib import Path

import click

sys_path = Path(__file__).parent
import sys
sys.path.insert(0, str(sys_path))
from scrape_v2 import stream_v2

PROJECT = Path(__file__).parent.parent
OUT = PROJECT / "data-v2"

FIELDS = ["code", "code_prefix", "code_orig", "modifier", "ndc", "apc", "rev_code",
          "internal_code", "billing_class", "patient_class", "payer_orig", "plan_orig",
          "payer_category", "rate", "rate_percent", "drug_unit", "drug_quantity"]


def load_hospitals(n: int, want_json: bool = True):
    seen = set()
    out = []
    for f in sorted(PROJECT.glob("dim/urls/*.json")):
        state = f.stem.upper()
        for e in json.load(open(f)):
            ccn, url = e.get("ccn"), e.get("file_url", "")
            if not ccn or ccn in seen or not url:
                continue
            if want_json and not url.lower().endswith(".json"):
                continue
            seen.add(ccn)
            out.append({"ccn": ccn, "state": state, "name": e.get("hospital_name", ""), "url": url})
            if len(out) >= n:
                return out
    return out


@click.command()
@click.option("--n", default=100, help="Number of hospitals to test")
def main(n: int):
    hospitals = load_hospitals(n)
    print(f"testing {len(hospitals)} JSON-MRF hospitals", flush=True)
    OUT.mkdir(exist_ok=True)
    field_hits = Counter()
    cat_counts = Counter()
    prefix_counts = Counter()
    total_rows = 0
    ok = fail = 0
    t0 = time.time()
    for i, h in enumerate(hospitals, 1):
        rows = []
        try:
            rows = list(stream_v2(h["url"]))
            ok += 1
        except Exception as e:
            fail += 1
            print(f"  [{i}] {h['ccn']} FAIL {type(e).__name__}: {str(e)[:60]}", flush=True)
            continue
        with open(OUT / f"{h['ccn']}.jsonl", "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
                for k in FIELDS:
                    if r.get(k) is not None:
                        field_hits[k] += 1
                cat_counts[r["payer_category"]] += 1
                if r["code_prefix"]:
                    prefix_counts[r["code_prefix"]] += 1
        total_rows += len(rows)
        if i % 10 == 0 or i == len(hospitals):
            print(f"  [{i}/{len(hospitals)}] rows so far: {total_rows:,} ({time.time()-t0:.0f}s)", flush=True)

    print("\n=== SUMMARY ===")
    print(f"hospitals ok/fail: {ok}/{fail}")
    print(f"total rows: {total_rows:,}")
    print("payer_category:", dict(cat_counts))
    print("code_prefix:", dict(prefix_counts.most_common()))
    print("field coverage (non-null rows / total):")
    for k in FIELDS:
        pct = 100 * field_hits[k] / total_rows if total_rows else 0
        print(f"  {k:15s} {field_hits[k]:>10,}  {pct:5.1f}%")


if __name__ == "__main__":
    main()
