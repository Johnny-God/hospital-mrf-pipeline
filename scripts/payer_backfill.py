#!/usr/bin/env python3
"""National payer-rate backfill: all JSON-format MRFs → data-payer/{STATE}.parquet.

Reuses payer_pilot.stream_rows. Per state: scrape each hospital to a transient
jsonl, convert to one parquet (with ccn column), delete jsonls. Skips GA
(already done by pilot; re-run with --include-ga to redo).

Run: .venv/bin/python scripts/payer_backfill.py [--include-ga]
"""

import csv
import json
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from payer_pilot import stream_rows

PROJECT = Path(__file__).parent.parent
OUT = PROJECT / "data-payer"
PROGRESS = OUT / "backfill_progress.json"


def load_progress() -> dict:
    if PROGRESS.exists():
        return json.load(open(PROGRESS))
    return {"done_states": [], "hospitals": 0, "rows": 0, "failures": 0}


def save_progress(p: dict) -> None:
    json.dump(p, open(PROGRESS, "w"), indent=1)


def do_state(state: str, hospitals: list[dict]) -> tuple[int, int, int]:
    """Scrape all JSON hospitals in a state; stream rows to one state CSV.
    ponytail: no per-hospital read_csv round-trip, no frames list — constant RAM.
    Returns (ok, rows, failed)."""
    tmpdir = OUT / f"tmp-{state}"
    tmpdir.mkdir(parents=True, exist_ok=True)
    state_csv = OUT / f"{state}.csv"
    ok = failed = rows = 0
    with open(state_csv, "w", newline="") as out_fh:
        w = None
        for r in hospitals:
            ccn, url = r["ccn"], r["file_url"]
            n_rows = 0
            try:
                for rec in stream_rows(url):
                    if w is None:
                        w = csv.DictWriter(out_fh, fieldnames=["ccn", "cpt", "payer", "plan", "setting", "price"])
                        w.writeheader()
                    rec["ccn"] = ccn
                    w.writerow(rec)
                    n_rows += 1
            except Exception as e:
                print(f"  {ccn} FAIL {type(e).__name__}: {str(e)[:70]}", flush=True)
                failed += 1
                continue
            if not n_rows:
                print(f"  {ccn} 0 payer rows", flush=True)
                continue
            rows += n_rows
            ok += 1
            print(f"  {ccn} {n_rows:>9,} rows", flush=True)
    if rows:
        print(f"{state}: {ok} hospitals, {rows:,} rows -> {state_csv.name} ({state_csv.stat().st_size/1e6:.1f} MB)", flush=True)
    else:
        state_csv.unlink(missing_ok=True)
    for f in tmpdir.glob("*"):
        f.unlink()
    tmpdir.rmdir()
    return ok, rows, failed


def main(include_ga: bool = False) -> None:
    progress = load_progress()
    for f in sorted((PROJECT / "dim" / "urls").glob("*.json")):
        state = f.stem.upper()
        if state in progress["done_states"]:
            continue
        if state == "GA" and not include_ga:
            progress["done_states"].append(state)  # pilot already covered GA
            save_progress(progress)
            continue
        hospitals = [r for r in json.load(open(f)) if ".json" in r["file_url"].lower()]
        if not hospitals:
            progress["done_states"].append(state)
            save_progress(progress)
            continue
        print(f"=== {state}: {len(hospitals)} JSON hospitals ===", flush=True)
        ok, rows, failed = do_state(state, hospitals)
        progress["done_states"].append(state)
        progress["hospitals"] += ok
        progress["rows"] += rows
        progress["failures"] += failed
        save_progress(progress)
    print(f"\nDONE: {progress['hospitals']} hospitals, {progress['rows']:,} rows, {progress['failures']} failures", flush=True)


if __name__ == "__main__":
    main(include_ga="--include-ga" in sys.argv)
