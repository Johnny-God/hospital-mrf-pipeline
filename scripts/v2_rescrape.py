#!/usr/bin/env python3
"""Parallel v2 rescrape: all JSON-MRF hospitals -> per-hospital JSONL in data-v2/.

3 worker processes (queue via simple shard-by-index over a sorted hospital list).
Resumable: skips CCNs whose .jsonl already exists and is non-empty.
Run: .venv/bin/python scripts/v2_rescrape.py --workers 3
"""
import json
import os
import sys
import time
from multiprocessing import Process
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from scrape_v2 import stream_v2

PROJECT = Path(__file__).parent.parent
OUT = PROJECT / "data-v2"


def load_hospitals():
    seen = set()
    out = []
    for f in sorted(PROJECT.glob("dim/urls/*.json")):
        state = f.stem.upper()
        for e in json.load(open(f)):
            ccn, url = e.get("ccn"), e.get("file_url", "")
            if not ccn or ccn in seen or not url:
                continue
            if not url.lower().endswith(".json"):
                continue
            seen.add(ccn)
            out.append((ccn, url))
    return sorted(out)


def worker(name: int, jobs: list, n_workers: int):
    for i, (ccn, url) in enumerate(jobs):
        if i % n_workers != name:
            continue
        out = OUT / f"{ccn}.jsonl"
        if out.exists() and out.stat().st_size > 0:
            continue  # resumable
        try:
            n = 0
            with open(out, "w") as fh:
                for r in stream_v2(url):
                    fh.write(json.dumps(r) + "\n")
                    n += 1
            print(f"[w{name}] {ccn} {n:,} rows", flush=True)
        except Exception as e:
            print(f"[w{name}] {ccn} FAIL {type(e).__name__}: {str(e)[:60]}", flush=True)
            out.unlink(missing_ok=True)


if __name__ == "__main__":
    n_workers = int(sys.argv[sys.argv.index("--workers") + 1]) if "--workers" in sys.argv else 3
    OUT.mkdir(exist_ok=True)
    hospitals = load_hospitals()
    done = len([f for f in OUT.glob("*.jsonl") if f.stat().st_size > 0])
    print(f"{len(hospitals)} JSON hospitals, {done} already done, launching {n_workers} workers", flush=True)
    procs = [Process(target=worker, args=(w, hospitals, n_workers)) for w in range(n_workers)]
    t0 = time.time()
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    done = len([f for f in OUT.glob("*.jsonl") if f.stat().st_size > 0])
    print(f"RESCRAPE_DONE: {done}/{len(hospitals)} in {(time.time()-t0)/60:.0f} min", flush=True)
