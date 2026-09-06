#!/usr/bin/env python3
"""PC shard runner: scrape hospitals whose ccn hash falls in this shard.
Works off the SAME data-v2 JSONL convention; results rsync back to the VM.

Usage: .venv/bin/python shard_runner.py --shard 0 --total 2 --workers 12
"""
import argparse
import hashlib
import json
import os
import sys
import time
from multiprocessing import Process, Value
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from scrape_v2 import stream_v2                      # noqa: E402
from v2_scrape_all import _raw_to_rows, _find_local_raw  # noqa: E402

ROOT = Path(__file__).parent.parent
OUT = ROOT / "data-v2"


def load_all():
    seen, out = set(), []
    for f in sorted(ROOT.glob("dim/urls/*.json")):
        for e in json.load(open(f)):
            ccn, url = e.get("ccn"), (e.get("file_url") or "").strip()
            if not ccn or ccn in seen or not url:
                continue
            seen.add(ccn)
            out.append({"ccn": ccn, "url": url})
    return out


def mine(ccn: str, shard: int, total: int) -> bool:
    return int(hashlib.md5(ccn.encode()).hexdigest(), 16) % total == shard


def fmt_of(url: str) -> str:
    ul = url.lower()
    if ".json" in ul: return "json"
    if ".csv" in ul: return "csv"
    if ".xlsx" in ul or ".xls" in ul: return "xlsx"
    if ".zip" in ul: return "zip"
    return "other"


def scrape_one(job: dict) -> dict:
    ccn, url = job["ccn"], job["url"]
    fmt = fmt_of(url)
    out = OUT / f"{ccn}.jsonl"
    if out.exists() and out.stat().st_size > 0:
        return {"ccn": ccn, "status": "skipped_existing"}
    t0 = time.time()
    tmpdl = None
    try:
        import tempfile
        if fmt == "json":
            rows = list(stream_v2(url))
        else:
            import requests
            resp = requests.get(url, timeout=300, stream=True,
                                headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            ext = ".zip" if fmt == "zip" else ".xlsx" if fmt == "xlsx" else ".csv"
            tmpdl = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
            for chunk in resp.iter_content(1 << 20):
                tmpdl.write(chunk)
            tmpdl.close()
            rows = _raw_to_rows(fmt, b"", ccn, raw_path=Path(tmpdl.name))
        with open(out, "w") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")
        return {"ccn": ccn, "status": "ok", "rows": len(rows), "secs": round(time.time() - t0, 1)}
    except Exception as e:
        out.unlink(missing_ok=True)
        return {"ccn": ccn, "status": "fail", "error": f"{type(e).__name__}: {str(e)[:70]}"}
    finally:
        if tmpdl:
            Path(tmpdl.name).unlink(missing_ok=True)


def worker(w: int, jobs: list, next_idx: Value):
    while True:
        with next_idx.get_lock():
            i = next_idx.value
            if i >= len(jobs):
                return
            next_idx.value = i + 1
        rec = scrape_one(jobs[i])
        rec["worker"] = w
        print(f"[w{w}] {rec['ccn']} {rec['status']}"
              + (f" {rec.get('rows', 0):,}" if rec["status"] == "ok" else ""), flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", type=int, required=True)
    ap.add_argument("--total", type=int, required=True)
    ap.add_argument("--workers", type=int, default=12)
    a = ap.parse_args()
    OUT.mkdir(exist_ok=True)
    all_h = load_all()
    jobs = [h for h in all_h if mine(h["ccn"], a.shard, a.total)]
    pending = [j for j in jobs if not (OUT / f"{j['ccn']}.jsonl").exists()
               or (OUT / f"{j['ccn']}.jsonl").stat().st_size == 0]
    print(f"shard {a.shard}/{a.total}: {len(jobs)} mine, {len(pending)} pending, {a.workers} workers", flush=True)
    next_idx = Value("i", 0)
    procs = [Process(target=worker, args=(w, pending, next_idx)) for w in range(a.workers)]
    t0 = time.time()
    [p.start() for p in procs]
    [p.join() for p in procs]
    print(f"SHARD_DONE: {(time.time()-t0)/60:.0f} min", flush=True)
