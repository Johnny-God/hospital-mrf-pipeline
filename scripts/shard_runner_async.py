#!/usr/bin/env python3
"""Async PC shard runner: scrape hospitals whose ccn hash falls in this shard.

One process, N concurrent connections via asyncio + httpx (no worker procs).
CPU-bound parsing (CSV/XLSX/JSON rows) runs in a thread pool so downloads
never block on parsing. Same data-v2 JSONL convention; results rsync to VM.

Usage: .venv/bin/python shard_runner_async.py --shard 0 --total 2 --concurrency 32
"""
import argparse
import asyncio
import hashlib
import io
import json
import sys
import tempfile
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent))
from v2_scrape_all import _raw_to_rows  # noqa: E402

ROOT = Path(__file__).parent.parent
OUT = ROOT / "data-v2"
UA = {"User-Agent": "Mozilla/5.0"}
SEMAPARSE = None  # parser-thread gate, set in main


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


def parse_bytes(fmt: str, ccn: str, raw: bytes) -> list[dict]:
    """CPU-bound parse from an in-memory buffer (thread-pool side)."""
    if fmt == "json":
        from scrape_v2 import stream_v2_bytes
        return list(stream_v2_bytes(raw))
    tmp = tempfile.NamedTemporaryFile(suffix=f".{fmt}", delete=False)
    tmp.write(raw)
    tmp.close()
    try:
        return _raw_to_rows(fmt, b"", ccn, raw_path=Path(tmp.name))
    finally:
        Path(tmp.name).unlink(missing_ok=True)


def write_jsonl(ccn: str, rows: list[dict]):
    with open(OUT / f"{ccn}.jsonl", "w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")


async def scrape_one(client: httpx.AsyncClient, loop, pool, job: dict, stats: dict):
    ccn, url = job["ccn"], job["url"]
    fmt = fmt_of(url)
    out = OUT / f"{ccn}.jsonl"
    if out.exists() and out.stat().st_size > 0:
        stats["skip"] += 1
        return
    t0 = time.time()
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        raw = resp.content
        async with SEMAPARSE:
            rows = await loop.run_in_executor(pool, parse_bytes, fmt, ccn, raw)
        await loop.run_in_executor(pool, write_jsonl, ccn, rows)
        stats["ok"] += 1
        print(f"{ccn} ok {len(rows):,} {time.time()-t0:.0f}s", flush=True)
    except Exception as e:
        out.unlink(missing_ok=True)
        stats["fail"] += 1
        print(f"{ccn} fail {type(e).__name__}: {str(e)[:60]}", flush=True)


async def amain(shard: int, total: int, concurrency: int):
    OUT.mkdir(exist_ok=True)
    global SEMAPARSE
    SEMAPARSE = asyncio.Semaphore(4)  # max concurrent parses (RAM gate)
    jobs = [h for h in load_all() if mine(h["ccn"], shard, total)]
    pending = [j for j in jobs if not (OUT / f"{j['ccn']}.jsonl").exists()
               or (OUT / f"{j['ccn']}.jsonl").stat().st_size == 0]
    print(f"shard {shard}/{total}: {len(jobs)} mine, {len(pending)} pending, "
          f"{concurrency} concurrent connections", flush=True)
    loop = asyncio.get_running_loop()
    from concurrent.futures import ThreadPoolExecutor
    pool = ThreadPoolExecutor(max_workers=6)
    stats = {"ok": 0, "fail": 0, "skip": 0}
    t0 = time.time()
    timeout = httpx.Timeout(300.0, connect=30.0)
    limits = httpx.Limits(max_connections=concurrency,
                          max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(timeout=timeout, limits=limits,
                                 follow_redirects=True, headers=UA) as client:
        batch_size = concurrency * 2  # bounded batches: bounded RAM, live progress
        for i in range(0, len(pending), batch_size):
            batch = pending[i:i + batch_size]
            await asyncio.gather(*(scrape_one(client, loop, pool, j, stats) for j in batch))
            el = time.time() - t0
            print(f"-- progress: {stats['ok']} ok / {stats['fail']} fail / "
                  f"{stats['skip']} skip | {el/60:.0f} min | "
                  f"{stats['ok']/max(el/60,0.01):.1f} ok/min", flush=True)
    pool.shutdown()
    print(f"SHARD_DONE: {(time.time()-t0)/60:.0f} min | {stats}", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", type=int, required=True)
    ap.add_argument("--total", type=int, required=True)
    ap.add_argument("--concurrency", type=int, default=32)
    a = ap.parse_args()
    asyncio.run(amain(a.shard, a.total, a.concurrency))
