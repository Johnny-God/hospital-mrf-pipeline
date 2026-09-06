#!/usr/bin/env python3
"""Preflight: decide which hospitals need re-scraping WITHOUT downloading MRFs.

HEADs every dim/urls URL (fallback: ranged GET for HEAD-hostile servers), compares
ETag / Last-Modified / Content-Length against the committed baseline
dim/fingerprints.json, and writes:
  ci/changed_ccns.json   - {"changed": [...], "dead_recent": [...]} for the scrape jobs
  ci/fingerprints.new.json - full new baseline (committed after the run)

Rules:
  - no baseline / ccn not in baseline            -> changed (full sweep on first run)
  - any fingerprint field differs                -> changed
  - HEAD+GET both fail: if the stored entry was
    also an error < DEAD_RETRY_DAYS old          -> dead_recent (skip; retry later)
    (errors older than that are retried = changed)
  - server gives no etag AND no last-modified    -> changed (can't prove unchanged)

Usage: python ci/preflight.py [--concurrency 50]
"""
import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).parent.parent
BASELINE = ROOT / "dim" / "fingerprints.json"
OUT_CHANGED = Path(__file__).parent / "changed_ccns.json"
OUT_NEW = Path(__file__).parent / "fingerprints.new.json"
DEAD_RETRY_DAYS = 30
UA = {"User-Agent": "Mozilla/5.0"}


def load_hospitals():
    seen, out = set(), []
    for f in sorted(ROOT.glob("dim/urls/*.json")):
        for e in json.load(open(f)):
            ccn, url = e.get("ccn"), (e.get("file_url") or "").strip()
            if ccn and url and ccn not in seen:
                seen.add(ccn)
                out.append({"ccn": ccn, "url": url})
    return out


def fingerprint_from_headers(h: httpx.Headers) -> dict:
    return {
        "etag": h.get("etag", ""),
        "last_modified": h.get("last-modified", ""),
        "length": h.get("content-length", ""),
        "status": "ok",
        "checked": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }


def needs_scrape(ccn, old, new) -> bool:
    if old is None:
        return True
    if old.get("status") == "error":
        # was dead recently -> skip; old enough -> retry
        age = (datetime.now(timezone.utc) -
               datetime.fromisoformat(old.get("checked", "2000-01-01") + "T00:00:00+00:00")).days
        return age >= DEAD_RETRY_DAYS
    if new is None:
        return True  # server died since baseline -> retry it
    if not (new["etag"] or new["last_modified"]):
        return True  # no change-detecting headers -> scrape
    return any(new[k] != old.get(k) for k in ("etag", "last_modified", "length"))


async def probe(client, sem, job, results):
    ccn, url = job["ccn"], job["url"]
    async with sem:
        fp = None
        for method, headers in (("HEAD", None), ("GET", {"Range": "bytes=0-0"})):
            try:
                r = await client.request(method, url, headers=headers)
                if r.status_code < 400:
                    fp = fingerprint_from_headers(r.headers)
                    break
            except Exception:
                pass
        results[ccn] = fp


async def main(concurrency: int):
    hospitals = load_hospitals()
    baseline = json.load(open(BASELINE)) if BASELINE.exists() else {}
    results, sem = {}, asyncio.Semaphore(concurrency)
    t0 = time.time()
    timeout = httpx.Timeout(30.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True,
                                 headers=UA, limits=httpx.Limits(max_connections=concurrency)) as c:
        batch = concurrency * 4
        for i in range(0, len(hospitals), batch):
            await asyncio.gather(*(probe(c, sem, j, results) for j in hospitals[i:i + batch]))
            print(f"  probed {min(i + batch, len(hospitals))}/{len(hospitals)} "
                  f"({time.time() - t0:.0f}s)", flush=True)

    changed, dead = [], []
    for h in hospitals:
        ccn, new = h["ccn"], results[h["ccn"]]
        old = baseline.get(ccn)
        if needs_scrape(ccn, old, new):
            changed.append(ccn)
        elif new is None:
            dead.append(ccn)
    # baseline only keeps entries we could actually fingerprint
    new_baseline = {h["ccn"]: results[h["ccn"]] for h in hospitals if results[h["ccn"]]}

    OUT_CHANGED.write_text(json.dumps({"changed": changed, "dead_recent": dead}))
    OUT_NEW.write_text(json.dumps(new_baseline, indent=1))
    print(f"PREFLIGHT_DONE: {len(changed)} to scrape, {len(dead)} dead-recent, "
          f"{len(new_baseline)} fingerprinted, {time.time() - t0:.0f}s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--concurrency", type=int, default=50)
    a = ap.parse_args()
    asyncio.run(main(a.concurrency))
