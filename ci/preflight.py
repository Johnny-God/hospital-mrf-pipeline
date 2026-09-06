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
import requests

ROOT = Path(__file__).parent.parent
BASELINE = ROOT / "dim" / "fingerprints.json"
OUT_CHANGED = Path(__file__).parent / "changed_ccns.json"
OUT_NEW = Path(__file__).parent / "fingerprints.new.json"
OUT_REPAIRS = Path(__file__).parent / "url_repairs.json"
DEAD_RETRY_DAYS = 30
UA = {"User-Agent": "Mozilla/5.0"}

sys.path.insert(0, str(ROOT / "scripts"))
from fix_broken_urls import scrape_transparency_page, validate_url  # noqa: E402


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


def _sync_fingerprint(url: str) -> dict | None:
    """HEAD (then ranged-GET fallback) a URL with requests; build fingerprint dict."""
    for method, headers in (("HEAD", None), ("GET", {"Range": "bytes=0-0"})):
        try:
            h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            r = requests.request(method, url, headers=headers, timeout=20, allow_redirects=True)
            if r.status_code < 400:
                return {
                    "etag": r.headers.get("etag", ""),
                    "last_modified": r.headers.get("last-modified", ""),
                    "length": r.headers.get("content-length", ""),
                    "status": "ok",
                    "checked": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                }
        except Exception:
            continue
    return None


def repair_dead(dead_jobs: list[dict], url_paths: dict[str, Path], results: dict) -> dict:
    """Repair pass for URLs the probe couldn't reach: scrape the hospital's
    transparency page for a new file link, validate it, update dim/urls in place.

    Reuses fix_broken_urls.py's proven page-scraper (priority patterns) + validator.
    Fixed hospitals get a fresh fingerprint in `results` so the baseline picks them up,
    and the caller marks them changed. Mutates the dim JSON files. Returns summary.
    """
    fixed, unfixable = [], []
    for job in dead_jobs:
        ccn, page = job["ccn"], job["transparency_page"]
        if not page:
            unfixable.append(ccn)
            continue
        new_url = scrape_transparency_page(page)
        if not new_url or new_url == job["url"]:
            unfixable.append(ccn)
            continue
        ok, _ = validate_url(new_url)
        if not ok:
            unfixable.append(ccn)
            continue
        with open(url_paths[ccn]) as f:
            entries = json.load(f)
        for e in entries:
            if e.get("ccn") == ccn:
                e["file_url"] = new_url
        with open(url_paths[ccn], "w") as f:
            json.dump(entries, f, indent=2)
            f.write("\n")
        fp = _sync_fingerprint(new_url)
        if fp:
            results[ccn] = fp  # baseline picks this up via the normal comprehension
        fixed.append({"ccn": ccn, "old": job["url"], "new": new_url})
        print(f"  repaired {ccn}: {new_url[:80]}", flush=True)
    return {"fixed": fixed, "unfixable": unfixable}


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

    # Repair pass: for probe-dead hospitals that WERE reachable at baseline,
    # re-find the file URL via the transparency page. Fixed ones become "changed"
    # and get re-fingerprinted into results (so the baseline comprehension below
    # picks them up) before outputs are written.
    if dead:
        h_by_ccn = {h["ccn"]: h for h in hospitals}
        url_paths = {}
        tps = {}
        for f in sorted(ROOT.glob("dim/urls/*.json")):
            for e in json.load(open(f)):
                if e.get("ccn"):
                    url_paths.setdefault(e["ccn"], f)
                    tps.setdefault(e["ccn"], e.get("transparency_page"))
        repairable = [dict(h_by_ccn[c], transparency_page=tps.get(c))
                      for c in dead if baseline.get(c, {}).get("status") == "ok"]
        if repairable:
            print(f"repairing {len(repairable)} dead URLs via transparency pages...", flush=True)
            rep = repair_dead(repairable, url_paths, results)
            changed += [r["ccn"] for r in rep["fixed"]]
            changed = list(dict.fromkeys(changed))  # dedupe, keep order
            OUT_REPAIRS.write_text(json.dumps(rep, indent=1))
            print(f"REPAIR_DONE: {len(rep['fixed'])} fixed, "
                  f"{len(rep['unfixable'])} unfixable", flush=True)

    # baseline only keeps entries we could actually fingerprint (incl. repairs)
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
