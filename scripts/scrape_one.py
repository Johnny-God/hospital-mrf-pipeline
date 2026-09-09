#!/usr/bin/env python3
"""Scrape exactly one CCN and write data-v2/{CCN}.jsonl. Exit 0 only on success.

Runs inside GitHub Actions via ci/pipeline.sh. Streams the download to disk
and rows to the output file — no multi-GB resp.content, no materialized row
lists (both OOM classes already bit the VM pipeline twice).
"""
import json
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent))
from v2_scrape_all import _raw_to_rows  # noqa: E402

ROOT = Path(__file__).parent.parent
OUT = ROOT / "data-v2"
# Full browser headers + generous timeout. Stub "Mozilla/5.0" drew 403s from
# anti-bot frontends; legacy-SSL servers are handled by httpfetch in the
# requests path, httpx handles most others via its own SSL stack.
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,text/csv,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}
TIMEOUT = httpx.Timeout(300.0, connect=30.0)


def fmt_of(url: str) -> str:
    ul = url.lower()
    if ".json" in ul: return "json"
    if ".csv" in ul: return "csv"
    if ".xlsx" in ul or ".xls" in ul: return "xlsx"
    if ".zip" in ul: return "zip"
    return "other"


def get_url(ccn: str) -> str:
    for f in sorted(ROOT.glob("dim/urls/*.json")):
        for e in json.load(open(f)):
            if e.get("ccn") == ccn:
                return (e.get("file_url") or "").strip()
    return ""


def main():
    ccn = sys.argv[1]
    url = get_url(ccn)
    if not url:
        print(f"no url for {ccn}")
        sys.exit(1)
    fmt = fmt_of(url)
    t0 = time.time()
    tmpdl = None
    try:
        OUT.mkdir(exist_ok=True)
        out = OUT / f"{ccn}.jsonl"
        with httpx.Client(timeout=TIMEOUT, follow_redirects=True, headers=UA) as c:
            with c.stream("GET", url) as r:
                r.raise_for_status()
                if fmt == "json":
                    suffix = ".json"
                elif fmt == "csv":
                    suffix = ".csv"
                else:
                    suffix = None  # xlsx/zip: smaller, parse from memory
                if suffix:
                    # stream download straight to disk, parse from disk
                    # (zero giant-RAM copies — 850MB CSVs exist here)
                    import tempfile
                    tmpdl = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
                    for chunk in r.iter_bytes(1 << 20):
                        tmpdl.write(chunk)
                    tmpdl.close()
                    rowsrc = _raw_to_rows(fmt, b"", ccn, raw_path=Path(tmpdl.name))
                else:
                    raw = b"".join(r.iter_bytes(1 << 20))
                    rowsrc = _raw_to_rows(fmt, raw, ccn)
        n = 0
        with open(out, "w") as fh:
            for row in rowsrc:
                fh.write(json.dumps(row) + "\n")
                n += 1
        # freshness metadata sidecar: file_last_modified straight from the
        # hospital's server headers, last_checked = our scrape time (UTC)
        meta = {
            "file_last_modified": r.headers.get("last-modified", ""),
            "last_checked": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        (OUT / f"{ccn}.meta.json").write_text(json.dumps(meta))
        print(f"{ccn} ok {n:,} {time.time() - t0:.0f}s")
    except Exception as e:
        Path(OUT / f"{ccn}.jsonl").unlink(missing_ok=True)
        Path(OUT / f"{ccn}.meta.json").unlink(missing_ok=True)
        if tmpdl:
            Path(tmpdl.name).unlink(missing_ok=True)
        print(f"{ccn} fail {type(e).__name__}: {str(e)[:80]}")
        sys.exit(1)
    finally:
        if tmpdl:
            Path(tmpdl.name).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
