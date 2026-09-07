#!/usr/bin/env python3
"""Scrape exactly one CCN and write data-v2/{CCN}.jsonl. Exit 0 only on success.

Thin wrapper over shard_runner_async's machinery, reusing its parser paths
(streaming tempfile parse, format buckets) via v2_scrape_all._raw_to_rows.
"""
import asyncio
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
UA = {"User-Agent": "Mozilla/5.0"}
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


def parse_bytes(fmt: str, ccn: str, raw: bytes) -> list[dict]:
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


def main():
    ccn = sys.argv[1]
    url = get_url(ccn)
    if not url:
        print(f"no url for {ccn}")
        sys.exit(1)
    t0 = time.time()
    try:
        with httpx.Client(timeout=TIMEOUT, follow_redirects=True, headers=UA) as c:
            r = c.get(url)
            r.raise_for_status()
            rows = parse_bytes(fmt_of(url), ccn, r.content)
        OUT.mkdir(exist_ok=True)
        with open(OUT / f"{ccn}.jsonl", "w") as fh:
            for row in rows:
                fh.write(json.dumps(row) + "\n")
        # freshness metadata sidecar: file_last_modified straight from the
        # hospital's server headers, last_checked = our scrape time (UTC)
        meta = {
            "file_last_modified": r.headers.get("last-modified", ""),
            "last_checked": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        (OUT / f"{ccn}.meta.json").write_text(json.dumps(meta))
        print(f"{ccn} ok {len(rows):,} {time.time() - t0:.0f}s")
    except Exception as e:
        Path(OUT / f"{ccn}.jsonl").unlink(missing_ok=True)
        Path(OUT / f"{ccn}.meta.json").unlink(missing_ok=True)
        print(f"{ccn} fail {type(e).__name__}: {str(e)[:80]}")
        sys.exit(1)


if __name__ == "__main__":
    main()
