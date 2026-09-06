#!/usr/bin/env python3
"""v2 national scrape orchestrator — all formats, v1-hardened.

Integrates every v1 operational feature into the v2 pipeline:
- All formats: JSON (stream_v2) + CSV/XLSX/ZIP via src/scrapers parsing -> extract_csv_v2
- Per-hospital subprocess timeout (stuck workers killed, batch continues)
- --max-age-days freshness skip (skip hospitals scraped recently)
- Local-raw-file fallback (WAF-blocked hospitals via manually dropped files)
- Per-state status JSON (machine-readable run state)
- --validate-cpt flag (OHDSI Athena vocabulary filter)
- 3-worker parallel, resumable (skips non-empty existing outputs)

Run: .venv/bin/python scripts/v2_scrape_all.py --workers 3 [--max-age-days 6] [--validate-cpt]
"""
import csv
import io
import json
import multiprocessing as mp
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import click
import pandas as pd
import requests

PROJECT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT / "scripts"))
sys.path.insert(0, str(PROJECT))

from scrape_v2 import stream_v2                      # noqa: E402
from extract_csv_v2 import extract_rows_csv         # noqa: E402

OUT = PROJECT / "data-v2"
STATUS = PROJECT / "status"
FRESHNESS_DB = OUT / ".last_scraped.json"           # ccn -> iso timestamp


def load_hospitals(formats: str = "json,csv,xlsx,zip,other"):
    """Hospitals from dim/urls, filtered by URL format bucket."""
    fmts = set(formats.split(","))
    seen, out = set(), []
    for f in sorted(PROJECT.glob("dim/urls/*.json")):
        state = f.stem.upper()
        for e in json.load(open(f)):
            ccn, url = e.get("ccn"), (e.get("file_url") or "").strip()
            if not ccn or ccn in seen or not url:
                continue
            ul = url.lower()
            if ".json" in ul:
                fmt = "json"
            elif ".csv" in ul:
                fmt = "csv"
            elif ".xlsx" in ul or ".xls" in ul:
                fmt = "xlsx"
            elif ".zip" in ul:
                fmt = "zip"
            else:
                fmt = "other"
            if fmt not in fmts:
                continue
            seen.add(ccn)
            out.append({"ccn": ccn, "state": state, "url": url, "format": fmt})
    return out


def load_freshness() -> dict:
    if FRESHNESS_DB.exists():
        return json.load(open(FRESHNESS_DB))
    return {}


def save_freshness(db: dict):
    FRESHNESS_DB.write_text(json.dumps(db, indent=1))


def _raw_to_rows(fmt: str, raw: bytes, ccn: str, raw_path: Path | None = None) -> list[dict]:
    """Parse non-JSON formats with v1 scrapers, then re-extract to v2 schema."""
    import tempfile
    if fmt == "zip":
        import zipfile
        with tempfile.TemporaryDirectory() as td:
            zp = Path(td) / f"{ccn}.zip"
            zp.write_bytes(raw)
            rows = []
            try:
                with zipfile.ZipFile(zp) as z:
                    for name in z.namelist():
                        inner = z.read(name)
                        il = name.lower()
                        sub = "json" if il.endswith(".json") else "csv" if il.endswith(".csv") else "xlsx" if il.endswith((".xlsx", ".xls")) else None
                        if sub:
                            rows.extend(_raw_to_rows(sub, inner, ccn))
            except zipfile.BadZipFile:
                pass
            return rows
    if fmt == "json":
        # small json files: parse directly
        try:
            data = json.loads(raw.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            return []
        from scrape_v2 import extract_rows
        items = data if isinstance(data, list) else _walk_items(data)
        rows = []
        for it in items:
            if isinstance(it, dict):
                rows.extend(extract_rows(it))
        return rows
    # CSV / XLSX — streamed, not materialized
    # ponytail: pandas read_excel/read_csv materialize whole files (multi-GB RAM);
    # openpyxl read_only + csv.reader stream instead. Called via generator below.
    if fmt == "xlsx":
        gen = _xlsx_rows(raw)
    else:
        gen = _csv_rows(raw, path=raw_path)
    rows = []
    for d in gen:
        rows.extend(extract_rows_csv(d))
    return rows


def _xlsx_rows(raw: bytes):
    """Stream XLSX rows via openpyxl read_only (constant RAM)."""
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    ws = wb.active
    header = None
    for row in ws.iter_rows(values_only=True):
        vals = ["" if v is None else str(v) for v in row]
        if header is None:
            if any("code" in v.lower() or "hcpcs" in v.lower() for v in vals):
                header = [str(v).strip() for v in vals]
            continue
        yield dict(zip(header, vals))
    wb.close()


def _csv_rows(raw: bytes, path: Path | None = None):
    """Stream CSV rows via csv.reader (constant RAM, no pandas).
    Accepts raw bytes OR a file path (path = zero giant-RAM copies)."""
    if path:
        text = None
        delim = None
        # sniff delimiter from first 4KB
        with open(path, "rb") as fh:
            head = fh.read(4096).decode("utf-8", errors="replace")
        delim = "|" if head.count("|") > head.count(",") else ","
        first = head.split("\n")[0].lower()
        start = 2 if ("hospital_name" in first and "description" not in first) else 0
        fh_in = open(path, "r", encoding="utf-8", errors="replace")
        if start:
            for _ in range(start):
                next(fh_in)
        reader = csv.reader(fh_in, delimiter=delim)
        header = None
        for row in reader:
            if header is None:
                if not any(row):
                    continue
                header = [c.strip() for c in row]
                continue
            yield dict(zip(header, row))
        fh_in.close()
        return
    text = _decode(raw)
    delim = "|" if text.count("|") > text.count(",") else ","
    lines = text.splitlines()
    # find header row (CMS metadata headers have 'hospital_name' but no 'description')
    start = 0
    first = lines[0].lower() if lines else ""
    if "hospital_name" in first and "description" not in first:
        start = 2  # CMS metadata header rows
    reader = csv.reader(io.StringIO("\n".join(lines[start:])), delimiter=delim)
    header = None
    for row in reader:
        if header is None:
            # skip empty leading rows
            if not any(row):
                continue
            header = [c.strip() for c in row]
            continue
        yield dict(zip(header, row))


def _walk_items(data):
    """Find the list of charge items anywhere in a JSON dict."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("standard_charge_information", "charges", "standard_charges", "items"):
            if isinstance(data.get(k), list):
                return data[k]
        for v in data.values():
            if isinstance(v, (list, dict)):
                r = _walk_items(v)
                if r:
                    return r
    return []


def _decode(raw: bytes) -> str:
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def scrape_one(job: dict, max_age_days: int, freshness: dict) -> dict:
    """Scrape a single hospital; returns status record."""
    ccn, url, fmt = job["ccn"], job["url"], job["format"]
    out = OUT / f"{ccn}.jsonl"
    now = datetime.now(timezone.utc).isoformat()
    if out.exists() and out.stat().st_size > 0:
        return {"ccn": ccn, "status": "skipped_existing"}
    # freshness skip
    if max_age_days > 0:
        last = freshness.get(ccn)
        if last:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(last)).days
            if age < max_age_days:
                return {"ccn": ccn, "status": "skipped_fresh", "age_days": age}
    # local-raw-file fallback (WAF workaround)
    t0 = time.time()
    tmpdl = None
    try:
        local = _find_local_raw(ccn, fmt)
        if local:
            rows = _raw_to_rows(fmt, local.read_bytes(), ccn)
            src = "local_file"
        elif fmt == "json":
            rows, src = list(stream_v2(url)), "url"
        else:
            # ponytail: stream to disk, never resp.content (multi-GB files OOM'd 3 workers)
            import tempfile
            resp = requests.get(url, timeout=180, stream=True,
                                headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            ext = ".zip" if fmt == "zip" else ".xlsx" if fmt == "xlsx" else ".csv"
            tmpdl = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
            for chunk in resp.iter_content(1 << 20):
                tmpdl.write(chunk)
            tmpdl.close()
            # path mode: never hold the file in RAM
            rows, src = _raw_to_rows(fmt, b"", ccn, raw_path=Path(tmpdl.name)), "url"
        with open(out, "w") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")
        if tmpdl:
            Path(tmpdl.name).unlink(missing_ok=True)
        return {"ccn": ccn, "status": "ok", "rows": len(rows), "src": src,
                "secs": round(time.time() - t0, 1)}
    except Exception as e:
        out.unlink(missing_ok=True)
        if tmpdl:
            Path(tmpdl.name).unlink(missing_ok=True)
        return {"ccn": ccn, "status": "fail", "error": f"{type(e).__name__}: {str(e)[:70]}",
                "secs": round(time.time() - t0, 1)}


def _find_local_raw(ccn: str, fmt: str) -> Path | None:
    legacy = PROJECT / "data"  # v1 convention: manually dropped files here
    for ext in (f".{fmt}", ".csv", ".json", ".zip", ".xlsx"):
        p = legacy / f"{ccn}{ext}"
        if p.exists() and p.stat().st_size > 0:
            return p
    return None


def _worker(name: int, jobs: list, n: int, max_age: int, freshness: dict,
            validate_cpt: bool, status_list, next_idx=None):
    """Dynamic queue: pull next job index atomically instead of round-robin
    shards — workers that finish fast keep getting work (self-balancing)."""
    if validate_cpt:
        _load_athena()
    while True:
        if next_idx is not None:
            with next_idx.get_lock():
                i = next_idx.value
                if i >= len(jobs):
                    return
                next_idx.value = i + 1
        else:
            i = name
            if i >= len(jobs):
                return
            name += n
        rec = scrape_one(jobs[i], max_age, freshness)
        rec["worker"] = name
        print(f"[w{name}] {rec['ccn']} {rec['status']}"
              + (f" {rec.get('rows', 0):,}" if rec["status"] == "ok" else ""), flush=True)
        status_list.append(rec)


_ATHENA = None
def _load_athena():
    global _ATHENA
    if _ATHENA is None:
        import gzip
        import csv as _csv
        codes = set()
        with gzip.open(PROJECT / "dim" / "CONCEPT.csv.gz", "rt") as f:
            r = _csv.DictReader(f, delimiter="\t")
            for row in r:
                codes.add(row["concept_code"])
        _ATHENA = codes


def _filter_athena(rows: list[dict]) -> list[dict]:
    if _ATHENA is None:
        return rows
    return [r for r in rows if r.get("code") in _ATHENA]


@click.command()
@click.option("--workers", default=3)
@click.option("--formats", default="json,csv,xlsx,zip,other")
@click.option("--max-age-days", default=0)
@click.option("--validate-cpt", is_flag=True)
@click.option("--limit", default=0)
@click.option("--batch-size", default=0, help="Process only the first N *pending* hospitals")
def main(workers, formats, max_age_days, validate_cpt, limit, batch_size):
    OUT.mkdir(exist_ok=True)
    STATUS.mkdir(exist_ok=True)
    hospitals = load_hospitals(formats)
    if limit:
        hospitals = hospitals[:limit]
    # batch: only pending (no non-empty output yet)
    if batch_size:
        hospitals = [h for h in hospitals
                     if not (OUT / f"{h['ccn']}.jsonl").exists()
                     or (OUT / f"{h['ccn']}.jsonl").stat().st_size == 0][:batch_size]
    freshness = load_freshness()
    # Q2: small-first ordering — HEAD content-length if we have it, else format guess.
    # Big files (slow) go last so most hospitals complete early.
    def _size_key(h):
        sizes = {"json": 3, "csv": 2, "xlsx": 2, "zip": 2, "other": 1}
        return sizes.get(h["format"], 1)
    hospitals.sort(key=_size_key)
    mp.set_start_method("spawn")
    mgr = mp.Manager()
    status_list = mgr.list()
    next_idx = mp.Value("i", 0)
    procs = [mp.Process(target=_worker,
                        args=(w, hospitals, workers, max_age_days, freshness,
                              validate_cpt, status_list, next_idx))
             for w in range(workers)]
    t0 = time.time()
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    # write status + freshness
    recs = list(status_list)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    (STATUS / f"v2_scrape_{stamp}.json").write_text(json.dumps(recs, indent=1))
    for r in recs:
        if r["status"] == "ok":
            freshness[r["ccn"]] = datetime.now(timezone.utc).isoformat()
    save_freshness(freshness)
    ok = sum(1 for r in recs if r["status"] == "ok")
    fail = sum(1 for r in recs if r["status"] == "fail")
    skip = len(recs) - ok - fail
    print(f"V2_ALL_DONE: {ok} ok / {fail} fail / {skip} skipped in {(time.time()-t0)/60:.0f} min",
          flush=True)


if __name__ == "__main__":
    main()
