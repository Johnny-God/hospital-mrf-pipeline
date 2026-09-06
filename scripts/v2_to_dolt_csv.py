#!/usr/bin/env python3
"""Convert data-v2/*.jsonl -> /tmp/dolt-import/rate_v2.csv for Dolt import.
Streams: constant memory. Adds ccn column, maps rate->standard_charge.

Usage: v2_to_dolt_csv.py [file.jsonl ...]
  With explicit files, OUT is NOT truncated — rows are appended (pairs with
  `dolt table import -a --continue`). With no args, falls back to all
  non-empty .jsonl newer than .last_import_stamp.
"""
import csv
import glob
import json
import os
import sys

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = "/tmp/dolt-import/rate_v2.csv"
HDR = ["id", "ccn", "code", "code_prefix", "code_orig", "modifier", "ndc", "apc",
       "rev_code", "internal_code", "billing_class", "patient_class",
       "payer_orig", "plan_orig", "payer_category", "standard_charge",
       "rate_percent", "drug_unit", "drug_quantity"]

files = [f for f in sys.argv[1:] if os.path.getsize(f) > 0]
if not files:
    stamp = os.path.join(PROJECT, ".last_import_stamp")
    cutoff = os.path.getmtime(stamp) if os.path.exists(stamp) else 0
    files = [f for f in sorted(glob.glob(os.path.join(PROJECT, "data-v2", "*.jsonl")))
             if os.path.getsize(f) > 0 and os.path.getmtime(f) > cutoff]

# Explicit list = incremental append; fallback (full rebuild) = truncate.
# Header whenever this invocation CREATES the file — dolt `-a --columns` treats
# line 1 of a headerless CSV as the header and silently drops that row.
mode = "w" if (sys.argv[1:] and not (os.path.exists(OUT) and os.path.getsize(OUT) > 0)) \
       or not sys.argv[1:] else "a"
n = 0
os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, mode, newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=HDR, extrasaction="ignore")
    if mode == "w":
        w.writeheader()
    for f in files:
        ccn = os.path.basename(f).replace(".jsonl", "")
        with open(f) as src:
            for line in src:
                r = json.loads(line)
                r["ccn"] = ccn
                r["standard_charge"] = r.get("rate")
                r["id"] = int(ccn) * 10**9 + n  # ponytail: deterministic ids (ci branches have no shared auto-increment); int(ccn) safe — ccn is a 6-digit CMS number
                w.writerow(r)
                n += 1
print(f"DONE: {len(files)} hospitals, {n:,} rows -> {OUT} (mode={mode})")
