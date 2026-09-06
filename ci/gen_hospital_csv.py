#!/usr/bin/env python3
"""Build ci/hospital.csv (hospital dim rows) for the CCNs scraped in this run."""
import csv
import glob
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

done = {os.path.basename(f).replace(".jsonl", "")
        for f in sys.argv[1:] if os.path.getsize(f) > 0}

n = 0
with open("ci/hospital.csv", "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["ccn", "hospital_name", "state", "file_url", "transparency_page"])
    for f in sorted(glob.glob(os.path.join(REPO, "dim", "urls", "*.json"))):
        state = os.path.basename(f).replace(".json", "")
        for e in json.load(open(f)):
            if e.get("ccn") in done:
                w.writerow([e.get("ccn"), e.get("hospital_name"), state,
                            e.get("file_url"), e.get("transparency_page")])
                n += 1
print(f"DONE: {n} hospital rows")
