#!/usr/bin/env python3
"""Merge any open DoltHub PRs on hospital-prices (v1alpha1 API; same quirks as open_pr.py)."""
import json
import os
import time
import sys
import urllib.request

DB = "johnnygod/hospital-prices"
TOKEN = os.environ["DOLTHUB_TOKEN"]
BASE = f"https://www.dolthub.com/api/v1alpha1/{DB}"


def api(path: str, body: dict | None = None, method: str = "GET"):
    last = None
    for attempt in range(4):
        try:
            req = urllib.request.Request(
                f"{BASE}/{path}",
                data=json.dumps(body).encode() if body is not None else None,
                headers={"authorization": f"token {TOKEN}", "content-type": "application/json"},
                method=method,
            )
            with urllib.request.urlopen(req) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            last = e
            if e.code < 500 and e.code != 429:
                raise  # client error: retrying won't help
            time.sleep(15 * (attempt + 1))
    raise last


pulls = api("pulls")
if pulls.get("status") != "Success":
    print("LIST FAILED:", json.dumps(pulls)[:300])
    raise SystemExit(1)
items = pulls.get("pulls") or pulls.get("data") or []
print(f"open pulls: {len(items)}")

failed_prs = []

for p in items:
    num = p.get("pull_id") or p.get("number")
    print(f"merging PR {num} ({p.get('title', p.get('title_description', ''))})")
    try:
        op = api(f"pulls/{num}/merge", method="POST")
        operation = op.get("operation_name")
        if not operation:
            print("  no operation returned:", json.dumps(op)[:300])
            continue
        merged = False
        for i in range(120):
            time.sleep(20)
            st = api(f"pulls/{num}/merge?operationName={operation}")
            s = st.get("job_status")
            if s == "Completed":
                print("  COMPLETED")
                merged = True
                break
            if s and s.lower() == "failed":
                print("  FAILED:", json.dumps(st)[:300])
                break
        else:
            print("  merge poll timeout — PR left open")
        if not merged:
            failed_prs.append(num)
    except Exception as e:
        print(f"  ERROR merging PR {num}: {e}")
        failed_prs.append(num)

print(f"done: {len(items) - len(failed_prs)} merged, {len(failed_prs)} failed: {failed_prs}")
if failed_prs:
    sys.exit(1)
