#!/usr/bin/env python3
"""Merge any open DoltHub PRs on hospital-prices (v1alpha1 API; same quirks as open_pr.py)."""
import json
import os
import time
import urllib.request

DB = "johnnygod/hospital-prices"
TOKEN = os.environ["DOLTHUB_TOKEN"]
BASE = f"https://www.dolthub.com/api/v1alpha1/{DB}"


def api(path: str, body: dict | None = None, method: str = "GET"):
    req = urllib.request.Request(
        f"{BASE}/{path}",
        data=json.dumps(body).encode() if body is not None else None,
        headers={"authorization": f"token {TOKEN}", "content-type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(req) as r:
        return json.load(r)


pulls = api("pulls")
if pulls.get("status") != "Success":
    print("LIST FAILED:", json.dumps(pulls)[:300])
    raise SystemExit(1)
items = pulls.get("pulls") or pulls.get("data") or []
print(f"open pulls: {len(items)}")

for p in items:
    num = p.get("pull_id") or p.get("number")
    print(f"merging PR {num} ({p.get('title', p.get('title_description', ''))})")
    op = api(f"pulls/{num}/merge", method="POST")
    operation = op.get("operation_name")
    if not operation:
        print("  no operation returned:", json.dumps(op)[:300])
        continue
    for i in range(120):
        time.sleep(20)
        st = api(f"pulls/{num}/merge?operationName={operation}")
        s = st.get("job_status")
        if s == "Completed":
            print("  COMPLETED")
            break
        if s and s.lower() == "failed":
            print("  FAILED:", json.dumps(st)[:300])
            break
