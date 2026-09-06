#!/usr/bin/env python3
"""Merge any open DoltHub PRs for the hospital-prices DB (API v2), then poll."""
import json
import os
import time
import urllib.request

DB = "johnnygod/hospital-prices"
TOKEN = os.environ["DOLTHUB_TOKEN"]


def api(path: str, body: dict | None = None, method: str = "GET"):
    req = urllib.request.Request(
        f"https://www.dolthub.com/api/v2/databases/{DB}/{path}",
        data=json.dumps(body).encode() if body else None,
        headers={"authorization": f"token {TOKEN}", "content-type": "application/json"},
        method=method if body else "GET",
    )
    with urllib.request.urlopen(req) as r:
        return json.load(r)


pulls = api("pulls")
data = pulls.get("data", pulls)
items = data if isinstance(data, list) else data.get("pulls", data.get("items", []))
print(f"open pulls: {len(items)}")

for p in items:
    num = p.get("pull_number") or p.get("number")
    print(f"merging PR {num} ({p.get('title', '')})")
    op = api(f"pulls/{num}/merge", {}, method="POST")
    ref = (op.get("data") or {}).get("operationRef") or op.get("operationName") or ""
    oid = ref.rstrip("/").split("/")[-1]
    for _ in range(60):
        time.sleep(10)
        st = api(f"operations/{oid}") if oid else op
        s = st.get("data", st)
        status = s.get("status")
        print(" ", status)
        if status == "succeeded":
            break
        if status == "failed":
            print("  MERGE FAILED:", json.dumps(s)[:300])
            break
