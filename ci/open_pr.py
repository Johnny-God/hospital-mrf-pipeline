#!/usr/bin/env python3
"""Open a DoltHub PR for a CI branch, via API v2. Auth: DOLTHUB_TOKEN secret."""
import json
import os
import sys
import urllib.request

DB = "johnnygod/hospital-prices"
TOKEN = os.environ["DOLTHUB_TOKEN"]


def api(path: str, body: dict | None = None):
    req = urllib.request.Request(
        f"https://www.dolthub.com/api/v2/databases/{DB}/{path}",
        data=json.dumps(body).encode() if body else None,
        headers={"authorization": f"token {TOKEN}", "content-type": "application/json"},
        method="POST" if body else "GET",
    )
    with urllib.request.urlopen(req) as r:
        return json.load(r)


branch = sys.argv[1]
shard = sys.argv[2]

if not TOKEN:
    print("DOLTHUB_TOKEN not set — branch pushed, PR/merge SKIPPED. Set the secret to enable auto-merge.")
    sys.exit(0)

# PR source = branch on the same database
pr = api("pulls", {
    "title": f"shard {shard} — {branch}",
    "fromBranchName": branch,
    "toBranchName": "main",
})
print("PR:", json.dumps(pr)[:300])

pull = pr["data"]["pull_number"] if isinstance(pr.get("data"), dict) else pr.get("pull_number")
if pull:
    m = api(f"pulls/{pull}/merge", {})
    print("merge op:", json.dumps(m)[:300])
