#!/usr/bin/env python3
"""Open + merge a DoltHub PR for a CI branch, via API v1alpha1. Auth: DOLTHUB_TOKEN secret.

v1alpha1 quirks (learned live 2026-09-06):
- Bearer/token auth WORKS here (v2 rejects dhat.v1 tokens).
- PR body requires description + all four branch owner/repo fields.
- PRs between unrelated histories fail "no common ancestor" — runners MUST
  clone the DoltHub repo (shared seed commit) rather than dolt init from scratch.
- Merge is async: POST returns operation_name; poll GET .../merge?operationName=...
  until job_status == "Completed".
"""
import json
import os
import sys
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
        method="POST" if body is not None else "GET",
    )
    with urllib.request.urlopen(req) as r:
        return json.load(r)


branch = sys.argv[1]
shard = sys.argv[2]

if not TOKEN:
    print("DOLTHUB_TOKEN not set — branch pushed, PR/merge SKIPPED. Set the secret to enable auto-merge.")
    sys.exit(0)

pr = api("pulls", {
    "title": f"shard {shard} — {branch}",
    "description": f"CI shard {shard}: scrape -> normalize -> import -> push",
    "fromBranchOwnerName": "johnnygod",
    "fromBranchRepoName": "hospital-prices",
    "fromBranchName": branch,
    "toBranchOwnerName": "johnnygod",
    "toBranchRepoName": "hospital-prices",
    "toBranchName": "main",
})
if pr.get("status") != "Success":
    print("PR CREATE FAILED:", json.dumps(pr)[:400])
    sys.exit(1)
pull = pr["pull_id"]
print(f"PR #{pull} created")

op = api(f"pulls/{pull}/merge", method="POST")  # no body needed
operation = op.get("operation_name")
print(f"merge started: {operation}")

for i in range(120):  # up to 40 min
    time.sleep(20)
    st = api(f"pulls/{pull}/merge?operationName={operation}")
    s = st.get("job_status")
    if s == "Completed":
        print("MERGE COMPLETED")
        break
    if s and s.lower() == "failed":
        print("MERGE FAILED:", json.dumps(st)[:400])
        sys.exit(1)
    if i % 6 == 5:
        print(f"  still merging... ({i * 20}s)")
else:
    print("merge poll timeout — PR left open, merge manually or re-run ci/merge_prs.py")
    sys.exit(1)
