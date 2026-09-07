#!/usr/bin/env python3
"""Merge orphaned ci/shard-* branches into main via DoltHub PRs (v1alpha1).

Runners bank one hospital per push to `ci/shard-N-<runid>` and only open their
final PR at the end. A killed runner leaves the branch with banked-but-unmerged
hospitals and no PR. This finds those, opens + merges a PR for each, so nothing
a dead runner banked is ever lost. Skip branches younger than 20 min (a live
runner may still be pushing).

Usage: python ci/merge_orphans.py   (env: DOLTHUB_TOKEN)
"""
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

DB = "johnnygod/hospital-prices"
TOKEN = os.environ["DOLTHUB_TOKEN"]
BASE = f"https://www.dolthub.com/api/v1alpha1/{DB}"
GRACE_MIN = 20  # ignore branches that might still be receiving pushes


def api(path: str, body: dict | None = None, method: str = "GET"):
    req = urllib.request.Request(
        f"{BASE}/{path}",
        data=json.dumps(body).encode() if body is not None else None,
        headers={"authorization": f"token {TOKEN}", "content-type": "application/json"},
        method="POST" if body is not None else "GET",
    )
    with urllib.request.urlopen(req) as r:
        return json.load(r)


def branch_age_min(name: str) -> float:
    """Minutes since the branch's last commit (log, newest first)."""
    q = urllib.parse.quote("SELECT commit_hash, committer_date FROM dolt_log "
                           f"WHERE commit_hash = (SELECT DOLT_HASH('{name}')) LIMIT 1")
    # simpler: query the branch's log directly
    q = urllib.parse.quote("SELECT committer_date FROM dolt_log ORDER BY committer_date DESC LIMIT 1")
    try:
        d = api(f"{name}?q={q}")
        rows = d.get("rows", [])
        if rows:
            ts = rows[0]["committer_date"]
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return (datetime.now(timezone.utc) - dt).total_seconds() / 60
    except Exception:
        pass
    return 1e9  # unknown age -> treat as old enough to merge


def main():
    d = api("branches")
    names = [b["name"] for b in d.get("branches", [])
             if re.match(r"^ci/shard-\d+-\d{8}-\d{6}$", b.get("name", ""))]
    print(f"candidate orphan branches: {len(names)}")

    merged = 0
    for name in names:
        age = branch_age_min(name)
        if age < GRACE_MIN:
            print(f"  skip {name} (last push {age:.0f} min ago — likely live)")
            continue
        pr = api("pulls", {
            "title": f"orphan sweep — {name}",
            "description": "auto-merged banked commits from a dead runner",
            "fromBranchOwnerName": "johnnygod",
            "fromBranchRepoName": "hospital-prices",
            "fromBranchName": name,
            "toBranchOwnerName": "johnnygod",
            "toBranchRepoName": "hospital-prices",
            "toBranchName": "main",
        })
        if pr.get("status") != "Success":
            print(f"  {name}: PR failed: {json.dumps(pr)[:200]}")
            continue
        pull = pr["pull_id"]
        op = api(f"pulls/{pull}/merge", method="POST")
        operation = op.get("operation_name")
        if not operation:
            print(f"  {name}: merge not accepted: {json.dumps(op)[:200]}")
            continue
        for _ in range(90):
            time.sleep(20)
            st = api(f"pulls/{pull}/merge?operationName={operation}")
            s = st.get("job_status")
            if s == "Completed":
                merged += 1
                print(f"  {name}: merged (PR #{pull})")
                break
            if s and s.lower() == "failed":
                print(f"  {name}: MERGE FAILED: {json.dumps(st)[:200]}")
                break
        # delete the branch so the sweep is idempotent
        try:
            req = urllib.request.Request(
                f"{BASE}/branches/{urllib.parse.quote(name)}",
                headers={"authorization": f"token {TOKEN}"},
                method="DELETE")
            with urllib.request.urlopen(req) as r:
                r.read()
        except Exception as e:
            print(f"  branch delete failed (non-fatal): {e}")
    print(f"ORPHAN_SWEEP_DONE: {merged} branches merged")


if __name__ == "__main__":
    main()
