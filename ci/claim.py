#!/usr/bin/env python3
"""Shared work queue stored in git (ci/queue.json) — the coordination DB.

States: pending -> claimed -> done | failed (attempts>=3)
Optimistic locking: pull --rebase, mutate, commit, push; on race, undo + retry.
A runner dies with hospitals claimed? Stale claims (>45 min) revert to pending
on the next claim call. Preflight rebuilds queue.json from ground truth each
sweep, so the queue can never permanently drift from reality.

Subcommands:
  claim <runner-id>   -> prints claimed CCNs (one per line), "QUEUE_EMPTY", or raises
  done  <runner-id> <ccn> [<ccn>...]  -> mark hospitals done (after verified push)
  fail  <runner-id> <ccn>             -> increment attempts; back to pending (or failed at 3)
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).parent.parent
QUEUE = REPO / "ci" / "queue.json"
CLAIM_BATCH = int(os.environ.get("CLAIM_BATCH", "5"))
STALE_MIN = 45
MAX_ATTEMPTS = 3


def sh(*args, check=True):
    return subprocess.run(args, cwd=REPO, capture_output=True, text=True, check=check)


def sync():
    sh("git", "pull", "--rebase", "-q", "origin", "main")


def _commit(msg: str) -> bool:
    """Commit queue.json; push; on race undo our commit and return False."""
    sh("git", "add", "ci/queue.json")
    sh("git", "commit", "-q", "-m", msg)
    p = sh("git", "push", "-q", "origin", "main", check=False)
    if p.returncode == 0:
        return True
    sh("git", "reset", "-q", "--hard", "HEAD~1")  # undo, retry fresh
    return False


def _load() -> list:
    return json.load(open(QUEUE))


def _save(q: list):
    QUEUE.write_text(json.dumps(q, indent=1))


def cmd_claim(runner: str):
    now = datetime.now(timezone.utc)
    for _ in range(25):
        sync()
        q = _load()
        changed = False
        for e in q:
            if e.get("status") == "claimed":
                ts = datetime.fromisoformat(e["ts"])
                if (now - ts).total_seconds() / 60 > STALE_MIN:
                    e["status"] = "pending"  # dead runner's claim expires
                    changed = True
        pending = [e for e in q if e["status"] == "pending"]
        if not pending:
            if changed:
                _save(q)
                if _commit(f"queue: stale claims released"):
                    continue  # released; loop again to confirm empty
            print("QUEUE_EMPTY")
            return
        take = pending[:CLAIM_BATCH]
        for e in take:
            e.update(status="claimed", claimed_by=runner, ts=now.isoformat(), attempts=e.get("attempts", 0) + 1)
        _save(q)
        if _commit(f"queue: {runner} claims {', '.join(e['ccn'] for e in take)}"):
            for e in take:
                print(e["ccn"])
            return
        # lost the race — loop and retry against the winner's version
    raise SystemExit("claim: too many races, giving up")


def cmd_done(runner: str, ccns: list[str]):
    for _ in range(25):
        sync()
        q = _load()
        n = 0
        for e in q:
            if e["ccn"] in ccns and e.get("claimed_by") == runner and e["status"] == "claimed":
                e["status"] = "done"
                e["done_ts"] = datetime.now(timezone.utc).isoformat()
                n += 1
        if n == 0:
            print(f"done: nothing to mark for {ccns} (already handled?)")
            return
        _save(q)
        if _commit(f"queue: {runner} done {', '.join(sorted(ccns))}"):
            return
    raise SystemExit("done: too many races")


def cmd_fail(runner: str, ccns: list[str]):
    for _ in range(25):
        sync()
        q = _load()
        for e in q:
            if e["ccn"] in ccns and e.get("claimed_by") == runner:
                e["attempts"] = e.get("attempts", 1)
                e["status"] = "failed" if e["attempts"] >= MAX_ATTEMPTS else "pending"
                e["claimed_by"] = ""
        _save(q)
        if _commit(f"queue: {runner} fail {', '.join(sorted(ccns))}"):
            return
    raise SystemExit("fail: too many races")


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "claim":
        cmd_claim(sys.argv[2])
    elif cmd == "done":
        cmd_done(sys.argv[2], sys.argv[3:])
    elif cmd == "fail":
        cmd_fail(sys.argv[2], sys.argv[3:])
    else:
        raise SystemExit(f"unknown subcommand {cmd}")
