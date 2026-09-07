#!/usr/bin/env python3
"""Progress dashboard for the hospital scrape queue — reads ci/queue.json,
computes total progress, per-runner stats, and average loop time.

Output: ci/dashboard.html (self-contained, GitHub dark theme, no deps).
Served two ways:
  - committed to the repo by the baseline job -> GitHub Pages renders it
  - locally: python ci/dashboard.py && open ci/dashboard.html

Loop time per runner = mean gap between consecutive done timestamps of that
runner (from the queue's history file ci/queue_history.jsonl, appended by
claim.py done/fail; claimed->done delta is the loop time).
"""
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).parent.parent
QUEUE = REPO / "ci" / "queue.json"
HIST = REPO / "ci" / "queue_history.jsonl"
OUT = REPO / "ci" / "dashboard.html"


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def main():
    q = json.load(open(QUEUE))
    total = len(q)
    by_status = {"done": 0, "claimed": 0, "pending": 0, "failed": 0}
    for e in q:
        by_status[e["status"]] = by_status.get(e["status"], 0) + 1

    # per-runner loop times from history: consecutive done events per runner
    loops = {}  # runner -> [seconds]
    events = {}  # runner -> last done ts
    if HIST.exists():
        for line in HIST.read_text().splitlines():
            try:
                h = json.loads(line)
            except json.JSONDecodeError:
                continue
            r = h.get("runner")
            if h.get("event") == "done" and r:
                ts = parse_ts(h["ts"])
                if r in events:
                    loops.setdefault(r, []).append((ts - events[r]).total_seconds())
                events[r] = ts
                # live claim age for currently-claimed items

    runners = {}
    for e in q:
        if e["status"] == "claimed":
            r = e.get("claimed_by") or "?"
            age = (datetime.now(timezone.utc) - parse_ts(e["ts"])).total_seconds() / 60
            runners.setdefault(r, {"claimed": 0, "oldest_claim_min": 0, "done": 0, "avg_loop_min": None})
            runners[r]["claimed"] += 1
            runners[r]["oldest_claim_min"] = max(runners[r]["oldest_claim_min"], age)
    for r, ls in loops.items():
        runners.setdefault(r, {"claimed": 0, "oldest_claim_min": 0, "done": 0, "avg_loop_min": None})
        runners[r]["done"] = len(ls)
        if ls:
            runners[r]["avg_loop_min"] = statistics.mean(ls) / 60

    pct = 100 * by_status["done"] / total if total else 0
    all_loops = [s for ls in loops.values() for s in ls]
    avg_loop = statistics.mean(all_loops) / 60 if all_loops else None

    rows = ""
    for r in sorted(runners, key=lambda x: -(runners[x]["done"] or 0)):
        d = runners[r]
        loop = f"{d['avg_loop_min']:.1f} min" if d["avg_loop_min"] else "—"
        stale = " ⚠️stale" if d["claimed"] and d["oldest_claim_min"] > 45 else ""
        rows += (f"<tr><td>{r}</td><td>{d['done']}</td><td>{d['claimed']}{stale}</td>"
                 f"<td>{loop}</td></tr>\n")

    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Hospital Scrape Progress</title>
<style>
body {{ background:#0d1117; color:#c9d1d9; font-family:-apple-system,Segoe UI,sans-serif; margin:2rem; }}
h1,h2 {{ color:#58a6ff; }}
.big {{ font-size:2.4rem; font-weight:700; color:#3fb950; }}
.bar {{ background:#21262d; border-radius:6px; height:24px; width:100%; max-width:640px; }}
.bar>div {{ background:#3fb950; height:24px; border-radius:6px; width:{pct:.1f}%; }}
table {{ border-collapse:collapse; margin-top:1rem; }}
td,th {{ border:1px solid #30363d; padding:6px 14px; text-align:left; }}
th {{ background:#161b22; color:#58a6ff; }}
.mut {{ color:#8b949e; }}
.warn {{ color:#d29922; }}
</style></head><body>
<h1>🏥 Hospital Scrape Progress</h1>
<p class="big">{by_status['done']} / {total} hospitals ({pct:.1f}%)</p>
<div class="bar"><div></div></div>
<p>✅ done: {by_status['done']} &nbsp; 🔄 claimed: {by_status['claimed']} &nbsp;
⏳ pending: {by_status['pending']} &nbsp; ❌ failed: {by_status['failed']}</p>
<p class="mut">Average loop time (all runners): <b>{f"{avg_loop:.1f} min" if avg_loop else "—"}</b></p>
<h2>Runners</h2>
<table><tr><th>Runner</th><th>Done (hospitals)</th><th>Now claiming</th><th>Avg loop</th></tr>
{rows}</table>
<p class="mut">Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} from ci/queue.json ·
loop = mean gap between a runner's consecutive completed hospitals</p>
</body></html>"""
    OUT.write_text(html)
    print(f"DASHBOARD: {OUT} — {by_status['done']}/{total} done, "
          f"{len(runners)} runners, avg loop {f'{avg_loop:.1f}min' if avg_loop else '—'}")


if __name__ == "__main__":
    main()
