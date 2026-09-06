#!/usr/bin/env python3
"""One-off pilot: extract payer-specific negotiated rates from GA JSON-format MRFs.

Streams each JSON MRF, emits one row per (code, payer, plan) into
data-payer/GA/{CCN}.jsonl and prints a size/coverage tally.

Schema: {"cpt", "payer", "plan", "setting", "price"}
Run: .venv/bin/python scripts/payer_pilot.py [--state GA]
"""

import ast
import itertools
import json
import re
import sys
import tempfile
from pathlib import Path

import click
import ijson
import requests

PROJECT = Path(__file__).parent.parent
OUT_BASE = PROJECT / "data-payer"
CHARGE_PATHS = ["standard_charge_information.item", "charges.item", "standard_charges.item", "item"]
PLAN_KEYS = ["plan_name", "plan_id", "plan", "tier"]
PAYER_KEYS = ["payer_name", "payer", "insurer", "payor"]
PRICE_KEYS = ["negotiated_rate", "negotiated_price", "negotiated_amount", "standard_charge_dollar", "rate", "amount", "standard_charge", "price"]
SETTING_KEYS = ["setting", "service_setting"]
# payer/plan names that are NOT real negotiated rates
NON_PAYER = {"gross", "cash", "discounted cash", "discounted_cash", "min", "minimum", "max", "maximum", "deidentified minimum", "deidentified maximum"}


def pick(d: dict, keys: list[str]) -> str:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return str(d[k]).strip()
    return ""


def to_float(v) -> float | None:
    try:
        f = float(v)
        return f if f >= 0 else None
    except (TypeError, ValueError):
        return None


def iter_std_charges(item: dict):
    """Yield payer-specific charge dicts from an item's standard_charges entries."""
    for sc in item.get("standard_charges", []) or []:
        if not isinstance(sc, dict):
            continue
        # nested payers_information (string-encoded list in some files)
        pi = sc.get("payers_information")
        if pi:
            try:
                lst = pi if isinstance(pi, list) else ast.literal_eval(str(pi))
                for p in lst:
                    if isinstance(p, dict):
                        yield p
            except (ValueError, SyntaxError):
                continue
        # direct payer fields on the charge dict
        else:
            payer = pick(sc, PAYER_KEYS).lower()
            if payer and payer not in NON_PAYER:
                yield sc


def stream_rows(url: str, timeout: int = 120):
    """Stream a JSON MRF, yield payer rows. Handles large files via temp file."""
    resp = requests.get(url, timeout=timeout, stream=True, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
        for chunk in resp.iter_content(1 << 20):
            tf.write(chunk)
        tmp = Path(tf.name)
    try:
        for path in CHARGE_PATHS:
            with open(tmp, "rb") as f:
                try:
                    first = next(ijson.items(f, path), None)
                except ijson.JSONError:
                    first = None
            if first is not None:
                with open(tmp, "rb") as f:
                    for item in ijson.items(f, path):
                        if not isinstance(item, dict):
                            continue
                        codes = [
                            str(ci.get("code", "")).strip()
                            for ci in (item.get("code_information") or [])
                            if isinstance(ci, dict)
                            and str(ci.get("type", "")).upper().replace("-", "") in {"CPT", "CPT4", "HCPCS", "HCPC"}
                            and ci.get("code")
                        ] or ([str(item.get("code", "")).strip()] if item.get("code") else [])
                        if not codes:
                            continue
                        for p in iter_std_charges(item):
                            payer = pick(p, PAYER_KEYS)
                            if not payer:
                                continue
                            price = to_float(pick(p, PRICE_KEYS).replace("$", "").replace(",", ""))
                            if price is None:
                                continue
                            for c in codes:
                                yield {
                                    "cpt": c,
                                    "payer": payer,
                                    "plan": pick(p, PLAN_KEYS),
                                    "setting": pick(p, SETTING_KEYS),
                                    "price": price,
                                }
                return
    finally:
        tmp.unlink(missing_ok=True)


@click.command()
@click.option("--state", default="GA")
@click.option("--limit", default=None, type=int, help="Only first N hospitals (test)")
def main(state: str, limit: int | None):
    urls = json.load(open(PROJECT / "dim" / "urls" / f"{state.lower()}.json"))
    json_hospitals = [r for r in urls if ".json" in r["file_url"].lower()]
    if limit:
        json_hospitals = json_hospitals[:limit]
    outdir = OUT_BASE / state.upper()
    outdir.mkdir(parents=True, exist_ok=True)

    totals = {"hospitals": 0, "ok": 0, "rows": 0, "no_payer_rows": 0, "failed": 0, "bytes": 0}
    payers: set[str] = set()
    for r in json_hospitals:
        ccn, name, url = r["ccn"], r["hospital_name"], r["file_url"]
        totals["hospitals"] += 1
        try:
            n = 0
            with open(outdir / f"{ccn}.jsonl", "w") as f:
                for row in stream_rows(url):
                    f.write(json.dumps(row) + "\n")
                    n += 1
                    payers.add(row["payer"])
            if n:
                totals["ok"] += 1
                totals["rows"] += n
                totals["bytes"] += (outdir / f"{ccn}.jsonl").stat().st_size
                print(f"  {ccn} {name[:38]:40s} {n:>8d} rows")
            else:
                totals["no_payer_rows"] += 1
                (outdir / f"{ccn}.jsonl").unlink()  # nothing gained
                print(f"  {ccn} {name[:38]:40s} 0 payer rows")
        except Exception as e:
            totals["failed"] += 1
            print(f"  {ccn} {name[:38]:40s} FAIL: {type(e).__name__}: {str(e)[:80]}")

    print("\n=== GA payer pilot tally ===")
    for k, v in totals.items():
        print(f"{k:16s} {v}")
    print(f"{'payers_seen':16s} {len(payers)}")
    print(f"{'output_mb':16s} {totals['bytes'] / 1e6:.1f}")
    # national extrapolation: GA is 141/3768 of hospitals, but JSON share varies
    if totals["rows"]:
        print(f"\nnational est. rows:  {totals['rows'] / len(json_hospitals) * 3768:,.0f}")
        print(f"national est. size:  {totals['bytes'] / len(json_hospitals) * 3768 / 1e9:.1f} GB (jsonl)")
        print(f"(CSV-format hospitals not counted — separate wide-format mapping needed)")


if __name__ == "__main__":
    main()
