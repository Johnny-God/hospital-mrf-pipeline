#!/usr/bin/env python3
"""scrape_v2: full-schema MRF extractor (DoltHub transparency-in-pricing field set).

One function core: extract_rows(item) -> list of rate rows. Row schema:
  code, code_prefix, code_orig, modifier, ndc, apc, rev_code, internal_code,
  billing_class, patient_class, payer_orig, plan_orig, payer_category,
  rate, rate_percent, drug_unit, drug_quantity

Handles JSON MRFs (CMS v2 spec + common variants). CLI:
  .venv/bin/python scripts/scrape_v2.py --ccn 110029 --url https://... [--limit-hospitals 100]
"""
import ast
import re
import sys
from pathlib import Path

import click
import ijson
import requests

PROJECT = Path(__file__).parent.parent

CHARGE_PATHS = [
    "standard_charge_information.item", "charges.item", "standard_charges.item", "item",
]
CODE_TYPE_MAP = {  # (type string normalized) -> code_prefix
    "CPT": "CPT", "CPT4": "CPT", "HCPCS": "HCPCS", "HCPC": "HCPCS",
    "MSDRG": "MS-DRG", "MS-DRG": "MS-DRG", "APRDRG": "APR-DRG",
    "APC": "APC", "REV": "REV", "REVCODE": "REV", "ICD": "ICD",
    "NDC": "NDC", "HIPPS": "HIPPS", "LOCAL": "LOCAL", "RC": "REV",
}
NON_PAYER = {
    "gross", "cash", "discounted cash", "discounted_cash", "discounted cash price",
    "min", "minimum", "max", "maximum",
    "deidentified minimum", "deidentified maximum", "deidentified min", "deidentified max",
}
PAYER_KEYS = ["payer_name", "payer", "insurer", "payor"]
PLAN_KEYS = ["plan_name", "plan_id", "plan", "tier"]
PRICE_KEYS = ["standard_charge", "negotiated_rate", "negotiated_price", "negotiated_amount", "rate", "amount"]
PCT_KEYS = ["standard_charge_percent", "percent_of_charge", "rate_percent"]
SETTING_KEYS = ["setting", "service_setting"]
BILLING_KEYS = ["billing_class", "billing_class_modifier"]
DRUG_UNIT_KEYS = ["drug_unit_of_measurement", "drug_unit", "unit_of_measurement"]
DRUG_QTY_KEYS = ["drug_quantity", "quantity", "drug_amount"]
REV_KEYS = ["rev_code", "revenue_code", "revcd"]
INTERNAL_KEYS = ["internal_code", "charge_code", "procedure_code", "local_code", "hospital_code"]
APC_KEYS = ["apc", "apc_code", "ambulatory_payment_classification"]

CODE_SPLIT = re.compile(r"\s+")


def pick(d: dict, keys) -> str:
    for k in keys:
        if d.get(k) not in (None, ""):
            return str(d[k]).strip()
    return ""


def to_float(v):
    if v in (None, ""):
        return None
    try:
        f = float(str(v).replace("$", "").replace(",", "").strip())
        return f if f >= 0 else None
    except (TypeError, ValueError):
        return None


def classify_code(type_str: str, code_orig: str) -> tuple[str, str]:
    """Return (code_prefix, code). Normalizes messy type strings; extracts code
    from compound originals like 'MS-DRG V32 (FY 2021) 003'."""
    t = str(type_str or "").upper().replace("-", "").replace("_", "").strip()
    # try map with original hyphens too
    t2 = str(type_str or "").upper().strip()
    prefix = CODE_TYPE_MAP.get(t2) or CODE_TYPE_MAP.get(t, "")
    if not prefix:
        # unknown: use first word of the type as prefix
        prefix = CODE_SPLIT.split(t2)[0] if t2 else "UNKNOWN"
    code = code_orig
    if prefix in ("MS-DRG", "APR-DRG"):
        m = re.search(r"(\d{2,4})\s*$", code_orig)
        code = m.group(1) if m else code_orig
    elif prefix == "NDC":
        code = code_orig  # keep dash format
    return prefix, code


def _flatten_payers(sc: dict):
    """Yield payer dicts from payers_information (list or string-encoded list)
    or from direct payer fields on the charge dict (gross/cash are handled separately)."""
    pi = sc.get("payers_information")
    if pi:
        try:
            lst = pi if isinstance(pi, list) else ast.literal_eval(str(pi))
            for p in lst:
                if isinstance(p, dict):
                    yield p
            return
        except (ValueError, SyntaxError):
            return
    payer = pick(sc, PAYER_KEYS)
    if payer:
        yield sc


def extract_rows(item: dict) -> list[dict]:
    """Extract all rate rows from one MRF item dict. Full DoltHub field set."""
    if not isinstance(item, dict):
        return []

    # --- codes ---
    code_orig = code = prefix = modifier = ndc = apc = rev_code = internal_code = None
    cis = item.get("code_information") or []
    code_recs = [ci for ci in cis if isinstance(ci, dict) and str(ci.get("code", "")).strip()]
    if code_recs:
        # prefer CPT/HCPCS as primary; fall back to first
        primary = next(
            (ci for ci in code_recs
             if str(ci.get("type", "")).upper().replace("-", "") in ("CPT", "CPT4", "HCPCS", "HCPC")),
            code_recs[0],
        )
        co = str(primary.get("code", "")).strip()
        prefix, code = classify_code(primary.get("type"), co)
        code_orig = co
        mod_list = []
        primary_norm = str(primary.get("type", "")).upper().replace("-", "").replace("_", "")
        for ci in code_recs:
            if str(ci.get("type", "")).upper().replace("-", "").replace("_", "") == primary_norm:
                mod_list += [str(m).strip() for m in mods_of(ci) if str(m).strip()]
        modifier = " ".join(mod_list) if mod_list else None
        for ci in code_recs:
            t = str(ci.get("type", "")).upper().replace("-", "").replace("_", "")
            p2, c2 = classify_code(ci.get("type"), str(ci.get("code", "")).strip())
            if p2 == "REV":
                rev_code = c2
            elif p2 == "NDC":
                ndc = c2
            elif p2 == "APC":
                apc = c2
    else:
        # no code_information: fall back to item-level code (internal)
        co = str(item.get("code", "")).strip()
        if co:
            internal_code = co

    # item-level internal/charge code alongside standard codes
    if not internal_code:
        ic = pick(item, INTERNAL_KEYS)
        if ic and ic != code:
            internal_code = ic

    # --- rates ---
    rows = []
    for sc in item.get("standard_charges", []) or []:
        if not isinstance(sc, dict):
            continue
        base = {
            "code": code,
            "code_prefix": prefix,
            "code_orig": code_orig,
            "modifier": modifier,
            "ndc": ndc,
            "apc": apc,
            "rev_code": rev_code if rev_code else pick(sc, REV_KEYS) or None,
            "internal_code": internal_code,
            "billing_class": pick(sc, BILLING_KEYS) or pick(item, BILLING_KEYS) or None,
            "patient_class": (pick(sc, SETTING_KEYS) or pick(item, SETTING_KEYS) or "").lower() or None,
            "drug_unit": pick(sc, DRUG_UNIT_KEYS) or None,
            "drug_quantity": pick(sc, DRUG_QTY_KEYS) or None,
            "rate_percent": None,
        }
        payer_cands = list(_flatten_payers(sc))
        # named categories
        for key, cat in (("gross_charge", "gross"), ("discounted_cash", "cash")):
            if sc.get(key) not in (None, ""):
                v = to_float(sc.get(key))
                if v is not None:
                    r = dict(base)
                    r.update(payer_orig=None, plan_orig=None, payer_category=cat,
                             rate=v, rate_percent=None)
                    rows.append(r)
        # payer rows
        for p in payer_cands:
            payer_name = pick(p, PAYER_KEYS)
            if not payer_name:
                continue
            cat = payer_name.lower().strip()
            cat_map = {"min": "min", "minimum": "min", "max": "max", "maximum": "max",
                       "deidentified minimum": "min", "deidentified maximum": "max",
                       "deidentified min": "min", "deidentified max": "max"}
            r = dict(base)
            r["payer_orig"] = payer_name
            r["plan_orig"] = pick(p, PLAN_KEYS) or None
            pct = to_float(next((p[k] for k in PCT_KEYS if p.get(k) not in (None, "")), None))
            price = to_float(next((p[k] for k in PRICE_KEYS if p.get(k) not in (None, "")), None))
            if cat in cat_map:
                r["payer_category"] = cat_map[cat]
                r["rate"] = price
            elif price is not None:
                r["payer_category"] = "payer"
                r["rate"] = price
            elif pct is not None:
                r["payer_category"] = "percent"
                r["rate"] = None
                r["rate_percent"] = pct
            else:
                continue
            rows.append(r)
    return rows


def mods_of(ci: dict):
    return ci.get("modifiers") or []


def mods_clean(mods):
    return [m for m in mods if m]


def mods_filter(mods):
    return mods


def stream_v2(url: str, timeout: int = 180):
    """Stream a JSON MRF, yield full-schema rows. Reuses payer_pilot temp-file pattern."""
    resp = requests.get(url, timeout=timeout, stream=True, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    import tempfile
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
            if first is None:
                continue
            with open(tmp, "rb") as f:
                for item in ijson.items(f, path):
                    for r in extract_rows(item):
                        yield r
            return
    finally:
        tmp.unlink(missing_ok=True)


def stream_v2_bytes(raw: bytes):
    """Yield full-schema rows from an in-memory JSON MRF (async runner side)."""
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
        tf.write(raw)
        tmp = Path(tf.name)
    try:
        for path in CHARGE_PATHS:
            with open(tmp, "rb") as f:
                try:
                    first = next(ijson.items(f, path), None)
                except ijson.JSONError:
                    first = None
            if first is None:
                continue
            with open(tmp, "rb") as f:
                for item in ijson.items(f, path):
                    for r in extract_rows(item):
                        yield r
            return
    finally:
        tmp.unlink(missing_ok=True)


if __name__ == "__main__":
    import sys
    print("use scrape_v2 CLI below", file=sys.stderr)
