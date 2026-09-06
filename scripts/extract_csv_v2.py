"""CSV/XLSX full-schema extraction (v2 field set) for non-JSON MRFs.

One core function: extract_rows_csv(df_row: dict, columns: list[str]) -> list[dict]
Same row schema as scrape_v2.extract_rows. Handles:
- CMS 2.0 CSV: code|1, code|1|type, ... + standard_charge_* columns
- Craneware/proprietary: HCPCS/CPT column + gross/cash columns
- Simple 'code' column files
- Rev codes, modifiers (code|1|modifiers), DRG/APC/NDC code types,
  patient/billing class, payer-specific columns when present

Run tests: .venv/bin/python -m pytest tests/test_extract_csv_v2.py -v
"""
import re
import sys
from pathlib import Path

# v2 row field order (mirror of scrape_v2 REQUIRED set)
FIELDS = [
    "code", "code_prefix", "code_orig", "modifier", "ndc", "apc",
    "rev_code", "internal_code", "billing_class", "patient_class",
    "payer_orig", "plan_orig", "payer_category", "rate", "rate_percent",
    "drug_unit", "drug_quantity",
]

CODE_TYPE_MAP = {
    "CPT": "CPT", "CPT4": "CPT", "HCPCS": "HCPCS", "HCPC": "HCPCS",
    "MSDRG": "MS-DRG", "MS-DRG": "MS-DRG", "APRDRG": "APR-DRG",
    "APC": "APC", "REV": "REV", "REVCODE": "REV", "RC": "REV",
    "ICD": "ICD", "NDC": "NDC", "HIPPS": "HIPPS", "LOCAL": "LOCAL",
}

GROSS_PAT = re.compile(r"gross|list_price|charge_master", re.I)
CASH_PAT = re.compile(r"discounted_cash|cash|self_?pay|discount", re.I)
PAYER_PAT = re.compile(r"standard_charge.*negotiated|negotiated_rate|payer", re.I)
MIN_PAT = re.compile(r"deidentified.*min|^min", re.I)
MAX_PAT = re.compile(r"deidentified.*max|^max", re.I)
PERCENT_PAT = re.compile(r"percent|percentage", re.I)
PAYER_NAME_PAT = re.compile(r"payer_name|plan_name|payer.*label", re.I)
SETTING_PAT = re.compile(r"setting|patient_class", re.I)
BILLING_PAT = re.compile(r"billing_class", re.I)
DRUG_UNIT_PAT = re.compile(r"drug_unit|unit_of_measure", re.I)
DRUG_QTY_PAT = re.compile(r"drug_quantity|^quantity", re.I)
REV_PAT = re.compile(r"rev_code|revenue_code|revcd", re.I)
INTERNAL_PAT = re.compile(r"internal_code|charge_code|procedure_code|local_code|cdm", re.I)
APC_PAT = re.compile(r"^apc|apc_code", re.I)
CODE_PAT = re.compile(r"^code(\||$)", re.I)
TYPE_PAT = re.compile(r"^code\|\d+\|type$", re.I)
MOD_PAT = re.compile(r"code\|\d+\|modifier", re.I)


def to_float(v):
    if v is None or v == "":
        return None
    try:
        f = float(str(v).replace("$", "").replace(",", "").strip())
        return f if f >= 0 else None
    except (TypeError, ValueError):
        return None


def classify(t: str, code: str) -> tuple[str, str]:
    t_norm = str(t or "").upper().replace("-", "").replace("_", "").strip()
    t2 = str(t or "").upper().strip()
    prefix = CODE_TYPE_MAP.get(t2) or CODE_TYPE_MAP.get(t_norm, "")
    if not prefix:
        prefix = re.split(r"\s+", t2)[0] if t2 else "UNKNOWN"
    if prefix in ("MS-DRG", "APR-DRG"):
        m = re.search(r"(\d{2,4})\s*$", code)
        code = m.group(1) if m else code
    return prefix, code


def _row_val(row: dict, pattern) -> str:
    for col, val in row.items():
        if pattern.search(col) and val not in (None, ""):
            return str(val).strip()
    return ""


def extract_rows_csv(row: dict) -> list[dict]:
    """Extract v2-schema rate rows from one CSV/XLSX row (dict col->str val)."""
    row = {str(k).strip(): v for k, v in row.items() if k}
    cols_lower = {c.lower() for c in row}

    # --- collect code records: (code_orig, type, modifiers) ---
    codes = []  # list of (code_orig, type_str, [mods])
    # CMS pipe format: code|N, code|N|type, code|N|modifiers
    pipe_idx = sorted({int(m.group(1)) for c in row
                       for m in [re.match(r"^code\|(\d+)$", c, re.I)] if m})
    for i in pipe_idx:
        co = str(row.get(f"code|{i}", "") or "").strip()
        if not co:
            continue
        t = str(row.get(f"code|{i}|type", "") or "").strip()
        mods_raw = str(row.get(f"code|{i}|modifiers", "") or "").strip()
        mods = re.split(r"[,\s]+", mods_raw) if mods_raw else []
        codes.append((co, t, [m for m in mods if m]))
    if not codes:
        # Craneware/simple: single code column
        for col, val in row.items():
            cl = col.lower()
            if cl in ("hcpcs", "cpt", "cpt4", "medicare_hcpcs") and str(val).strip():
                codes.append((str(val).strip(), "HCPCS" if "hcpcs" in cl else "CPT", []))
            elif cl == "code" and "code|" not in col:
                v = str(val).strip()
                if v and len(v) <= 8:
                    t = "HCPCS" if v and v[0].isalpha() else "CPT"
                    codes.append((v, t, []))
    if not codes:
        return []

    # primary code = first CPT/HCPCS else first
    primary = next((c for c in codes
                    if classify(c[1], c[0])[0] in ("CPT", "HCPCS")), codes[0])
    prefix, code = classify(primary[1], primary[0])
    modifier = " ".join(primary[2]) if primary[2] else None

    # side codes: rev / ndc / apc
    rev_code = ndc = apc = None
    for co, t, _ in codes:
        p, c = classify(t, co)
        if p in ("REV",):
            rev_code = c
        elif p == "NDC":
            ndc = c
        elif p == "APC":
            apc = c

    # --- row-level extras ---
    rv = _row_val(row, REV_PAT)
    if rv and not rev_code:
        rev_code = rv
    internal = _row_val(row, INTERNAL_PAT) or None
    billing = _row_val(row, BILLING_PAT) or None
    setting = _row_val(row, SETTING_PAT).lower() or None
    drug_unit = _row_val(row, DRUG_UNIT_PAT) or None
    drug_qty = _row_val(row, DRUG_QTY_PAT) or None
    payer_name = _row_val(row, PAYER_NAME_PAT) or None

    base = {
        "code": code,
        "code_prefix": prefix,
        "code_orig": primary[0],
        "modifier": modifier,
        "ndc": ndc,
        "apc": apc,
        "rev_code": rev_code,
        "internal_code": internal,
        "billing_class": billing,
        "patient_class": setting,
        "drug_unit": drug_unit,
        "drug_quantity": drug_qty,
    }

    rows = []
    seen_cols = set()
    for col, val in row.items():
        cl = col.lower()
        v = to_float(val)
        if cl in seen_cols:
            continue
        # explicit named columns
        if GROSS_PAT.search(cl) and not CASH_PAT.search(cl) and v is not None:
            r = dict(base, payer_orig=None, plan_orig=None, payer_category="gross",
                     rate=v, rate_percent=None)
            rows.append(r)
        elif CASH_PAT.search(cl) and v is not None:
            r = dict(base, payer_orig=None, plan_orig=None, payer_category="cash",
                     rate=v, rate_percent=None)
            rows.append(r)
        elif MIN_PAT.search(cl) and v is not None:
            rows.append(dict(base, payer_orig=col, plan_orig=None,
                             payer_category="min", rate=v, rate_percent=None))
        elif MAX_PAT.search(cl) and v is not None:
            rows.append(dict(base, payer_orig=col, plan_orig=None,
                             payer_category="max", rate=v, rate_percent=None))
        elif PAYER_PAT.search(cl) and not PERCENT_PAT.search(cl) and v is not None:
            r = dict(base, payer_orig=payer_name or col, plan_orig=None,
                     payer_category="payer", rate=v, rate_percent=None)
            rows.append(r)
        elif PAYER_PAT.search(cl) and PERCENT_PAT.search(cl) and v is not None:
            rows.append(dict(base, payer_orig=payer_name or col, plan_orig=None,
                             payer_category="percent", rate=None, rate_percent=v))
    return rows


if __name__ == "__main__":
    print("module only - see tests/test_extract_csv_v2.py", file=sys.stderr)
