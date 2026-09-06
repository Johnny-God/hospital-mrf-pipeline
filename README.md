# US Hospital Standard-Charge (MRF) Scraper

GitHub Actions pipeline that scrapes hospital price-transparency machine-readable
files (Hospital Price Transparency Rule, 45 CFR §180), normalizes them to the
CMS 2.0 13-field schema, and pushes them to DoltHub:
**https://dolthub.com/johnnygod/hospital-prices**

## How it works

- `dim/urls/{state}.json` — hospital URL index (CCN, name, MRF URL, transparency page).
- `.github/workflows/scrape.yml` — 10 parallel jobs, sharded by `md5(ccn) % 10`,
  each running the full chain: scrape → normalize → Dolt import → push branch → PR → merge.
- Branches insert only disjoint rows (per-shard CCNs) with deterministic row ids
  (`ccn * 1e9 + n`), so concurrent merges never conflict.
- Runs daily at 06:00 UTC; resumable (skips CCNs already scraped in the same run).
- `scripts/` — the scrapers (JSON MRF streaming via ijson, CSV/XLSX extraction,
  CMS pipe-codes, Craneware columns, DRG/NDC/REV/APC side codes).

## Schema

`hospital(ccn PK, hospital_name, state, file_url, transparency_page)` +
`rate(id PK, ccn, code, code_prefix, code_orig, modifier, ndc, apc, rev_code,
internal_code, billing_class, patient_class, payer_orig, plan_orig,
payer_category, standard_charge, rate_percent, drug_unit, drug_quantity)`

`payer_category`: gross | cash | payer | percent | min | max.
