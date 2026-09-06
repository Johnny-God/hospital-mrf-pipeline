#!/usr/bin/env bash
# convert+import any new data-v2 jsonl -> dolt, commit, clean CSV. One cycle per call.
set -u
HERE="/home/ubuntu/data broker/hospital-prices"
DOLT="/home/ubuntu/data broker/hospital-prices-dolt"
LOCK=/tmp/hpt-pipeline.lock
exec 9>"$LOCK"
flock -n 9 || { echo "import: locked, skip"; exit 0; }
cd "$HERE"
mapfile -t NEWFILES < <(find data-v2 -name "*.jsonl" -size +0c -newer .last_import_stamp 2>/dev/null)
[ "${#NEWFILES[@]}" -eq 0 ] && exit 0
.venv/bin/python scripts/v2_to_dolt_csv.py "${NEWFILES[@]}" >> logs/v2_full.log 2>&1
(cd "$DOLT" && dolt table import -a --continue \
  --columns ccn,code,code_prefix,code_orig,modifier,ndc,apc,rev_code,internal_code,billing_class,patient_class,payer_orig,plan_orig,payer_category,standard_charge,rate_percent,drug_unit,drug_quantity \
  rate /tmp/dolt-import/rate_v2.csv >> "$HERE/logs/v2_full.log" 2>&1 \
  && dolt add -A && dolt commit -m "pipeline cycle: +batch $(date -Iseconds)" >> "$HERE/logs/v2_full.log" 2>&1)
touch "$HERE/.last_import_stamp"
rm -f /tmp/dolt-import/rate_v2.csv
echo "$(date -Iseconds) imported ${#NEWFILES[@]} hospitals"
