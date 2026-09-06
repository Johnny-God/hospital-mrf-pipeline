#!/usr/bin/env bash
# hospital-prices pipeline sequencer: scrape batch -> import batch, with lock
# so scrape and import never run concurrently (the OOM rule).
# Usage: pipeline_cycle.sh <workers> <batch_size> <max_age_days>
# Designed to be called repeatedly (cron / loop); each call = one batch.
set -u
HERE="/home/ubuntu/data broker/hospital-prices"
DOLT="/home/ubuntu/data broker/hospital-prices-dolt"
LOCK=/tmp/hpt-pipeline.lock
WORKERS="${1:-8}"
BATCH="${2:-500}"
MAXAGE="${3:-0}"

exec 9>"$LOCK"
if ! flock -n 9; then
  echo "$(date -Iseconds) another pipeline cycle running, exiting"
  exit 0
fi

# 1. scrape batch (resumable; freshness skip applies)
cd "$HERE"
.venv/bin/python scripts/v2_scrape_all.py --workers "$WORKERS" --batch-size "$BATCH" \
  --max-age-days "$MAXAGE" --formats json,csv,xlsx,zip,other >> logs/v2_full.log 2>&1
rc=$?
echo "$(date -Iseconds) scrape batch rc=$rc"

# 2. convert + import if any new jsonl landed
mapfile -t NEWFILES < <(find data-v2 -name "*.jsonl" -size +0c -newer .last_import_stamp 2>/dev/null)
if [ "${#NEWFILES[@]}" -gt 0 ]; then
  .venv/bin/python scripts/v2_to_dolt_csv.py "${NEWFILES[@]}" >> logs/v2_full.log 2>&1
  (cd "$DOLT" && dolt table import -a --continue \
    --columns ccn,code,code_prefix,code_orig,modifier,ndc,apc,rev_code,internal_code,billing_class,patient_class,payer_orig,plan_orig,payer_category,standard_charge,rate_percent,drug_unit,drug_quantity \
    rate /tmp/dolt-import/rate_v2.csv >> "$HERE/logs/v2_full.log" 2>&1 \
    && dolt add -A && dolt commit -m "pipeline cycle: +batch $(date -Iseconds)" >> "$HERE/logs/v2_full.log" 2>&1)
  touch "$HERE/.last_import_stamp"
  rm -f /tmp/dolt-import/rate_v2.csv   # appended-CSV is fully imported; next cycle starts fresh
  echo "$(date -Iseconds) imported ${#NEWFILES[@]} new hospitals"
fi
echo "$(date -Iseconds) cycle complete"
