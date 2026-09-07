#!/usr/bin/env bash
# One runner = one shard. Banks EVERY hospital: scrape 1 -> import -> commit ->
# push to this run's branch (the push IS the bank) -> repeat until shard dry.
# ONE PR+merge at the end. Runner rotation mid-run loses at most the hospital
# being scraped; its already-pushed commits sit safely on DoltHub and get merged
# by the preflight orphan-sweeper (ci/merge_orphans.py) next run.
# Never clones main (won't fit runner disk) — fetches the schema-only `seed`.
set -euo pipefail

SHARD="${SHARD:?}"
TOTAL="${TOTAL:-50}"
DB="johnnygod/hospital-prices"
REPO="$(pwd)"
RUN_ID="$(date +%Y%m%d-%H%M%S)"
BRANCH="ci/shard-${SHARD}-${RUN_ID}"

echo "=== 0. dolt install check + creds ==="
dolt version
mkdir -p ~/.dolt/creds
printf '%s' "$DOLT_CREDS_JWK" > ~/.dolt/creds/ci.jwk
dolt config --global --add user.creds ci
dolt config --global --add user.email "bot@johnnygod.dev"
dolt config --global --add user.name "actions-ci"
dolt creds check

echo "=== 1. seed-based work repo (tiny fetch, never clone main) ==="
mkdir -p /tmp/doltwork && cd /tmp/doltwork
dolt init
dolt remote add origin "https://doltremoteapi.dolthub.com/$DB"
dolt fetch origin seed
dolt checkout -b ciwork remotes/origin/seed

echo "=== 2. bank loop: one hospital per cycle -> branch $BRANCH ==="
BANKED=0
CYCLE=0
while :; do
  CYCLE=$((CYCLE + 1))
  (cd "$REPO" && python scripts/shard_runner_async.py --shard "$SHARD" --total "$TOTAL" \
    --concurrency 4 --max-hospitals 1)
  FILES=$(find "$REPO/data-v2" -name '*.jsonl' -size +0c 2>/dev/null || true)
  if [ -z "$FILES" ]; then echo "shard dry after $CYCLE cycles"; break; fi

  echo "--- bank cycle $CYCLE ---"
  rm -f /tmp/dolt-import/rate_v2.csv
  # shellcheck disable=SC2086
  python "$REPO/scripts/v2_to_dolt_csv.py" $FILES
  # shellcheck disable=SC2086
  (cd "$REPO" && python ci/gen_hospital_csv.py $FILES)
  dolt table import -a --columns "ccn,hospital_name,state,file_url,transparency_page" hospital "$REPO/ci/hospital.csv" || true
  dolt table import -a --columns "id,ccn,code,code_prefix,code_orig,modifier,ndc,apc,rev_code,internal_code,billing_class,patient_class,payer_orig,plan_orig,payer_category,standard_charge,rate_percent,drug_unit,drug_quantity" rate /tmp/dolt-import/rate_v2.csv
  dolt add -A
  CCN=$(basename "$FILES" .jsonl)
  dolt commit -m "shard $SHARD hospital $CCN" || { echo "no changes; skipping push"; continue; }
  dolt push origin "ciwork:$BRANCH"
  BANKED=$((BANKED + 1))
  # clear banked output so the next cycle scrapes the next hospital
  # shellcheck disable=SC2086
  rm -f $FILES
done

echo "=== 3. PR + merge ($BANKED hospitals on branch) ==="
if [ "$BANKED" -gt 0 ]; then
  python "$REPO/ci/open_pr.py" "$BRANCH" "$SHARD"
else
  echo "nothing banked; no PR"
fi

echo "=== PIPELINE_DONE: shard=$SHARD banked=$BANKED ==="
