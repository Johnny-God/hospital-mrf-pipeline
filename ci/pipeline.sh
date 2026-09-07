#!/usr/bin/env bash
# One runner = one shard = full chain in CHECKPOINTED batches:
#   scrape -> [convert -> dolt import -> commit -> push -> PR+merge] x N batches
# Runners NEVER clone main (grows unbounded) — they fetch the schema-only `seed`
# branch (few MB) for shared PR ancestry. Deterministic ids (ccn*1e9+n) keep merges
# conflict-free. Checkpoint every BATCH hospitals: a runner kill loses <= BATCH,
# and no single repo ever has to fit the runner disk.
set -euo pipefail

SHARD="${SHARD:?}"
TOTAL="${TOTAL:-50}"
DB="johnnygod/hospital-prices"
REPO="$(pwd)"
BATCH=25

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

echo "=== 2. scrape + bank loop (every $BATCH hospitals) ==="
CYCLE=0
while :; do
  CYCLE=$((CYCLE + 1))
  before=$(find "$REPO/data-v2" -name '*.jsonl' -size +0c 2>/dev/null | wc -l)
  (cd "$REPO" && python scripts/shard_runner_async.py --shard "$SHARD" --total "$TOTAL" \
    --concurrency 4 --max-hospitals "$BATCH")
  mapfile -t FILES < <(find "$REPO/data-v2" -name '*.jsonl' -size +0c | sort)
  ok=${#FILES[@]}
  if [ "$ok" -eq 0 ]; then echo "nothing new this cycle; shard dry"; break; fi
  if [ "$ok" -eq "$before" ] && [ "$CYCLE" -gt 1 ]; then
    echo "no new outputs since last cycle; shard dry"; break
  fi

  echo "=== bank cycle $CYCLE: $ok hospitals on disk ==="
  rm -f /tmp/dolt-import/rate_v2.csv
  python "$REPO/scripts/v2_to_dolt_csv.py" "${FILES[@]}"
  (cd "$REPO" && python ci/gen_hospital_csv.py "${FILES[@]}")
  dolt table import -a --columns "ccn,hospital_name,state,file_url,transparency_page" hospital "$REPO/ci/hospital.csv" || true
  dolt table import -a --columns "id,ccn,code,code_prefix,code_orig,modifier,ndc,apc,rev_code,internal_code,billing_class,patient_class,payer_orig,plan_orig,payer_category,standard_charge,rate_percent,drug_unit,drug_quantity" rate /tmp/dolt-import/rate_v2.csv
  dolt add -A
  dolt commit -m "shard $SHARD/$TOTAL bank $CYCLE: $ok hospitals"
  BRANCH="ci/shard-${SHARD}-b${CYCLE}-$(date +%s)"
  dolt push origin "ciwork:$BRANCH"
  python "$REPO/ci/open_pr.py" "$BRANCH" "$SHARD"
  # clear banked outputs so the next cycle only re-scrapes failures/new work
  find "$REPO/data-v2" -name '*.jsonl' -size +0c -delete
done

echo "=== PIPELINE_DONE: shard=$SHARD cycles=$CYCLE ==="
