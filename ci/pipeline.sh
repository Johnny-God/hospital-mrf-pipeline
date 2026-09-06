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

echo "=== 1. scrape shard $SHARD/$TOTAL ==="
python scripts/shard_runner_async.py --shard "$SHARD" --total "$TOTAL" --concurrency 4

mapfile -t FILES < <(find data-v2 -name '*.jsonl' -size +0c | sort)
ok=${#FILES[@]}
echo "=== scraped hospitals on disk: $ok ==="
[ "$ok" -gt 0 ] || { echo "nothing scraped, exiting 0"; exit 0; }

echo "=== 2. seed-based work repo (tiny fetch, never clone main) ==="
mkdir -p /tmp/doltwork && cd /tmp/doltwork
dolt init
dolt remote add origin "https://doltremoteapi.dolthub.com/$DB"
dolt fetch origin seed
dolt checkout -b ciwork remotes/origin/seed

echo "=== 3. checkpointed batches of $BATCH ==="
K=0
for ((i = 0; i < ok; i += BATCH)); do
  K=$((K + 1))
  CHUNK=("${FILES[@]:i:BATCH}")
  n=${#CHUNK[@]}
  echo "--- batch $K: $n hospitals ---"
  rm -f /tmp/dolt-import/rate_v2.csv
  python "$REPO/scripts/v2_to_dolt_csv.py" "${CHUNK[@]}"
  python "$REPO/ci/gen_hospital_csv.py" "${CHUNK[@]}"
  dolt table import -a --columns "ccn,hospital_name,state,file_url,transparency_page" hospital "$REPO/ci/hospital.csv"
  dolt table import -a --columns "id,ccn,code,code_prefix,code_orig,modifier,ndc,apc,rev_code,internal_code,billing_class,patient_class,payer_orig,plan_orig,payer_category,standard_charge,rate_percent,drug_unit,drug_quantity" rate /tmp/dolt-import/rate_v2.csv
  dolt add -A
  dolt commit -m "shard $SHARD/$TOTAL batch $K: $n hospitals"
  BRANCH="ci/shard-${SHARD}-b${K}-$(date +%s)"
  dolt push origin "ciwork:$BRANCH"
  python "$REPO/ci/open_pr.py" "$BRANCH" "$SHARD"
done

echo "=== PIPELINE_DONE: shard=$SHARD batches=$K ==="
