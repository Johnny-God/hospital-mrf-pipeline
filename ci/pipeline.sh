#!/usr/bin/env bash
# One runner = one shard = one full chain: scrape -> convert -> dolt import -> push -> PR -> merge.
# Branches only insert disjoint rows (md5(ccn)%total sharding) with deterministic ids, so merges never conflict.
set -euo pipefail

SHARD="${SHARD:?}"
TOTAL="${TOTAL:-10}"
DB="johnnygod/hospital-prices"
BRANCH="ci/shard-${SHARD}-$(date +%Y%m%d-%H%M%S)"
REPO="$(pwd)"

echo "=== 0. dolt install check + creds ==="
dolt version
mkdir -p ~/.dolt/creds
echo "$DOLT_CREDS_JWK" > ~/.dolt/creds/ci.jwk
dolt config --global --add user.creds ci.jwk
dolt config --global --add user.email "bot@johnnygod.dev"
dolt config --global --add user.name "actions-ci"
dolt creds check

echo "=== 1. scrape shard $SHARD/$TOTAL ==="
python scripts/shard_runner_async.py --shard "$SHARD" --total "$TOTAL" --concurrency 4

ok=$(find data-v2 -name '*.jsonl' -size +0c | wc -l)
echo "=== scraped hospitals on disk: $ok ==="
[ "$ok" -gt 0 ] || { echo "nothing scraped, exiting 0"; exit 0; }

echo "=== 2. convert to CSV (deterministic ids) + hospital dim ==="
python scripts/v2_to_dolt_csv.py data-v2/*.jsonl
python ci/gen_hospital_csv.py data-v2/*.jsonl
CSV=/tmp/dolt-import/rate_v2.csv
wc -l "$CSV" ci/hospital.csv

echo "=== 3. fresh local dolt repo + import ==="
mkdir -p /tmp/doltrepo && cd /tmp/doltrepo
dolt init
dolt sql < "$REPO/ci/schema.sql"
dolt table import -c hospital "$REPO/ci/hospital.csv"
dolt table import -a --columns "id,ccn,code,code_prefix,code_orig,modifier,ndc,apc,rev_code,internal_code,billing_class,patient_class,payer_orig,plan_orig,payer_category,standard_charge,rate_percent,drug_unit,drug_quantity" rate "$CSV"
dolt add -A
dolt commit -m "shard $SHARD/$TOTAL: $ok hospitals"

echo "=== 4. push branch + PR + merge ==="
dolt remote add origin "doltremoteapi://$DB"
dolt push origin "main:$BRANCH"
python "$REPO/ci/open_pr.py" "$BRANCH" "$SHARD"

echo "=== PIPELINE_DONE: branch=$BRANCH ==="
