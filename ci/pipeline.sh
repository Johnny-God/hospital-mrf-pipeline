#!/usr/bin/env bash
# One runner = a claim loop against the shared queue (ci/queue.json in git).
# Preflight builds the queue from ground truth; runners claim -> scrape ->
# bank per-hospital (push to this run's branch) -> mark done after verified push.
# Dead runners' claims expire (45 min) and get re-claimed by survivors.
# One PR+merge at the end of the run; orphan branches swept by preflight next run.
set -euo pipefail

DB="johnnygod/hospital-prices"
REPO="$(pwd)"
RUN_ID="$(date +%Y%m%d-%H%M%S)"
BRANCH="ci/queue-${RUN_ID}"

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

RUNNER="runner-${RUN_ID}-$RANDOM"
BANKED=0

echo "=== 2. claim -> scrape -> bank loop ==="
while :; do
  cd "$REPO"
  CLAIMED=$(python ci/claim.py claim "$RUNNER" || true)
  if [ -z "$CLAIMED" ] || [ "$CLAIMED" = "QUEUE_EMPTY" ]; then
    echo "queue empty — runner exiting (banked $BANKED)"
    break
  fi
  echo "claimed: $CLAIMED"

  cd /tmp/doltwork
  for CCN in $CLAIMED; do
    OUT="$REPO/data-v2/$CCN.jsonl"
    rm -f "$OUT"
    (cd "$REPO" && python scripts/scrape_one.py "$CCN") || { echo "scrape failed: $CCN"; continue; }
    [ -s "$OUT" ] || { echo "empty output: $CCN"; continue; }

    echo "--- bank $CCN ---"
    rm -f /tmp/dolt-import/rate_v2.csv
    python "$REPO/scripts/v2_to_dolt_csv.py" "$OUT"
    (cd "$REPO" && python ci/gen_hospital_csv.py "$OUT")
    dolt table import -a --columns "ccn,hospital_name,state,file_url,transparency_page" hospital "$REPO/ci/hospital.csv" || true
    dolt table import -a --columns "id,ccn,code,code_prefix,code_orig,modifier,ndc,apc,rev_code,internal_code,billing_class,patient_class,payer_orig,plan_orig,payer_category,standard_charge,rate_percent,drug_unit,drug_quantity" rate /tmp/dolt-import/rate_v2.csv
    dolt add -A
    dolt commit -m "shard hospital $CCN"
    dolt push origin "ciwork:$BRANCH"
    BANKED=$((BANKED + 1))
    rm -f "$OUT"
  done

  # push verified (all pushes exited 0) -> mark done in the queue
  cd "$REPO"
  # shellcheck disable=SC2086
  python ci/claim.py done "$RUNNER" $CLAIMED
done

echo "=== 3. PR + merge ($BANKED hospitals on branch) ==="
if [ "$BANKED" -gt 0 ]; then
  python "$REPO/ci/open_pr.py" "$BRANCH" "queue"
else
  echo "nothing banked; no PR"
fi
echo "=== PIPELINE_DONE: runner=$RUNNER banked=$BANKED ==="
