#!/usr/bin/env bash
# Upload sample files for a date range to the landing bucket:
#   gs://<project-id>-landing/ecommerce/<feed>/<yyyy-mm-dd>/<file>.csv + .ctrl
# Upload .ctrl last (after its .csv).
# Usage: ingest/upload.sh <from yyyy-mm-dd> [to yyyy-mm-dd]
set -euo pipefail
from=$1; to=${2:-$1}
root=$(cd "$(dirname "$0")/.." && pwd)
source "$root/bootstrap/.env"
src="$root/data/sample/ecommerce"
bucket="gs://${PROJECT_ID}-landing"

d=$from
while [[ ! "$d" > "$to" ]]; do
  for feed in transaction customer product campaign; do
    dir="$src/$feed/$d"
    [ -d "$dir" ] || { echo "skip $feed/$d (no sample data)"; continue; }
    gcloud storage cp "$dir"/*.csv "$bucket/ecommerce/$feed/$d/" --quiet
    gcloud storage cp "$dir"/*.ctrl "$bucket/ecommerce/$feed/$d/" --quiet
    echo "uploaded $feed/$d"
  done
  d=$(date -j -v+1d -f %Y-%m-%d "$d" +%Y-%m-%d 2>/dev/null || date -d "$d + 1 day" +%Y-%m-%d)
done
