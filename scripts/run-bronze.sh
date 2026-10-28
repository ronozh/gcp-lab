#!/usr/bin/env bash
# Bronze: one feed's new landing files -> bronze.<feed> (Serverless Spark).
# Usage: scripts/run-bronze.sh <feed> [incremental|full_refresh]      (SOURCE=ecommerce by default)
#   LOCAL=1 scripts/run-bronze.sh <feed> ...   # run your local code (dev loop, no push needed)
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
source "$root/bootstrap/.env"
usage() { echo "usage: run-bronze.sh <feed> [incremental|full_refresh]" >&2; exit 2; }
source=${SOURCE:-ecommerce}; feed=${1:-}; mode=${2:-incremental}
# Check here: a typo inside the batch costs ~3 min and the only quota slot.
[ -n "$feed" ] && [ -f "$root/spark/bronze/$source/$feed/config.py" ] || usage
case $mode in incremental|full_refresh) ;; *) usage ;; esac

BATCH_SUFFIX=${feed//_/-} exec "$root/scripts/run-spark.sh" bronze_transform.py \
  --source="$source" --feed="$feed" --mode="$mode" --landing="gs://${PROJECT_ID}-landing"
