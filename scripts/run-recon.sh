#!/usr/bin/env bash
# Recon (Serverless Spark): one feed, one business date, AWS output vs GCP bronze -> BigQuery recon.<feed>.
# Usage: scripts/run-recon.sh <feed> <yyyy-mm-dd> [keys]      (keys: default <feed>_id; SOURCE=ecommerce by default)
#   LOCAL=1 scripts/run-recon.sh <feed> <date>   # run your local code (dev loop, no push needed)
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
source "$root/bootstrap/.env"
usage() { echo "usage: run-recon.sh <feed> <yyyy-mm-dd> [keys]" >&2; exit 2; }
source=${SOURCE:-ecommerce}; feed=${1:-}; day=${2:-}; keys=${3:-${feed}_id}
[ -n "$feed" ] && [ -f "$root/spark/bronze/$source/$feed/config.py" ] || usage
[[ "$day" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || usage

BATCH_SUFFIX=${feed//_/-} exec "$root/scripts/run-spark.sh" recon_transform.py \
  --source="$source" --feed="$feed" --keys="$keys" --date="$day" \
  --processed="gs://${PROJECT_ID}-processed" --temp-bucket="${PROJECT_ID}-code"
