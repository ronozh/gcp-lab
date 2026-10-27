#!/usr/bin/env bash
# Gold features: gold.fct_sales -> declarative rules -> gold.sales_features (Serverless Spark).
# Run after scripts/run-dbt.sh build (it reads fct_sales' exported Iceberg metadata).
# Usage: scripts/run-gold.sh        LOCAL=1 scripts/run-gold.sh   # run your local code
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
source "$root/bootstrap/.env"
exec "$root/scripts/run-spark.sh" gold_features.py --lake="gs://${PROJECT_ID}-lake"
