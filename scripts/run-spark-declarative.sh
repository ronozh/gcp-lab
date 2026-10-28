#!/usr/bin/env bash
# Declarative business rules (Serverless Spark): gold.fct_sales -> registered rules -> gold.sales_features.
# Run after scripts/run-dbt.sh build (it reads fct_sales' exported Iceberg metadata).
# Usage: scripts/run-spark-declarative.sh        LOCAL=1 scripts/run-spark-declarative.sh   # run your local code
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
source "$root/bootstrap/.env"
exec "$root/scripts/run-spark.sh" gold_transform.py --model=sales_features --lake="gs://${PROJECT_ID}-lake"
