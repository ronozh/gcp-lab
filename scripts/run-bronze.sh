#!/usr/bin/env bash
# Submit the bronze ingest job to Serverless Spark and wait for it.
# Usage: scripts/run-bronze.sh <feed> [incremental|full_refresh]
#   LOCAL=1 scripts/run-bronze.sh <feed> ...   # run your local copy (dev loop, no push needed)
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
source "$root/bootstrap/.env"
usage() { echo "usage: run-bronze.sh <feed> [incremental|full_refresh]" >&2; exit 2; }
feed=${1:-}; mode=${2:-incremental}
# Check here: a typo inside the batch costs ~3 min and the only quota slot.
[ -n "$feed" ] && [ -f "$root/spark/bronze/config/$feed.py" ] || usage
case $mode in incremental|full_refresh) ;; *) usage ;; esac

if [ "${LOCAL:-}" = 1 ]; then
  tmp=$(mktemp -d)
  (cd "$root/spark" && zip -qr "$tmp/bronze.zip" bronze -x '*__pycache__*')
  job="$root/spark/bronze_ingest.py"; py_files="$tmp/bronze.zip"      # gcloud uploads them to --deps-bucket
else
  job="gs://${PROJECT_ID}-code/spark/bronze_ingest.py"; py_files="gs://${PROJECT_ID}-code/spark/bronze.zip"   # deployed by CI
fi
batch="bronze-${feed//_/-}-$(date +%Y%m%d-%H%M%S)"   # batch ids: [a-z0-9-]

props=(
  # Smallest footprint: 4-core driver + 2 x 4-core executors = 12 vCPU (the free-trial quota).
  spark.driver.cores=4 spark.executor.cores=4 spark.executor.instances=2
  spark.dynamicAllocation.enabled=false
  spark.dataproc.driver.disk.size=250g spark.dataproc.executor.disk.size=250g
  spark.sql.session.timeZone=UTC
  # Iceberg catalog "lake" = BigQuery metastore: namespace = BigQuery dataset, files under warehouse.
  # Runtime 2.3 already bundles Iceberg + Google's BigQuery metastore catalog: no --jars needed.
  spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.lake.type=bigquery
  spark.sql.catalog.lake.gcp_project=$PROJECT_ID
  spark.sql.catalog.lake.gcp_location=$REGION
  spark.sql.catalog.lake.warehouse=gs://${PROJECT_ID}-lake/iceberg
)

gcloud dataproc batches submit pyspark "$job" \
  --project="$PROJECT_ID" --region="$REGION" --batch="$batch" \
  --version=2.3 \
  --service-account="spark-runner@${PROJECT_ID}.iam.gserviceaccount.com" \
  --subnet=default \
  --deps-bucket="gs://${PROJECT_ID}-code" \
  --py-files="$py_files" \
  --properties="$(IFS=,; echo "${props[*]}")" \
  -- --feed="$feed" --mode="$mode" --landing="gs://${PROJECT_ID}-landing" --batch-id="$batch"
