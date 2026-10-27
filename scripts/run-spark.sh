#!/usr/bin/env bash
# Submit a PySpark job to Serverless Spark and wait for it. Used by run-bronze.sh and run-gold.sh.
# Usage: scripts/run-spark.sh <entry.py> [job args...]      (--batch-id is added automatically)
#   LOCAL=1 ...   run your local code (dev loop, no push needed); default: the code CI deployed
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
source "$root/bootstrap/.env"
entry=${1:?usage: run-spark.sh <entry.py> [args...]}; shift

if [ "${LOCAL:-}" = 1 ]; then
  tmp=$(mktemp -d)
  (cd "$root/spark" && zip -qr "$tmp/lib.zip" bronze declarative -x '*__pycache__*')
  job="$root/spark/$entry"; py_files="$tmp/lib.zip"                   # gcloud uploads them to --deps-bucket
else
  job="gs://${PROJECT_ID}-code/spark/$entry"; py_files="gs://${PROJECT_ID}-code/spark/lib.zip"   # deployed by CI
fi
name=${entry%.py}; batch="${name//_/-}-${BATCH_SUFFIX:+$BATCH_SUFFIX-}$(date +%Y%m%d-%H%M%S)"   # batch ids: [a-z0-9-]

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
  -- "$@" --batch-id="$batch"
