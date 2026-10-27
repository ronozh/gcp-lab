#!/usr/bin/env bash
# Submit the bronze ingest job to Serverless Spark and wait for it.
# Usage: scripts/run-bronze.sh [--reload '<regex>']
#   JOB=spark/bronze_ingest.py scripts/run-bronze.sh   # run your local copy (dev loop, no push needed)
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
source "$root/bootstrap/.env"

job=${JOB:-gs://${PROJECT_ID}-code/spark/bronze_ingest.py}   # default: the copy CI deployed
batch="bronze-$(date +%Y%m%d-%H%M%S)"

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
  --properties="$(IFS=,; echo "${props[*]}")" \
  -- --landing="gs://${PROJECT_ID}-landing" --batch-id="$batch" "$@"
