#!/usr/bin/env bash
# Low-level helper: submit a PySpark entry file to Serverless Spark and wait. Use run-bronze.sh / run-gold.sh.
# Usage: scripts/run-spark.sh <entry.py> [job args...]      (--batch-id is added automatically)
#   LOCAL=1 ...   run your local code (dev loop, no push needed); default: the code CI deployed
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
source "$root/bootstrap/.env"
entry=${1:-}
if [ ! -f "$root/spark/$entry" ] || [[ "$entry" != *.py ]]; then
  echo "run-spark.sh is the low-level helper: its first argument is a Spark entry file (bronze_ingest.py, gold_features.py)." >&2
  echo "To load a feed use: scripts/run-bronze.sh <feed> [incremental|full_refresh]" >&2
  exit 2
fi
shift

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

# The trial quota (12 vCPU) fits one batch: wait while another batch is active or still holds vCPUs.
busy() {
  local active cpus
  active=$(gcloud dataproc batches list --project="$PROJECT_ID" --region="$REGION" --limit=10 --format=json |
    jq '[.[] | select(.state == "PENDING" or .state == "RUNNING")] | length')
  cpus=$(gcloud compute project-info describe --project="$PROJECT_ID" --format=json |
    jq '.quotas[] | select(.metric == "CPUS_ALL_REGIONS") | .usage | floor')
  [ "$active" -gt 0 ] || [ "$cpus" -gt 0 ]
}
while busy; do echo "waiting: another batch is using the vCPU quota (one batch at a time on the trial)..."; sleep 20; done

gcloud dataproc batches submit pyspark "$job" \
  --project="$PROJECT_ID" --region="$REGION" --batch="$batch" \
  --version=2.3 \
  --service-account="spark-runner@${PROJECT_ID}.iam.gserviceaccount.com" \
  --subnet=default \
  --deps-bucket="gs://${PROJECT_ID}-code" \
  --py-files="$py_files" \
  --properties="$(IFS=,; echo "${props[*]}")" \
  -- "$@" --batch-id="$batch"
