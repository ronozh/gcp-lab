#!/usr/bin/env bash
# Airflow on Cloud Run: start, stop, open the UI, trigger a DAG.
# Usage: scripts/airflow.sh up | down | status | ui | run <feed> [incremental|full_refresh]
#   up/down   start/stop the metadata DB (Cloud SQL) and the container (Cloud Run instances 1/0)
#   ui        http://localhost:8080 via an authenticated local proxy (Ctrl-C to stop)
#   run       trigger bronze_<feed> through the REST API, as the event function does
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
source "$root/bootstrap/.env"
export CLOUDSDK_CORE_PROJECT="$PROJECT_ID"
where=(--region="$REGION" --quiet)

url() { gcloud run services describe airflow "${where[@]}" --format='value(status.url)'; }
call() { # METHOD PATH [JSON]: the Google token goes in X-Serverless-Authorization (Cloud Run IAM)
  curl -sS -X "$1" "$(url)$2" -H "X-Serverless-Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H 'content-type: application/json' ${3:+-d "$3"}
}

case ${1:-} in
  up)
    gcloud sql instances patch airflow --activation-policy=ALWAYS --quiet
    gcloud run services update airflow --scaling=1 "${where[@]}"
    echo "waiting for Airflow to be healthy (~1-2 min)..."
    until call GET /api/v2/monitor/health 2>/dev/null | grep -q '"scheduler":{"status":"healthy"'; do sleep 10; done
    echo "up. UI: scripts/airflow.sh ui"
    ;;
  down)
    gcloud run services update airflow --scaling=0 "${where[@]}"
    gcloud sql instances patch airflow --activation-policy=NEVER --quiet
    echo "down. Files that land meanwhile load on the next run of their DAG."
    ;;
  status)
    echo "cloud sql: $(gcloud sql instances describe airflow --format='value(state,settings.activationPolicy)')"
    echo "cloud run instances: $(gcloud run services describe airflow "${where[@]}" --format='value(scaling.manualInstanceCount)')"
    ;;
  ui)
    echo "Airflow UI: http://localhost:8080"
    exec gcloud run services proxy airflow "${where[@]}" --port=8080
    ;;
  run)
    feed=${2:?usage: airflow.sh run <feed> [incremental|full_refresh]}; mode=${3:-incremental}
    call POST "/api/v2/dags/bronze_${feed}/dagRuns" "{\"logical_date\": null, \"conf\": {\"mode\": \"$mode\"}}" |
      jq '{dag_id, dag_run_id, state, conf}'
    ;;
  *) sed -n '3,7p' "$0"; exit 2 ;;
esac
