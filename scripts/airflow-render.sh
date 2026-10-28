#!/usr/bin/env bash
# Render airflow/dags/*.yml -> airflow/build/dags/*.py and import-check them,
# inside the same Airflow image the server runs (no local Airflow install). Used by CI and locally.
# Usage: scripts/airflow-render.sh
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
image=$(sed -n 's/^ *airflow_image *= *"\([^"]*\)".*/\1/p' "$root/terraform/airflow.tf")
docker run --rm --user "$(id -u):0" -v "$root/airflow:/work/airflow" -w /work \
  -e GOOGLE_CLOUD_PROJECT=check -e GOOGLE_CLOUD_REGION=check \
  --entrypoint python "$image" airflow/render.py --check
