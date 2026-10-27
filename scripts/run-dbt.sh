#!/usr/bin/env bash
# Run dbt locally with the project's config. Needs: gcloud auth application-default login
# Usage: scripts/run-dbt.sh build | run | test | debug | docs generate ...   (any dbt args)
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
set -a; source "$root/bootstrap/.env"; set +a   # PROJECT_ID, REGION -> env_var() in profiles/catalogs
cd "$root/dbt"
[ -d .venv ] || uv venv .venv --python 3.13 -q        # needs uv: brew install uv
uv pip install -q -p .venv -r requirements.txt          # no-op unless requirements.txt changed
exec .venv/bin/dbt "$@" --profiles-dir .
