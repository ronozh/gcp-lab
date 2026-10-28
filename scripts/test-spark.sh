#!/usr/bin/env bash
# Run the Spark unit tests locally: local PySpark, tiny in-memory data, no GCP, ~20 s.
# Needs: brew install openjdk@17 uv
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
unset SPARK_HOME                       # use the venv's pyspark, not a Homebrew Spark from your shell profile
export PYTHONPATH="$root/spark"        # import bronze, gold
cd "$root"
exec uv run --quiet --python 3.12 --with 'pyspark==3.5.*' --with pytest -- pytest -q spark/tests "$@"
