"""A file lands in GCS → trigger its feed's bronze DAG in Airflow (≈ S3 event → Lambda).

Eventarc calls this function for every new object in the landing bucket:
    ecommerce/<feed>/<yyyy-mm-dd>/<file>.csv  →  POST /api/v2/dags/bronze_<feed>/dagRuns
Other objects (.ctrl, ...) are ignored. No retries: if Airflow is down, the next run of that DAG
loads every file not yet loaded (incremental), so a missed event is caught up then.
"""
import os

import functions_framework
import google.auth.transport.requests
import google.oauth2.id_token
import requests

AIRFLOW_URL = os.environ["AIRFLOW_URL"]


@functions_framework.cloud_event
def on_file_landed(event):
    bucket, name = event.data["bucket"], event.data["name"]
    parts = name.split("/")
    if not name.endswith(".csv") or len(parts) != 4:
        print(f"skip {name}")
        return
    dag_id = f"bronze_{parts[1]}"

    # Cloud Run IAM checks this Google ID token. It goes in X-Serverless-Authorization, which
    # Airflow ignores (Airflow reads Authorization for its own tokens).
    token = google.oauth2.id_token.fetch_id_token(google.auth.transport.requests.Request(), AIRFLOW_URL)
    try:
        r = requests.post(
            f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns",
            headers={"X-Serverless-Authorization": f"Bearer {token}"},
            json={"logical_date": None, "conf": {"file": f"gs://{bucket}/{name}"}},
            timeout=30,
        )
    except requests.RequestException as e:
        print(f"WARNING airflow unreachable, {name} will load on the next {dag_id} run: {e}")
        return
    if r.ok:
        print(f"triggered {dag_id} run {r.json()['dag_run_id']} for {name}")
    else:
        print(f"WARNING {dag_id} not triggered for {name}: HTTP {r.status_code} {r.text[:200]}")
