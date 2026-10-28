# Every GCP service is off in a new project; enable what the lab uses.
# Bootstrap enabled the ones Terraform itself needs (iam, storage, ...).
locals {
  apis = [
    "dataproc.googleapis.com",           # Serverless Spark
    "bigquery.googleapis.com",           # warehouse + the Iceberg catalog Spark uses (BigQuery metastore)
    "bigqueryconnection.googleapis.com", # connection BigQuery uses to write Iceberg files to GCS
    "biglake.googleapis.com",            # BigLake (managed Iceberg tables)
    "datacatalog.googleapis.com",        # policy tags (phase 4)
    "bigquerydatapolicy.googleapis.com", # data masking (phase 4)
    "run.googleapis.com",                # Cloud Run: Airflow + the function (phase 6)
    "sqladmin.googleapis.com",           # Cloud SQL: Airflow metadata DB
    "secretmanager.googleapis.com",      # the DB connection URL
    "cloudfunctions.googleapis.com",     # Cloud Run functions
    "cloudbuild.googleapis.com",         # builds the function's container
    "artifactregistry.googleapis.com",   # stores that container
    "eventarc.googleapis.com",           # GCS event → function
    "pubsub.googleapis.com",             # Eventarc's transport for GCS events
  ]
}

resource "google_project_service" "api" {
  for_each           = toset(local.apis)
  service            = each.key
  disable_on_destroy = false
}
