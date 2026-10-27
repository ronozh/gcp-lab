# Every GCP service is off in a new project; enable what the lab uses.
# Bootstrap enabled the ones Terraform itself needs (iam, storage, ...).
locals {
  apis = [
    "compute.googleapis.com",            # default network used by Serverless Spark
    "dataproc.googleapis.com",           # Serverless Spark
    "bigquery.googleapis.com",           # warehouse + Iceberg catalog
    "bigqueryconnection.googleapis.com", # connection BigQuery uses to write Iceberg files to GCS
    "biglake.googleapis.com",            # BigLake metastore
    "datacatalog.googleapis.com",        # policy tags (phase 4)
    "bigquerydatapolicy.googleapis.com", # data masking (phase 4)
  ]
}

resource "google_project_service" "api" {
  for_each           = toset(local.apis)
  service            = each.key
  disable_on_destroy = false
}
