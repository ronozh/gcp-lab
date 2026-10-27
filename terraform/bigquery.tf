# One dataset per medallion layer. A dataset ≈ a schema; bronze also acts as the Iceberg namespace for Spark.
resource "google_bigquery_dataset" "layer" {
  for_each                   = toset(["bronze", "silver", "gold"])
  dataset_id                 = each.key
  location                   = var.region
  delete_contents_on_destroy = true
  depends_on                 = [google_project_service.api]
}

# Connection = a Google-managed SA that BigQuery uses to read/write files in GCS (gold Iceberg).
resource "google_bigquery_connection" "lake" {
  connection_id = "lake"
  location      = var.region
  cloud_resource {}
  depends_on = [google_project_service.api]
}

resource "google_storage_bucket_iam_member" "lake_connection" {
  for_each = toset(["roles/storage.objectUser", "roles/storage.legacyBucketReader"])
  bucket   = google_storage_bucket.lake.name
  role     = each.key
  member   = "serviceAccount:${google_bigquery_connection.lake.cloud_resource[0].service_account_id}"
}

# dbt always creates Iceberg tables `WITH CONNECTION DEFAULT`, so make `lake` the project default
# (a per-region project option). No Terraform resource exists for it, so run the DDL as a BigQuery job.
# Jobs can't be deleted, and job IDs are unique forever: a random suffix lets a rebuild run it again.
locals {
  default_connection_sql = "ALTER PROJECT `${var.project_id}` SET OPTIONS (`region-${var.region}.default_cloud_resource_connection_id` = 'lake')"
}

resource "random_id" "default_connection_job" {
  byte_length = 4
  keepers = {
    sql        = local.default_connection_sql
    connection = google_bigquery_connection.lake.id
  }
}

resource "google_bigquery_job" "default_connection" {
  job_id   = "set-default-connection-${random_id.default_connection_job.hex}"
  location = var.region

  query {
    query              = local.default_connection_sql
    use_legacy_sql     = false
    create_disposition = ""
    write_disposition  = ""
  }

  depends_on = [google_bigquery_connection.lake]
}
