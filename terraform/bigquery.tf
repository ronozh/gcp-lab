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

# dbt always creates Iceberg tables `WITH CONNECTION DEFAULT`, so make `lake` the project default.
# A job runs once; change the job_id to run it again.
resource "google_bigquery_job" "default_connection" {
  job_id   = "set-default-connection-v1"
  location = var.region

  query {
    query              = "ALTER PROJECT `${var.project_id}` SET OPTIONS (`region-${var.region}.default_cloud_resource_connection_id` = 'lake')"
    use_legacy_sql     = false
    create_disposition = ""
    write_disposition  = ""
  }

  depends_on = [google_bigquery_connection.lake]
}
