# Identity the Spark job runs as (≈ an EMR job execution role).
resource "google_service_account" "spark" {
  account_id   = "spark-runner"
  display_name = "Serverless Spark runtime"
}

# Required by Serverless Spark. Note: it also grants object read/write on every bucket in the project.
resource "google_project_iam_member" "spark_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.spark.email}"
}

# Create and update Iceberg tables (BigQuery metastore catalog): bronze.*, and gold.sales_features.
resource "google_bigquery_dataset_iam_member" "spark_bronze" {
  dataset_id = google_bigquery_dataset.layer["bronze"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.spark.email}"
}

resource "google_bigquery_dataset_iam_member" "spark_gold" {
  dataset_id = google_bigquery_dataset.layer["gold"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.spark.email}"
}
