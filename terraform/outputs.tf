output "landing_bucket" {
  value = google_storage_bucket.landing.name
}

output "lake_bucket" {
  value = google_storage_bucket.lake.name
}

output "code_bucket" {
  value = google_storage_bucket.code.name
}

output "spark_sa" {
  value = google_service_account.spark.email
}

output "airflow_bucket" {
  value = google_storage_bucket.airflow.name
}
