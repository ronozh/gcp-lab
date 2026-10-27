# CI deploys the PySpark job: push spark/** to main -> new object in the code bucket.
resource "google_storage_bucket_object" "bronze_ingest" {
  bucket = google_storage_bucket.code.name
  name   = "spark/bronze_ingest.py"
  source = "${path.module}/../spark/bronze_ingest.py"
}
