# CI deploys the PySpark job: push spark/** to main -> new objects in the code bucket.
# Entry file + the `bronze` package zipped for --py-files.
resource "google_storage_bucket_object" "bronze_ingest" {
  bucket = google_storage_bucket.code.name
  name   = "spark/bronze_ingest.py"
  source = "${path.module}/../spark/bronze_ingest.py"
}

data "archive_file" "bronze" {
  type        = "zip"
  source_dir  = "${path.module}/../spark"
  excludes    = ["bronze_ingest.py", "**/__pycache__/**"]
  output_path = "${path.module}/.build/bronze.zip"
}

resource "google_storage_bucket_object" "bronze_zip" {
  bucket = google_storage_bucket.code.name
  name   = "spark/bronze.zip"
  source = data.archive_file.bronze.output_path
}
