# CI deploys the PySpark code: push spark/** to main -> new objects in the code bucket.
# Entry files + one zip of the packages (bronze/, gold/) for --py-files.
resource "google_storage_bucket_object" "job" {
  for_each = toset(["bronze_transform.py", "gold_transform.py"])
  bucket   = google_storage_bucket.code.name
  name     = "spark/${each.key}"
  source   = "${path.module}/../spark/${each.key}"
}

data "archive_file" "lib" {
  type        = "zip"
  source_dir  = "${path.module}/../spark"
  excludes    = ["bronze_transform.py", "gold_transform.py", "tests/**", "**/__pycache__/**", ".pytest_cache/**"]
  output_path = "${path.module}/.build/lib.zip"
}

resource "google_storage_bucket_object" "lib" {
  bucket = google_storage_bucket.code.name
  name   = "spark/lib.zip"
  source = data.archive_file.lib.output_path
}
