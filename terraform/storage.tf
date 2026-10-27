# landing: raw files as delivered (versioned = audit trail of overwrites).
resource "google_storage_bucket" "landing" {
  name                        = "${var.project_id}-landing"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true

  versioning {
    enabled = true
  }
}

# lake: Iceberg data + metadata files (bronze written by Spark, gold by BigQuery).
# No versioning or lock: BigQuery-managed Iceberg tables require that.
resource "google_storage_bucket" "lake" {
  name                        = "${var.project_id}-lake"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
}

# code: PySpark job files (deployed by CI) and Spark staging files (--deps-bucket).
resource "google_storage_bucket" "code" {
  name                        = "${var.project_id}-code"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
}
