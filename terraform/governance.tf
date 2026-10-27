# Fine-grained access on gold (≈ Lake Formation LF-tags + data filters).
#
#   taxonomy "pii" ── policy tag "high"   → customer_email
#                  └─ policy tag "medium" → customer_first_name, customer_last_name
#
# Who sees what on a tagged column:
#   fine-grained reader (lab-admin)                    → the value
#   anyone else (analysts, even the project owner)     → "Access Denied" when selecting the column
# Masking (hash/NULL instead of denied) needs data policies, which need a GCP Organization:
# not available on a personal (gmail) account.
# dbt attaches the tags to columns (gold.yml) and creates the row access policies (post-hooks).

resource "google_data_catalog_taxonomy" "pii" {
  display_name           = "pii"
  region                 = var.region
  activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]
  depends_on             = [google_project_service.api]
}

resource "google_data_catalog_policy_tag" "pii" {
  for_each     = toset(["high", "medium"])
  taxonomy     = google_data_catalog_taxonomy.pii.id
  display_name = each.key
}

# Test identities. You impersonate them to see what each one gets.
locals {
  test_identities = ["lab-admin", "lab-analyst", "lab-analyst-nsw"]
}

resource "google_service_account" "test" {
  for_each     = toset(local.test_identities)
  account_id   = each.key
  display_name = "Governance test identity: ${each.key}"
}

# Read gold only (no silver, no buckets: governance applies inside BigQuery only) + run queries.
resource "google_bigquery_dataset_iam_member" "test_gold_viewer" {
  for_each   = google_service_account.test
  dataset_id = google_bigquery_dataset.layer["gold"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${each.value.email}"
}

resource "google_project_iam_member" "test_job_user" {
  for_each = google_service_account.test
  project  = var.project_id
  role     = "roles/bigquery.jobUser"
  member   = "serviceAccount:${each.value.email}"
}

# You may act as each test identity (bq/gcloud --impersonate...).
resource "google_service_account_iam_member" "owner_impersonates_test" {
  for_each           = google_service_account.test
  service_account_id = each.value.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "user:${var.owner_email}"
}

# lab-admin: clear values on both tags.
resource "google_data_catalog_policy_tag_iam_member" "admin_fine_grained" {
  for_each   = google_data_catalog_policy_tag.pii
  policy_tag = each.value.name
  role       = "roles/datacatalog.categoryFineGrainedReader"
  member     = "serviceAccount:${google_service_account.test["lab-admin"].email}"
}
