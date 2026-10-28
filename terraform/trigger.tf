# Event-driven ingestion (≈ S3 event notification → Lambda):
#
#   object created in <project>-landing ─Eventarc─▶ function on-file-landed ─REST─▶ Airflow DAG bronze_<feed>
#
# Eventarc delivers GCS events through Pub/Sub (hidden from you), so the GCS service agent must publish.
# A Cloud Run function is a Cloud Run service built from source by Cloud Build.

# Who the function runs as, and who Eventarc calls it as.
resource "google_service_account" "trigger" {
  account_id   = "file-trigger"
  display_name = "on-file-landed function + its Eventarc trigger"
}

resource "google_project_iam_member" "trigger_event_receiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.trigger.email}"
}

# May call Airflow (Cloud Run IAM).
resource "google_cloud_run_v2_service_iam_member" "trigger_calls_airflow" {
  name     = google_cloud_run_v2_service.airflow.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.trigger.email}"
}

# Eventarc may call the function (its Cloud Run service has the same name).
resource "google_cloud_run_v2_service_iam_member" "trigger_calls_function" {
  name     = google_cloudfunctions2_function.on_file_landed.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.trigger.email}"
}

data "google_storage_project_service_account" "gcs" {}

# Eventarc's own Google-managed identity validates the bucket when the trigger is created.
# Its role is normally granted when the API is enabled, but not reliably on first use: grant it explicitly.
resource "google_project_iam_member" "eventarc_agent" {
  project    = var.project_id
  role       = "roles/eventarc.serviceAgent"
  member     = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-eventarc.iam.gserviceaccount.com"
  depends_on = [google_project_service.api]
}

resource "google_project_iam_member" "gcs_publishes_events" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_storage_project_service_account.gcs.email_address}"
}

# Cloud Build builds the function's container as this identity (new projects give the default one no roles).
resource "google_service_account" "builder" {
  account_id   = "function-builder"
  display_name = "Cloud Build for Cloud Run functions"
}

resource "google_project_iam_member" "builder" {
  for_each = toset(["roles/logging.logWriter", "roles/artifactregistry.writer", "roles/storage.objectViewer"])
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.builder.email}"
}

# Source: functions/on_file_landed/ zipped; a new zip name on each change triggers a rebuild.
data "archive_file" "on_file_landed" {
  type        = "zip"
  source_dir  = "${path.module}/../functions/on_file_landed"
  excludes    = ["**/__pycache__/**"]
  output_path = "${path.module}/.build/on_file_landed.zip"
}

resource "google_storage_bucket_object" "on_file_landed" {
  bucket = google_storage_bucket.code.name
  name   = "functions/on_file_landed-${data.archive_file.on_file_landed.output_md5}.zip"
  source = data.archive_file.on_file_landed.output_path
}

resource "google_cloudfunctions2_function" "on_file_landed" {
  name     = "on-file-landed"
  location = var.region

  build_config {
    runtime         = "python312"
    entry_point     = "on_file_landed"
    service_account = google_service_account.builder.id
    source {
      storage_source {
        bucket = google_storage_bucket.code.name
        object = google_storage_bucket_object.on_file_landed.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.trigger.email
    available_memory      = "256M"
    max_instance_count    = 3
    environment_variables = { AIRFLOW_URL = google_cloud_run_v2_service.airflow.uri }
  }

  event_trigger {
    trigger_region        = var.region # same region as the bucket
    event_type            = "google.cloud.storage.object.v1.finalized"
    retry_policy          = "RETRY_POLICY_DO_NOT_RETRY" # a missed file loads on the next run
    service_account_email = google_service_account.trigger.email
    event_filters {
      attribute = "bucket"
      value     = google_storage_bucket.landing.name
    }
  }

  depends_on = [
    google_project_service.api,
    google_project_iam_member.builder,
    google_project_iam_member.gcs_publishes_events,
    google_project_iam_member.trigger_event_receiver,
    google_project_iam_member.eventarc_agent,
  ]
}
