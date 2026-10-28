# Airflow 3 on Cloud Run (≈ MWAA, without the always-on bill). One container runs `airflow standalone`:
# api-server (UI + REST), scheduler, dag-processor, triggerer, LocalExecutor.
#
#   Cloud Run service "airflow" ── manual scaling: 1 instance = up, 0 = down (scripts/airflow.sh)
#     ├─ /opt/airflow/gcs   ← bucket <project>-airflow mounted read-only; DAGs in dags/ (deployed by CI)
#     └─ /cloudsql          ← Cloud SQL Postgres "airflow" (metadata DB; stopped when down)
#
# Stateless: on start it migrates the DB and creates the pool, so a recreated stack works at once.
# Access: Cloud Run IAM only (no public invoker). Airflow's own login is off (all users are admin).

locals {
  airflow_image = "docker.io/apache/airflow:3.3.2-python3.12" # scripts/airflow-render.sh checks DAGs with it too
  airflow_start = join(" && ", [
    "airflow db migrate",
    # 1 slot: the free trial's 12 vCPU quota fits one Spark batch.
    "airflow pools set spark 1 'Serverless Spark: one batch at a time'",
    "exec airflow standalone",
  ])
}

# DAG files (CI renders them from airflow/dags/*.yml and syncs them to dags/).
resource "google_storage_bucket" "airflow" {
  name                        = "${var.project_id}-airflow"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
}

# --- Identity: what Airflow runs as ---------------------------------------------------------------
resource "google_service_account" "airflow" {
  account_id   = "airflow"
  display_name = "Airflow on Cloud Run"
}

resource "google_project_iam_member" "airflow" {
  for_each = toset([
    "roles/dataproc.editor", # submit + watch Serverless Spark batches
    "roles/cloudsql.client", # connect to the metadata DB
    "roles/compute.viewer",  # read the vCPU quota before submitting (free-trial quota fits one batch)
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# Submit batches that run as spark-runner (≈ iam:PassRole).
resource "google_service_account_iam_member" "airflow_acts_as_spark" {
  service_account_id = google_service_account.spark.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_storage_bucket_iam_member" "airflow_reads_dags" {
  bucket = google_storage_bucket.airflow.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

# --- Metadata DB: Cloud SQL Postgres (≈ RDS) --------------------------------------------------------
resource "google_sql_database_instance" "airflow" {
  name                = "airflow"
  database_version    = "POSTGRES_17"
  region              = var.region
  deletion_protection = false # recreate freely: losing run history is fine in this lab

  settings {
    edition           = "ENTERPRISE"
    tier              = "db-f1-micro" # smallest (shared core)
    availability_type = "ZONAL"
    disk_type         = "PD_HDD"
    disk_size         = 10
    activation_policy = "ALWAYS" # scripts/airflow.sh down sets NEVER (= stopped)

    # Public IP but no authorized networks: only the Cloud SQL connector (IAM) can reach it.
    ip_configuration {
      ipv4_enabled = true
    }
    backup_configuration {
      enabled = false
    }
  }

  lifecycle {
    ignore_changes = [settings[0].activation_policy] # owned by scripts/airflow.sh up|down
  }
  depends_on = [google_project_service.api]
}

resource "google_sql_database" "airflow" {
  name     = "airflow"
  instance = google_sql_database_instance.airflow.name
}

resource "random_password" "airflow_db" {
  length  = 24
  special = false # goes into a URL
}

resource "google_sql_user" "airflow" {
  name     = "airflow"
  instance = google_sql_database_instance.airflow.name
  password = random_password.airflow_db.result
}

# Connection URL (contains the password) → Secret Manager (≈ Secrets Manager) → env var in the container.
resource "google_secret_manager_secret" "airflow_db_url" {
  secret_id = "airflow-db-url"
  replication {
    auto {}
  }
  depends_on = [google_project_service.api]
}

resource "google_secret_manager_secret_version" "airflow_db_url" {
  secret      = google_secret_manager_secret.airflow_db_url.id
  secret_data = "postgresql+psycopg2://airflow:${random_password.airflow_db.result}@/airflow?host=/cloudsql/${google_sql_database_instance.airflow.connection_name}"
}

resource "google_secret_manager_secret_iam_member" "airflow_db_url" {
  secret_id = google_secret_manager_secret.airflow_db_url.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.airflow.email}"
}

# --- The Airflow container (Cloud Run ≈ Fargate service) -------------------------------------------
resource "google_cloud_run_v2_service" "airflow" {
  name                = "airflow"
  location            = var.region
  deletion_protection = false
  ingress             = "INGRESS_TRAFFIC_ALL" # reachable, but only by identities with run.invoker

  scaling {
    scaling_mode          = "MANUAL"
    manual_instance_count = 1 # scripts/airflow.sh up|down sets 1|0
  }

  template {
    service_account       = google_service_account.airflow.email
    execution_environment = "EXECUTION_ENVIRONMENT_GEN2" # needed for the bucket mount

    containers {
      image = local.airflow_image
      args  = ["bash", "-c", local.airflow_start]

      ports {
        container_port = 8080
      }
      resources {
        limits   = { cpu = "2", memory = "4Gi" }
        cpu_idle = false # always-on CPU: the scheduler works between requests
      }

      env {
        name = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.airflow_db_url.secret_id
            version = "latest"
          }
        }
      }
      dynamic "env" {
        for_each = {
          AIRFLOW__CORE__EXECUTOR                       = "LocalExecutor"
          AIRFLOW__CORE__LOAD_EXAMPLES                  = "False"
          AIRFLOW__CORE__DAGS_FOLDER                    = "/opt/airflow/gcs/dags"
          AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS = "True" # Cloud Run IAM is the gate
          AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL      = "30"   # pick up new DAG files within ~30 s
          AIRFLOW__API__WORKERS                         = "1"
          AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT             = "google-cloud-platform://" # = the service account
          GOOGLE_CLOUD_PROJECT                          = var.project_id             # read by the DAGs
          GOOGLE_CLOUD_REGION                           = var.region
        }
        content {
          name  = env.key
          value = env.value
        }
      }

      volume_mounts {
        name       = "gcs"
        mount_path = "/opt/airflow/gcs"
      }
      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }

      # First start migrates an empty DB: allow up to 5 min.
      startup_probe {
        tcp_socket {
          port = 8080
        }
        period_seconds    = 10
        timeout_seconds   = 5
        failure_threshold = 30
      }
    }

    volumes {
      name = "gcs"
      gcs {
        bucket    = google_storage_bucket.airflow.name
        read_only = true
      }
    }
    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [google_sql_database_instance.airflow.connection_name]
      }
    }
  }

  lifecycle {
    ignore_changes = [scaling[0].manual_instance_count] # owned by scripts/airflow.sh up|down
  }
  depends_on = [
    google_project_service.api,
    google_project_iam_member.airflow,
    google_secret_manager_secret_iam_member.airflow_db_url,
    google_secret_manager_secret_version.airflow_db_url,
    google_storage_bucket_iam_member.airflow_reads_dags,
  ]
}
