# Serverless Spark runs on the `default` subnet (it already has the `default-allow-internal` firewall rule).
# Spark workers have no public IP, so they need Private Google Access to reach GCS/BigQuery.
# The subnet came with the project; `import` adopts it into Terraform instead of creating a new one.
import {
  to = google_compute_subnetwork.default
  id = "projects/${var.project_id}/regions/${var.region}/subnetworks/default"
}

resource "google_compute_subnetwork" "default" {
  name                     = "default"
  region                   = var.region
  network                  = "default"
  ip_cidr_range            = "10.152.0.0/20" # auto-mode range for australia-southeast1
  private_ip_google_access = true
}
