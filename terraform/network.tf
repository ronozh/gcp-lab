# Private Google Access on the `default` subnet is now set by bootstrap/bootstrap.sh
# (auto-mode subnets can't be deleted, so `terraform destroy` would fail on it).
# This block makes Terraform forget the subnet without touching it. Safe to delete after one apply.
removed {
  from = google_compute_subnetwork.default
  lifecycle {
    destroy = false
  }
}
