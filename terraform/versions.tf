terraform {
  required_version = ">= 1.10"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # State in the bootstrap bucket. Bucket comes from backend.hcl (gitignored; CI writes it).
  backend "gcs" {
    prefix = "terraform"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
