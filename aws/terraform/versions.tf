# The simulated ANZ estate on AWS. Applied locally with SSO (scripts/aws.sh), not by CI:
# in the real migration this side belongs to ANZ, not to the GCP platform pipeline.
terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }

  # Same GCS state bucket as the GCP stack, own prefix. Bucket from ../../terraform/backend.hcl.
  backend "gcs" {
    prefix = "aws"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      project    = "gcp-lab"
      managed_by = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
}
