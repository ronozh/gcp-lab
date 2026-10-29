# The bridge AWS -> GCP: Storage Transfer Service (STS) reads each SQS event and copies the new object.
#   s3://raw-<acct>/ecommerce/...       -> gs://<project>-landing/ecommerce/...    (the GCP pipeline starts there)
#   s3://processed-<acct>/ecommerce/... -> gs://<project>-processed/ecommerce/...  (AWS output, for the recon)
# Keyless both ways: STS's Google service agent assumes an AWS role via web identity (OIDC).
# Here, not in the GCP stack: both halves need each other's ids, and the AWS account id stays out of CI.

provider "google" {
  project = var.project_id
  region  = var.gcp_region
}

# Google's per-project STS identity; subject_id is what AWS trusts.
data "google_storage_transfer_project_service_account" "sts" {}

data "aws_iam_policy_document" "sts_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = ["accounts.google.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "accounts.google.com:sub"
      values   = [data.google_storage_transfer_project_service_account.sts.subject_id]
    }
  }
}

resource "aws_iam_role" "sts" {
  name               = "gcp-storage-transfer"
  description        = "Assumed by Google Storage Transfer Service (gcp-lab): read raw + processed, consume their -to-gcp queues."
  assume_role_policy = data.aws_iam_policy_document.sts_trust.json
}

data "aws_iam_policy_document" "sts" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [for b in aws_s3_bucket.this : b.arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = [for b in aws_s3_bucket.this : "${b.arn}/*"]
  }
  statement {
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.this["raw-to-gcp"].arn, aws_sqs_queue.this["processed-to-gcp"].arn]
  }
}

resource "aws_iam_role_policy" "sts" {
  name   = "gcp-storage-transfer"
  role   = aws_iam_role.sts.id
  policy = data.aws_iam_policy_document.sts.json
}

# STS writes into the GCP buckets (created by the GCP stack).
locals {
  transfers = {
    raw       = { queue = "raw-to-gcp", sink = "${var.project_id}-landing" }
    processed = { queue = "processed-to-gcp", sink = "${var.project_id}-processed" }
  }
  sink_roles = ["roles/storage.objectAdmin", "roles/storage.legacyBucketReader"] # objects + buckets.get
}

resource "google_storage_bucket_iam_member" "sts" {
  for_each = { for p in setproduct(keys(local.transfers), local.sink_roles) : "${p[0]}/${p[1]}" => { bucket = local.transfers[p[0]].sink, role = p[1] } }
  bucket   = each.value.bucket
  role     = each.value.role
  member   = "serviceAccount:${data.google_storage_transfer_project_service_account.sts.email}"
}

resource "google_storage_transfer_job" "this" {
  for_each    = local.transfers
  description = "${each.key}: s3 ${aws_s3_bucket.this[each.key].id} -> gs ${each.value.sink} (event-driven)"

  event_stream {
    name = aws_sqs_queue.this[each.value.queue].arn # no schedule: runs on each event
  }

  transfer_spec {
    aws_s3_data_source {
      bucket_name = aws_s3_bucket.this[each.key].id
      path        = "ecommerce/"
      role_arn    = aws_iam_role.sts.arn
    }
    gcs_data_sink {
      bucket_name = each.value.sink
      path        = "ecommerce/" # same layout as S3
    }
    transfer_options {
      overwrite_when = "DIFFERENT"
    }
  }

  depends_on = [aws_iam_role_policy.sts, google_storage_bucket_iam_member.sts]
}
