# raw:       S3 (*.csv) ─▶ SNS raw-events ─┬─▶ SQS raw-to-gcp     (Storage Transfer Service, phase 2)
#                                          └─▶ SQS raw-to-lambda  ─▶ Lambda csv-to-parquet
# processed: S3 (*.parquet) ─▶ SQS processed-to-gcp               (Storage Transfer Service, phase 2)
# Two consumers need a fan-out (SNS): S3 allows one target per event + prefix, and one SQS
# message goes to one consumer only. Raw delivery: each queue gets the S3 event JSON as is.

resource "aws_sns_topic" "raw" {
  name = "raw-events" # unencrypted: S3 can't publish to a topic using the AWS-managed SNS key; holds no data
}

data "aws_iam_policy_document" "raw_topic" {
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.raw.arn]
    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.this["raw"].arn]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_sns_topic_policy" "raw" {
  arn    = aws_sns_topic.raw.arn
  policy = data.aws_iam_policy_document.raw_topic.json
}

resource "aws_s3_bucket_notification" "raw" {
  bucket = aws_s3_bucket.this["raw"].id
  topic {
    topic_arn     = aws_sns_topic.raw.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "ecommerce/"
    filter_suffix = ".csv"
  }
  depends_on = [aws_sns_topic_policy.raw]
}

# One queue per consumer. Messages a consumer fails 3 times go to its dead-letter queue.
locals {
  queues = {
    raw-to-gcp       = { source = aws_sns_topic.raw.arn, service = "sns.amazonaws.com", visibility = 30 }
    raw-to-lambda    = { source = aws_sns_topic.raw.arn, service = "sns.amazonaws.com", visibility = 360 } # >= 6x Lambda timeout
    processed-to-gcp = { source = aws_s3_bucket.this["processed"].arn, service = "s3.amazonaws.com", visibility = 30 }
  }
}

resource "aws_sqs_queue" "dlq" {
  for_each                  = local.queues
  name                      = "${each.key}-dlq"
  message_retention_seconds = 1209600 # 14 days, the maximum
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "this" {
  for_each                   = local.queues
  name                       = each.key
  visibility_timeout_seconds = each.value.visibility
  sqs_managed_sse_enabled    = true
  redrive_policy             = jsonencode({ deadLetterTargetArn = aws_sqs_queue.dlq[each.key].arn, maxReceiveCount = 3 })
}

data "aws_iam_policy_document" "queue" {
  for_each = local.queues
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.this[each.key].arn]
    principals {
      type        = "Service"
      identifiers = [each.value.service]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [each.value.source]
    }
  }
}

resource "aws_sqs_queue_policy" "this" {
  for_each  = local.queues
  queue_url = aws_sqs_queue.this[each.key].id
  policy    = data.aws_iam_policy_document.queue[each.key].json
}

resource "aws_sns_topic_subscription" "raw" {
  for_each             = toset(["raw-to-gcp", "raw-to-lambda"])
  topic_arn            = aws_sns_topic.raw.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.this[each.key].arn
  raw_message_delivery = true
  depends_on           = [aws_sqs_queue_policy.this]
}

resource "aws_s3_bucket_notification" "processed" {
  bucket = aws_s3_bucket.this["processed"].id
  queue {
    queue_arn     = aws_sqs_queue.this["processed-to-gcp"].arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "ecommerce/"
    filter_suffix = ".parquet"
  }
  depends_on = [aws_sqs_queue_policy.this]
}
