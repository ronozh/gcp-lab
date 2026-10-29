output "raw_bucket" {
  value = aws_s3_bucket.this["raw"].id
}

output "processed_bucket" {
  value = aws_s3_bucket.this["processed"].id
}

output "queues" {
  description = "Queue ARNs, by name. Storage Transfer Service reads raw-to-gcp and processed-to-gcp (phase 2)."
  value       = { for k, q in aws_sqs_queue.this : k => q.arn }
}
