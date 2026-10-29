# csv-to-parquet: the stand-in for ANZ's legacy pipeline. raw *.csv -> processed *.parquet.
data "aws_ssm_parameter" "pandas_layer" {
  name = "/aws/service/aws-sdk-pandas/${var.pandas_layer_version}/py3.12/arm64/layer-arn"
}

data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/csv_to_parquet"
  output_path = "${path.module}/.build/csv_to_parquet.zip"
  excludes    = ["__pycache__"]
}

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "csv-to-parquet"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

data "aws_iam_policy_document" "lambda" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.this["raw"].arn}/*"]
  }
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.this["processed"].arn}/*"]
  }
  statement {
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.this["raw-to-lambda"].arn]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lambda.arn}:*"]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "csv-to-parquet"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/csv-to-parquet"
  retention_in_days = 14
}

resource "aws_lambda_function" "csv_to_parquet" {
  function_name    = "csv-to-parquet"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  architectures    = ["arm64"]
  handler          = "main.handler"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  layers           = [data.aws_ssm_parameter.pandas_layer.value]
  memory_size      = 512
  timeout          = 60
  environment {
    variables = {
      OUTPUT_BUCKET = aws_s3_bucket.this["processed"].id
      LEGACY_QUIRK  = tostring(var.legacy_quirk)
    }
  }
  depends_on = [aws_cloudwatch_log_group.lambda, aws_iam_role_policy.lambda]
}

resource "aws_lambda_event_source_mapping" "raw" {
  event_source_arn        = aws_sqs_queue.this["raw-to-lambda"].arn
  function_name           = aws_lambda_function.csv_to_parquet.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
}
