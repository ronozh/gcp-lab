variable "aws_region" {
  type    = string
  default = "ap-southeast-2"
}

variable "pandas_layer_version" {
  description = "AWS SDK for pandas (pandas + pyarrow) Lambda layer, published by AWS. List: aws ssm get-parameters-by-path --path /aws/service/aws-sdk-pandas --recursive"
  type        = string
  default     = "3.9.1"
}

variable "legacy_quirk" {
  description = "Demo: the Lambda rounds budget_aud to the nearest 1000, a logic difference the recon should catch."
  type        = bool
  default     = false
}

variable "project_id" {
  description = "GCP project. scripts/aws.sh reads it from terraform/terraform.tfvars (gitignored)."
  type        = string
}

variable "gcp_region" {
  type    = string
  default = "australia-southeast1"
}
