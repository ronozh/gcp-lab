variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "australia-southeast1"
}

variable "owner_email" {
  description = "Human owner; granted access to impersonate test identities (phase 4)."
  type        = string
}
