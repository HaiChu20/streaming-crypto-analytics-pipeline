variable "bucket_name" {
  type        = string
  description = "Globally unique S3 bucket name (TF_VAR_bucket_name)."
}

variable "region" {
  type        = string
  description = "AWS region (TF_VAR_region)."
}

variable "budget_email" {
  type        = string
  description = "Email address for monthly budget notifications."
}

variable "budget_usd" {
  type        = number
  default     = 10
  description = "Monthly AWS cost budget in USD."
}
