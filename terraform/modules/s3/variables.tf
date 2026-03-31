###############################################################################
# modules/s3/variables.tf
###############################################################################

variable "bucket_name" {
  description = <<-EOT
    Globally unique S3 bucket name for Flink checkpoints and FASC savepoints.
    Example: "flink-fasc-savepoints-prod-eu-west-2-123456789012"
  EOT
  type = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9\\-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "Bucket name must be 3–63 lowercase alphanumeric characters or hyphens, and not start/end with a hyphen."
  }
}

variable "kms_key_arn" {
  description = <<-EOT
    ARN of a KMS CMK for S3 server-side encryption (aws:kms).
    Set to an empty string "" to fall back to AES256 (SSE-S3).
  EOT
  type    = string
  default = ""
}

variable "eks_node_role_arns" {
  description = <<-EOT
    List of IAM role ARNs for EKS node instance profiles and IRSA roles
    that are explicitly allowed by the bucket policy.  Include both the
    node instance-profile role and the Flink IRSA role.
  EOT
  type    = list(string)
  default = []
}

variable "environment" {
  description = "Deployment environment (e.g. production, staging, dev)."
  type        = string
}

variable "common_tags" {
  description = "Map of tags applied to every resource created by this module."
  type        = map(string)
  default     = {}
}
