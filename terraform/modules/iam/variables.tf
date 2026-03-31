variable "environment" {
  description = "Deployment environment (e.g. production, staging)"
  type        = string
}

variable "cluster_oidc_issuer_url" {
  description = "OIDC issuer URL from the EKS cluster (used to build IRSA trust policies)"
  type        = string
}

variable "fasc_leader_table_arn" {
  description = "ARN of the fasc-leader DynamoDB table"
  type        = string
}

variable "coordinator_lock_table_arn" {
  description = "ARN of the fasc-coordinator-lock DynamoDB table"
  type        = string
}

variable "savepoint_metadata_table_arn" {
  description = "ARN of the fasc-savepoint-metadata DynamoDB table"
  type        = string
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 bucket used for Flink checkpoints and FASC savepoints"
  type        = string
}

variable "msk_cluster_arn" {
  description = "ARN of the MSK cluster"
  type        = string
}

variable "common_tags" {
  description = "Tags applied to all IAM resources"
  type        = map(string)
  default     = {}
}
