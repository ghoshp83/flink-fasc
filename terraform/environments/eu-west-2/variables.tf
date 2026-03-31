variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "production"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-2"
}

variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where all resources will be deployed"
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs (one per AZ: eu-west-2a, eu-west-2b, eu-west-2c)"
  type        = list(string)
  validation {
    condition     = length(var.private_subnet_ids) == 3
    error_message = "Exactly 3 private subnets are required (one per AZ in eu-west-2)."
  }
}

variable "bucket_name" {
  description = "S3 bucket name for Flink checkpoints and FASC savepoints"
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key ARN for S3 and MSK encryption. Leave empty to use AWS-managed keys."
  type        = string
  default     = ""
}

variable "dynamodb_table_name_prefix" {
  description = "Prefix for DynamoDB table names"
  type        = string
  default     = "fasc"
}

variable "common_tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default = {
    Project   = "flink-fasc"
    ManagedBy = "terraform"
  }
}
