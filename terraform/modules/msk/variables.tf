###############################################################################
# modules/msk/variables.tf
###############################################################################

variable "cluster_name" {
  description = "Name of the MSK cluster. Also used for child resource names."
  type        = string
}

variable "vpc_id" {
  description = "ID of the VPC in which MSK brokers will be placed."
  type        = string
}

variable "private_subnet_ids" {
  description = <<-EOT
    List of exactly 3 private subnet IDs, one per AZ (eu-west-2a, 2b, 2c).
    MSK requires one subnet per broker node; mismatched counts cause an error.
  EOT
  type        = list(string)

  validation {
    condition     = length(var.private_subnet_ids) == 3
    error_message = "MSK requires exactly 3 subnets — one per AZ in eu-west-2."
  }
}

variable "eks_node_security_group_id" {
  description = <<-EOT
    Security group ID attached to EKS managed nodes.  The MSK security group
    allows inbound Kafka traffic only from this source SG.
  EOT
  type        = string
}

variable "kms_key_arn" {
  description = "ARN of the KMS key used to encrypt MSK broker EBS storage at rest."
  type        = string
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
