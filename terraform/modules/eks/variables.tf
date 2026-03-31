###############################################################################
# modules/eks/variables.tf
###############################################################################

variable "cluster_name" {
  description = "Name of the EKS cluster. Also used as a prefix for child resources."
  type        = string
}

variable "vpc_id" {
  description = "ID of the VPC in which the EKS cluster will be created."
  type        = string
}

variable "private_subnet_ids" {
  description = <<-EOT
    List of private subnet IDs (one per AZ) used for EKS nodes and the cluster
    control-plane ENIs.  For eu-west-2 this should be three subnets —
    eu-west-2a, eu-west-2b, eu-west-2c.
  EOT
  type        = list(string)

  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "At least two private subnets are required for high availability."
  }
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
