###############################################################################
# modules/dynamodb/variables.tf
###############################################################################

variable "table_name_prefix" {
  description = <<-EOT
    Prefix applied to all three DynamoDB table names.
    Example: "fasc-prod" produces tables "fasc-prod-leader",
    "fasc-prod-coordinator-lock", and "fasc-prod-savepoint-metadata".
  EOT
  type    = string
  default = "fasc"
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
