###############################################################################
# modules/eks/outputs.tf
###############################################################################

output "cluster_name" {
  description = "Name of the EKS cluster."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "API server endpoint URL (private, within VPC)."
  value       = module.eks.cluster_endpoint
}

output "cluster_oidc_issuer_url" {
  description = <<-EOT
    OIDC issuer URL for the cluster — used by the IAM module to build
    IRSA trust-policy conditions for service-account role assumptions.
  EOT
  value = module.eks.cluster_oidc_issuer_url
}

output "oidc_provider_arn" {
  description = "Full ARN of the IAM OIDC provider created for this cluster."
  value       = module.eks.oidc_provider_arn
}

output "node_security_group_id" {
  description = <<-EOT
    Security group ID attached to all EKS managed nodes.  The MSK module
    uses this to allow inbound Kafka traffic from the cluster nodes.
  EOT
  value = module.eks.node_security_group_id
}

output "cluster_certificate_authority_data" {
  description = "Base64-encoded certificate authority data for the cluster."
  value       = module.eks.cluster_certificate_authority_data
  sensitive   = true
}
