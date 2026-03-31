output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "eks_cluster_endpoint" {
  description = "EKS cluster API server endpoint"
  value       = module.eks.cluster_endpoint
}

output "msk_bootstrap_brokers_tls" {
  description = "MSK TLS bootstrap brokers — use this in Flink and coordinator config"
  value       = module.msk.bootstrap_brokers_tls
  sensitive   = true
}

output "msk_bootstrap_brokers_plaintext" {
  description = "MSK plaintext bootstrap brokers — for dev/internal use only"
  value       = module.msk.bootstrap_brokers
  sensitive   = true
}

output "s3_bucket_name" {
  description = "S3 bucket for Flink checkpoints and FASC savepoints"
  value       = module.s3.bucket_name
}

output "dynamodb_fasc_leader_table" {
  description = "DynamoDB table name for FASC leader election"
  value       = module.dynamodb.fasc_leader_table_name
}

output "dynamodb_coordinator_lock_table" {
  description = "DynamoDB table name for coordinator lock"
  value       = module.dynamodb.coordinator_lock_table_name
}

output "flink_app_role_arn" {
  description = "IAM role ARN for Flink app pods — use as IRSA annotation"
  value       = module.iam.flink_app_role_arn
}

output "fasc_coordinator_role_arn" {
  description = "IAM role ARN for FASC coordinator pods — use as IRSA annotation"
  value       = module.iam.fasc_coordinator_role_arn
}

output "helm_values_snippet" {
  description = "Ready-to-paste Helm values for the FASC coordinator"
  value = <<-EOT
    # Paste this into your Helm values or --set flags:
    serviceAccount:
      annotations:
        eks.amazonaws.com/role-arn: ${module.iam.fasc_coordinator_role_arn}
    env:
      FASC_BOOTSTRAP_SERVERS: "${module.msk.bootstrap_brokers_tls}"
      FASC_SAVEPOINT_S3_BUCKET: "${module.s3.bucket_name}"
      FASC_DYNAMODB_LEADER_TABLE: "${module.dynamodb.fasc_leader_table_name}"
      FASC_DYNAMODB_LOCK_TABLE: "${module.dynamodb.coordinator_lock_table_name}"
      AWS_REGION: "eu-west-2"
  EOT
  sensitive = true
}
