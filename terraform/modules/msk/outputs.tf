###############################################################################
# modules/msk/outputs.tf
###############################################################################

output "bootstrap_brokers_tls" {
  description = <<-EOT
    Comma-separated list of TLS broker endpoints (port 9094).
    Use this in Flink and FASC coordinator configuration for production traffic.
  EOT
  value     = aws_msk_cluster.this.bootstrap_brokers_tls
  sensitive = false
}

output "bootstrap_brokers" {
  description = <<-EOT
    Comma-separated list of plaintext broker endpoints (port 9092).
    Use only for internal tooling / debugging within the VPC.
  EOT
  value     = aws_msk_cluster.this.bootstrap_brokers
  sensitive = false
}

output "bootstrap_brokers_sasl_iam" {
  description = "Broker endpoints for IAM SASL authentication (port 9098)."
  value       = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
  sensitive   = false
}

output "zookeeper_connect_string" {
  description = <<-EOT
    Zookeeper connection string.  Required by some Kafka admin tools; not
    needed by Flink or the FASC coordinator under Kafka 3.x KRaft mode.
  EOT
  value     = aws_msk_cluster.this.zookeeper_connect_string
  sensitive = false
}

output "cluster_arn" {
  description = "ARN of the MSK cluster. Used in IAM policy resources."
  value       = aws_msk_cluster.this.arn
}

output "cluster_name" {
  description = "Name of the MSK cluster."
  value       = aws_msk_cluster.this.cluster_name
}

output "msk_security_group_id" {
  description = "ID of the security group attached to MSK brokers."
  value       = aws_security_group.msk.id
}
