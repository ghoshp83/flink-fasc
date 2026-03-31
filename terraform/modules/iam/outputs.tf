output "flink_app_role_arn" {
  description = "IAM role ARN for Flink app pods (app1 + app2). Use as IRSA annotation on the flink-app ServiceAccount."
  value       = aws_iam_role.flink_app.arn
}

output "flink_app_role_name" {
  description = "IAM role name for Flink app pods"
  value       = aws_iam_role.flink_app.name
}

output "fasc_coordinator_role_arn" {
  description = "IAM role ARN for FASC coordinator pods. Use as IRSA annotation on the fasc-coordinator ServiceAccount."
  value       = aws_iam_role.fasc_coordinator.arn
}

output "fasc_coordinator_role_name" {
  description = "IAM role name for FASC coordinator pods"
  value       = aws_iam_role.fasc_coordinator.name
}
