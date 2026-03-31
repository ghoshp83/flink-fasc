###############################################################################
# modules/dynamodb/outputs.tf
###############################################################################

output "fasc_leader_table_name" {
  description = "Name of the FASC leader-election DynamoDB table."
  value       = aws_dynamodb_table.fasc_leader.name
}

output "fasc_leader_table_arn" {
  description = "ARN of the FASC leader-election DynamoDB table. Used in IAM policies."
  value       = aws_dynamodb_table.fasc_leader.arn
}

output "coordinator_lock_table_name" {
  description = "Name of the FASC coordinator distributed-lock DynamoDB table."
  value       = aws_dynamodb_table.fasc_coordinator_lock.name
}

output "coordinator_lock_table_arn" {
  description = "ARN of the coordinator distributed-lock table. Used in IAM policies."
  value       = aws_dynamodb_table.fasc_coordinator_lock.arn
}

output "savepoint_metadata_table_name" {
  description = "Name of the FASC savepoint-metadata DynamoDB table."
  value       = aws_dynamodb_table.fasc_savepoint_metadata.name
}

output "savepoint_metadata_table_arn" {
  description = "ARN of the savepoint-metadata table. Used in IAM policies."
  value       = aws_dynamodb_table.fasc_savepoint_metadata.arn
}
