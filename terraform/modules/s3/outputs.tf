###############################################################################
# modules/s3/outputs.tf
###############################################################################

output "bucket_name" {
  description = "Name of the S3 bucket for Flink checkpoints and FASC savepoints."
  value       = aws_s3_bucket.this.bucket
}

output "bucket_arn" {
  description = "ARN of the S3 bucket. Used in IAM policy resources."
  value       = aws_s3_bucket.this.arn
}

output "bucket_domain_name" {
  description = "Bucket domain name (for use in Flink configuration as checkpoint path prefix)."
  value       = aws_s3_bucket.this.bucket_domain_name
}

output "bucket_regional_domain_name" {
  description = "Region-specific domain name — avoids cross-region redirect latency."
  value       = aws_s3_bucket.this.bucket_regional_domain_name
}
