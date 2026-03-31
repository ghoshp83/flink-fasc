# ── FASC Infrastructure — eu-west-2 (London) ─────────────────────────────────
# Replace all placeholder values with your actual AWS resource IDs.

environment = "production"
aws_region  = "eu-west-2"

# EKS cluster name
cluster_name = "flink-fasc-cluster"

# Your existing VPC — run: aws ec2 describe-vpcs --region eu-west-2
vpc_id = "vpc-xxxxxxxxxxxxxxxxx"

# Three private subnets — one per AZ (eu-west-2a, eu-west-2b, eu-west-2c)
# Run: aws ec2 describe-subnets --filters "Name=vpc-id,Values=vpc-xxx" --region eu-west-2
private_subnet_ids = [
  "subnet-aaaaaaaaaaaaaaaaa",  # eu-west-2a
  "subnet-bbbbbbbbbbbbbbbbb",  # eu-west-2b
  "subnet-ccccccccccccccccc",  # eu-west-2c
]

# S3 bucket for Flink checkpoints + FASC savepoints
# Must be globally unique
bucket_name = "flink-fasc-savepoints-prod-<your-account-id>"

# KMS key for encryption (optional — leave empty for AWS-managed keys)
# Run: aws kms list-keys --region eu-west-2
kms_key_arn = ""

# DynamoDB table name prefix — tables will be named:
#   fasc-leader, fasc-coordinator-lock, fasc-savepoint-metadata
dynamodb_table_name_prefix = "fasc"

common_tags = {
  Project     = "flink-fasc"
  Environment = "production"
  ManagedBy   = "terraform"
  Team        = "data-engineering"
  Region      = "eu-west-2"
}
