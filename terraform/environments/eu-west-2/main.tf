terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
  }

  # Remote state in S3 — update bucket/key for your organisation
  backend "s3" {
    bucket         = "your-terraform-state-bucket"
    key            = "fasc/eu-west-2/terraform.tfstate"
    region         = "eu-west-2"
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = merge(var.common_tags, {
      Environment = var.environment
      Region      = var.aws_region
    })
  }
}

# ── S3 Bucket (no dependencies) ───────────────────────────────────────────────
module "s3" {
  source = "../../modules/s3"

  bucket_name  = var.bucket_name
  kms_key_arn  = var.kms_key_arn
  environment  = var.environment
  common_tags  = var.common_tags

  # EKS node role ARNs are added after EKS is created
  # Initial deploy creates the bucket; IAM permissions are added via the iam module
  eks_node_role_arns = []
}

# ── DynamoDB Tables (no dependencies) ─────────────────────────────────────────
module "dynamodb" {
  source = "../../modules/dynamodb"

  table_name_prefix = var.dynamodb_table_name_prefix
  environment       = var.environment
  common_tags       = var.common_tags
}

# ── EKS Cluster ───────────────────────────────────────────────────────────────
module "eks" {
  source = "../../modules/eks"

  cluster_name       = var.cluster_name
  vpc_id             = var.vpc_id
  private_subnet_ids = var.private_subnet_ids
  environment        = var.environment
  common_tags        = var.common_tags
}

# ── MSK Cluster (depends on EKS security group for inbound rules) ─────────────
module "msk" {
  source = "../../modules/msk"

  cluster_name                 = "${var.cluster_name}-msk"
  vpc_id                       = var.vpc_id
  private_subnet_ids           = var.private_subnet_ids
  eks_node_security_group_id   = module.eks.node_security_group_id
  kms_key_arn                  = var.kms_key_arn
  environment                  = var.environment
  common_tags                  = var.common_tags
}

# ── IAM IRSA Roles (depends on EKS OIDC + all resource ARNs) ─────────────────
module "iam" {
  source = "../../modules/iam"

  environment                  = var.environment
  cluster_oidc_issuer_url      = module.eks.cluster_oidc_issuer_url
  fasc_leader_table_arn        = module.dynamodb.fasc_leader_table_arn
  coordinator_lock_table_arn   = module.dynamodb.coordinator_lock_table_arn
  savepoint_metadata_table_arn = module.dynamodb.savepoint_metadata_table_name == "" ? "" : module.dynamodb.savepoint_metadata_table_arn
  s3_bucket_arn                = module.s3.bucket_arn
  msk_cluster_arn              = module.msk.cluster_arn
  common_tags                  = var.common_tags
}
