# ── FASC IAM Module ───────────────────────────────────────────────────────────
# Creates IRSA (IAM Roles for Service Accounts) for:
#   1. Flink App pods (app1 + app2) — shared role
#   2. FASC Coordinator pods — separate role with broader DynamoDB permissions
#
# IRSA allows Kubernetes service accounts to assume IAM roles without
# long-lived credentials stored in pods or Kubernetes secrets.

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Extract the OIDC provider ID from the full URL (strip https://)
locals {
  oidc_provider_id = replace(var.cluster_oidc_issuer_url, "https://", "")
}

# ── 1. Flink App IAM Role (shared by App1 and App2) ──────────────────────────

data "aws_iam_policy_document" "flink_app_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/${local.oidc_provider_id}"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_id}:sub"
      # Both app1 and app2 use the "flink-app" service account in the "flink" namespace
      values   = ["system:serviceaccount:flink:flink-app"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_id}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flink_app" {
  name               = "fasc-flink-app-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.flink_app_trust.json

  tags = merge(var.common_tags, {
    Name      = "fasc-flink-app-${var.environment}"
    Component = "flink-app"
  })
}

data "aws_iam_policy_document" "flink_app_policy" {
  # DynamoDB: read the leader record to check write permission before each batch
  statement {
    sid    = "DynamoDbLeaderRead"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
    ]
    resources = [var.fasc_leader_table_arn]
  }

  # MSK: discover broker endpoints
  statement {
    sid    = "MskDescribe"
    effect = "Allow"
    actions = [
      "kafka:DescribeCluster",
      "kafka:GetBootstrapBrokers",
      "kafka:ListScramSecrets",
    ]
    resources = [var.msk_cluster_arn]
  }

  # S3: read/write Flink checkpoints and savepoints
  statement {
    sid    = "S3CheckpointsAndSavepoints"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      var.s3_bucket_arn,
      "${var.s3_bucket_arn}/fasc/*",
    ]
  }

  # CloudWatch: publish FASC metrics from ShadowSink
  statement {
    sid    = "CloudWatchMetrics"
    effect = "Allow"
    actions = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["FlinkFASC"]
    }
  }
}

resource "aws_iam_role_policy" "flink_app" {
  name   = "fasc-flink-app-policy"
  role   = aws_iam_role.flink_app.id
  policy = data.aws_iam_policy_document.flink_app_policy.json
}

# ── 2. FASC Coordinator IAM Role ─────────────────────────────────────────────

data "aws_iam_policy_document" "fasc_coordinator_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/${local.oidc_provider_id}"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_id}:sub"
      values   = ["system:serviceaccount:fasc:fasc-coordinator"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_id}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "fasc_coordinator" {
  name               = "fasc-coordinator-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.fasc_coordinator_trust.json

  tags = merge(var.common_tags, {
    Name      = "fasc-coordinator-${var.environment}"
    Component = "fasc-coordinator"
  })
}

data "aws_iam_policy_document" "fasc_coordinator_policy" {
  # Full access to all three FASC DynamoDB tables (leader election, lock, savepoint metadata)
  statement {
    sid    = "DynamoDbFascTables"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:Query",
      "dynamodb:Scan",
    ]
    resources = [
      var.fasc_leader_table_arn,
      var.coordinator_lock_table_arn,
      var.savepoint_metadata_table_arn,
    ]
  }

  # S3: read and write savepoints triggered on behalf of Flink apps
  statement {
    sid    = "S3Savepoints"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject",
    ]
    resources = [
      var.s3_bucket_arn,
      "${var.s3_bucket_arn}/fasc/savepoints/*",
    ]
  }

  # CloudWatch: publish coordinator-level metrics
  statement {
    sid    = "CloudWatchMetrics"
    effect = "Allow"
    actions = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["FlinkFASC"]
    }
  }

  # MSK: read broker info to monitor consumer groups via AdminClient
  statement {
    sid    = "MskDescribe"
    effect = "Allow"
    actions = [
      "kafka:DescribeCluster",
      "kafka:GetBootstrapBrokers",
      "kafka:DescribeClusterV2",
    ]
    resources = [var.msk_cluster_arn]
  }

  # CloudWatch Logs: publish coordinator operational logs
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/fasc/*",
    ]
  }
}

resource "aws_iam_role_policy" "fasc_coordinator" {
  name   = "fasc-coordinator-policy"
  role   = aws_iam_role.fasc_coordinator.id
  policy = data.aws_iam_policy_document.fasc_coordinator_policy.json
}
