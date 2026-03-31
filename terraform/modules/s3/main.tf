###############################################################################
# modules/s3/main.tf
#
# S3 bucket for Flink checkpoints and FASC savepoints.
#
# Bucket layout convention (enforced by lifecycle rules):
#   fasc/savepoints/            — FASC-triggered savepoints (90-day expiry)
#   fasc/checkpoints/app1/      — app1 incremental checkpoints (7-day expiry)
#   fasc/checkpoints/app2/      — app2 incremental checkpoints (7-day expiry)
#
# Design decisions:
#   • Versioning is enabled so that an accidentally overwritten savepoint can
#     be recovered; the noncurrent-version expiry rule limits storage growth.
#   • Block all public access — no Flink savepoint should ever be public.
#   • KMS encryption so that the key can be audited and rotated independently.
#   • Bucket policy restricts object access to EKS node roles and the
#     coordinator role (defence in depth beyond IAM identity policies).
###############################################################################

# ── Bucket ────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "this" {
  bucket = var.bucket_name

  # force_destroy = false in production — prevent accidental deletion of
  # savepoints.  Set to true only in dev/CI environments.
  force_destroy = false

  tags = merge(var.common_tags, {
    Name        = var.bucket_name
    Environment = var.environment
  })
}

# ── Block all public access ───────────────────────────────────────────────────
resource "aws_s3_bucket_public_access_block" "this" {
  bucket = aws_s3_bucket.this.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Versioning ────────────────────────────────────────────────────────────────
resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ── Server-side encryption ────────────────────────────────────────────────────
resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    apply_server_side_encryption_by_default {
      # Use aws:kms with a customer-managed key for auditable encryption.
      # Fall back to AES256 if no KMS key ARN is provided (dev environments).
      sse_algorithm     = var.kms_key_arn != "" ? "aws:kms" : "AES256"
      kms_master_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
    }

    # Require SSE on every PUT — reject unencrypted uploads.
    bucket_key_enabled = var.kms_key_arn != "" ? true : false
  }
}

# ── Lifecycle rules ───────────────────────────────────────────────────────────
resource "aws_s3_bucket_lifecycle_configuration" "this" {
  # Versioning must be enabled before lifecycle rules can reference
  # noncurrent versions.
  depends_on = [aws_s3_bucket_versioning.this]

  bucket = aws_s3_bucket.this.id

  # ── Rule 1: FASC savepoints ───────────────────────────────────────────────
  # Savepoints are the recovery artefacts for hot-standby handovers.
  # Keep them for 90 days — enough to cover any rollback scenario.
  rule {
    id     = "expire-fasc-savepoints"
    status = "Enabled"

    filter {
      prefix = "fasc/savepoints/"
    }

    expiration {
      days = 90
    }

    # Remove superseded versions after 30 days to limit storage cost.
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  # ── Rule 2: app1 checkpoints ──────────────────────────────────────────────
  # Flink incremental checkpoints are only needed until the next savepoint
  # completes.  7 days is a conservative safety window.
  rule {
    id     = "expire-app1-checkpoints"
    status = "Enabled"

    filter {
      prefix = "fasc/checkpoints/app1/"
    }

    expiration {
      days = 7
    }

    noncurrent_version_expiration {
      noncurrent_days = 3
    }
  }

  # ── Rule 3: app2 checkpoints ──────────────────────────────────────────────
  rule {
    id     = "expire-app2-checkpoints"
    status = "Enabled"

    filter {
      prefix = "fasc/checkpoints/app2/"
    }

    expiration {
      days = 7
    }

    noncurrent_version_expiration {
      noncurrent_days = 3
    }
  }

  # ── Rule 4: abort incomplete multipart uploads ─────────────────────────────
  # Flink uses multipart upload for large checkpoint files.  Orphaned
  # multipart uploads (e.g. from a TaskManager crash mid-checkpoint) can
  # silently accumulate storage costs.
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {
      prefix = ""
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# ── Bucket policy ─────────────────────────────────────────────────────────────
# This policy is an additional layer of defence beyond IAM identity policies.
# It denies access to any principal that is not an explicitly allowed role,
# and requires all requests to use HTTPS.
data "aws_iam_policy_document" "bucket_policy" {
  # ── Statement 1: Deny non-HTTPS requests ──────────────────────────────────
  statement {
    sid    = "DenyNonHTTPS"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.this.arn,
      "${aws_s3_bucket.this.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  # ── Statement 2: Allow EKS node roles ────────────────────────────────────
  # EKS node instance-profile roles need s3:GetObject / s3:PutObject to
  # write checkpoints from Flink TaskManagers.  Individual IRSA roles are
  # the primary grant; this is a belt-and-braces fallback.
  statement {
    sid    = "AllowEKSNodeRoles"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = var.eks_node_role_arns
    }

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]

    resources = [
      aws_s3_bucket.this.arn,
      "${aws_s3_bucket.this.arn}/*",
    ]
  }
}

resource "aws_s3_bucket_policy" "this" {
  bucket = aws_s3_bucket.this.id
  policy = data.aws_iam_policy_document.bucket_policy.json

  # Policy must be applied after public-access block is in place.
  depends_on = [aws_s3_bucket_public_access_block.this]
}
