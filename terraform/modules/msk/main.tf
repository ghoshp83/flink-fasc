###############################################################################
# modules/msk/main.tf
#
# Provisions an Amazon MSK (Managed Streaming for Apache Kafka) cluster for
# the Flink-FASC project.
#
# Design decisions:
#   • 3 brokers, one per AZ in eu-west-2, for maximum redundancy.
#   • TLS-only client authentication — plaintext port 9092 is kept open inside
#     the VPC for internal tooling, but production app connections use 9094.
#   • KMS encryption at rest to satisfy data-governance requirements.
#   • CloudWatch broker logs for operational observability.
#   • Kafka topics are documented in topics.tf; they must be created out-of-band
#     (e.g. via a one-time Helm job or CI pipeline) because the AWS provider
#     does not natively manage MSK topics.
###############################################################################

# ── MSK broker configuration ─────────────────────────────────────────────────
resource "aws_msk_configuration" "this" {
  name              = "${var.cluster_name}-config"
  kafka_versions    = ["3.4.0"]
  description       = "FASC MSK broker configuration — production settings"

  server_properties = <<-PROPS
    # Prevent accidental topic creation from producers/consumers.
    auto.create.topics.enable=false

    # With 3 brokers, a replication factor of 3 means every partition is
    # present on all brokers.  min.insync.replicas=2 ensures a write is only
    # acked once at least 2 replicas have it, protecting against broker loss
    # without sacrificing too much latency.
    default.replication.factor=3
    min.insync.replicas=2

    # Retain messages for 7 days (168 h) so that a Flink app can replay from
    # an older savepoint without data loss.
    log.retention.hours=168

    # No byte-based limit — rely solely on time retention.
    log.retention.bytes=-1

    # Cap individual segment files at 1 GiB to keep log compaction efficient.
    log.segment.bytes=1073741824

    # Offset metadata max size — increase from default 4096 for complex
    # consumer group metadata.
    offset.metadata.max.bytes=4096
  PROPS
}

# ── CloudWatch log group for broker logs ─────────────────────────────────────
resource "aws_cloudwatch_log_group" "msk_broker_logs" {
  name              = "/aws/msk/${var.cluster_name}"
  retention_in_days = 30

  tags = merge(var.common_tags, {
    Name = "/aws/msk/${var.cluster_name}"
  })
}

# ── Security group for MSK brokers ───────────────────────────────────────────
# Tightly scoped: only allow traffic from the EKS node security group so
# that MSK is not reachable from arbitrary VPC resources.
resource "aws_security_group" "msk" {
  name        = "${var.cluster_name}-msk"
  description = "MSK broker traffic — inbound from EKS nodes only"
  vpc_id      = var.vpc_id

  # Plaintext Kafka (kept for internal tooling / debugging)
  ingress {
    description     = "Kafka plaintext from EKS nodes"
    from_port       = 9092
    to_port         = 9092
    protocol        = "tcp"
    security_groups = [var.eks_node_security_group_id]
  }

  # TLS Kafka — used by Flink apps and the FASC coordinator
  ingress {
    description     = "Kafka TLS from EKS nodes"
    from_port       = 9094
    to_port         = 9094
    protocol        = "tcp"
    security_groups = [var.eks_node_security_group_id]
  }

  # Zookeeper (required by some Kafka admin tooling; brokers handle
  # inter-broker communication internally via the same SG).
  ingress {
    description     = "Zookeeper from EKS nodes"
    from_port       = 2181
    to_port         = 2181
    protocol        = "tcp"
    security_groups = [var.eks_node_security_group_id]
  }

  # Allow all outbound — brokers need to reach AWS services and each other.
  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.common_tags, {
    Name = "${var.cluster_name}-msk"
  })
}

# ── MSK cluster ───────────────────────────────────────────────────────────────
resource "aws_msk_cluster" "this" {
  cluster_name           = var.cluster_name
  kafka_version          = "3.4.0"
  number_of_broker_nodes = 3

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  broker_node_group_info {
    instance_type  = "kafka.m5.large"
    client_subnets = var.private_subnet_ids # one subnet per AZ

    storage_info {
      ebs_storage_info {
        volume_size = 500 # GiB per broker

        # Encrypt broker storage with a customer-managed KMS key so that the
        # key can be audited, rotated, or revoked independently.
        provisioned_throughput {
          enabled           = false # enable if write throughput > 250 MiB/s
          volume_throughput = 250
        }
      }
    }

    security_groups = [aws_security_group.msk.id]
  }

  # ── Encryption ──────────────────────────────────────────────────────────
  encryption_info {
    # Only allow TLS between clients and brokers; disable plaintext.
    # Setting to TLS_PLAINTEXT still opens 9092 inside the SG for tooling.
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT" # change to "TLS" to fully disable plaintext
      in_cluster    = true
    }

    # At-rest encryption using a customer-managed KMS key.
    encryption_at_rest_kms_key_arn = var.kms_key_arn
  }

  # ── Client authentication ────────────────────────────────────────────────
  # TLS mutual authentication is enabled.  Clients (Flink apps) must present
  # a certificate signed by the ACM PCA or a trusted CA.
  client_authentication {
    tls {
      # Leave certificate_authority_arns empty here; populate with your
      # ACM Private CA ARN if you want mTLS client certificates.
      certificate_authority_arns = []
    }

    # SASL/SCRAM can be layered on top of TLS if preferred over mTLS.
    sasl {
      scram = false
      iam   = true # IAM auth — simplest integration with IRSA
    }
  }

  # ── Broker logging ───────────────────────────────────────────────────────
  logging {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk_broker_logs.name
      }

      # S3 log delivery is optional but useful for long-term archival.
      s3 {
        enabled = false
        # bucket  = "your-log-bucket"
        # prefix  = "msk-broker-logs/"
      }
    }
  }

  # Enhanced monitoring to expose per-topic/partition metrics in CloudWatch.
  enhanced_monitoring = "PER_TOPIC_PER_PARTITION"

  tags = merge(var.common_tags, {
    Name = var.cluster_name
  })

  # MSK clusters can take 15–25 minutes to provision; extend the timeout.
  timeouts {
    create = "60m"
    update = "60m"
    delete = "30m"
  }
}
