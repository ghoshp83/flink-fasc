###############################################################################
# modules/dynamodb/main.tf
#
# Three DynamoDB tables that form the FASC coordination backbone:
#
#   1. fasc-leader              — single-item table tracking which Flink app
#                                  is currently the active leader.
#   2. fasc-coordinator-lock    — distributed lock table used by coordinator
#                                  replicas to prevent split-brain.
#   3. fasc-savepoint-metadata  — stores savepoint S3 paths and status so
#                                  both apps can restore from a known-good
#                                  checkpoint after a leadership handover.
#
# All tables use PAY_PER_REQUEST (on-demand) billing so that throughput
# scales automatically without capacity planning.  Point-in-time recovery
# is enabled on all tables for disaster-recovery.
###############################################################################

# ── 1. fasc-leader ────────────────────────────────────────────────────────────
#
# Item schema (example):
#   PK                  = "active-flink-app"   (partition key, always this value)
#   activeApp           = "app1"               (current leader)
#   leaderSince         = "2026-01-15T10:30:00Z"
#   lastHeartbeat       = "2026-01-15T10:31:55Z"
#   version             = 42                   (optimistic-locking counter)
#
# The coordinator uses conditional writes (version attribute) to implement
# optimistic locking — preventing two coordinator replicas from writing
# simultaneously and causing inconsistent leader state.
resource "aws_dynamodb_table" "fasc_leader" {
  name         = "${var.table_name_prefix}-leader"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  # Point-in-time recovery lets us restore to any second within the past 35
  # days — important given this table drives zero-downtime switchovers.
  point_in_time_recovery {
    enabled = true
  }

  # Server-side encryption using an AWS-managed key (default).
  # For compliance environments, replace with a CMK by adding:
  #   server_side_encryption { enabled = true; kms_key_arn = var.kms_key_arn }
  server_side_encryption {
    enabled = true
  }

  tags = merge(var.common_tags, {
    Name        = "${var.table_name_prefix}-leader"
    FascRole    = "leader-election"
    Environment = var.environment
  })
}

# ── 2. fasc-coordinator-lock ──────────────────────────────────────────────────
#
# Item schema (example):
#   pk         = "coordinator-lock"   (partition key)
#   holder     = "fasc-pod-0"         (who holds the lock)
#   acquiredAt = "2026-01-15T10:30:00Z"
#   ttlEpoch   = 1705319455           (Unix epoch; DynamoDB auto-deletes expired items)
#
# TTL ensures stale locks (e.g. from a crashed coordinator pod) are
# automatically removed, avoiding indefinite deadlocks.  The coordinator
# must renew the TTL before it expires to keep the lock active.
resource "aws_dynamodb_table" "fasc_coordinator_lock" {
  name         = "${var.table_name_prefix}-coordinator-lock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  # TTL attribute — DynamoDB will delete items where ttlEpoch < current time.
  # Set ttlEpoch = now + 30 seconds in coordinator; renew every 10 seconds.
  ttl {
    attribute_name = "ttlEpoch"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = merge(var.common_tags, {
    Name        = "${var.table_name_prefix}-coordinator-lock"
    FascRole    = "distributed-lock"
    Environment = var.environment
  })
}

# ── 3. fasc-savepoint-metadata ────────────────────────────────────────────────
#
# Item schema (example):
#   pk            = "savepoint-latest"     (or "savepoint-{uuid}")
#   app           = "app1"                 (which app produced this savepoint)
#   s3Path        = "s3://bucket/fasc/savepoints/app1/sp-abc123"
#   triggeredAt   = "2026-01-15T10:30:00Z"
#   status        = "COMPLETED"            (PENDING | COMPLETED | FAILED)
#   kafkaOffsets  = "{...}"                (JSON: topic-partition -> offset)
#   ttlEpoch      = 1713300000             (auto-expire old savepoint records)
#
# Two well-known primary keys:
#   "savepoint-latest"   — always points to the last successfully completed
#                           savepoint; updated atomically after each handover.
#   "savepoint-{uuid}"   — historical savepoint entries; TTL-expired after 90d.
resource "aws_dynamodb_table" "fasc_savepoint_metadata" {
  name         = "${var.table_name_prefix}-savepoint-metadata"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  ttl {
    attribute_name = "ttlEpoch"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = merge(var.common_tags, {
    Name        = "${var.table_name_prefix}-savepoint-metadata"
    FascRole    = "savepoint-tracking"
    Environment = var.environment
  })
}
