###############################################################################
# modules/msk/topics.tf
#
# MSK topic definitions — OUT-OF-BAND CREATION
#
# The AWS Terraform provider does not have a native resource for MSK/Kafka
# topics.  The two options are:
#
#   Option A) null_resource + local-exec running kafka-topics.sh
#             (requires the kafka CLI on the Terraform runner and network
#              access to the MSK brokers — awkward in CI).
#
#   Option B) A post-deploy Kubernetes Job (Helm) that runs inside EKS and
#             has network access to MSK (preferred for production).
#
# This file documents the required topics and provides the kafka-topics.sh
# commands so that they can be run manually or wired into a CI step.
#
# ── Topic specifications ─────────────────────────────────────────────────────
#
# 1. business-topic
#    Purpose   : Main business-event stream consumed by both Flink apps.
#    Partitions: 24  (matches the Flink parallelism so that each task gets
#                     exactly one partition, minimising rebalancing overhead)
#    Replication factor: 3
#    Retention : 604800000 ms (7 days) — allows replay from week-old savepoints
#    Cleanup   : delete
#
# 2. fasc-control-topic
#    Purpose   : Control channel used by the FASC coordinator to publish
#                leadership changes and savepoint commands to both Flink apps.
#    Partitions: 1   (strictly ordered; the coordinator is the sole producer)
#    Replication factor: 3
#    Retention : 86400000 ms (24 hours) — short; apps must be healthy within
#                a day of a control event being published.
#    Cleanup   : delete
#
# ── Creation commands ────────────────────────────────────────────────────────
#
#   BOOTSTRAP=$(terraform output -raw msk_bootstrap_brokers_tls)
#
#   # business-topic
#   kafka-topics.sh \
#     --bootstrap-server "$BOOTSTRAP" \
#     --command-config client.properties \
#     --create --if-not-exists \
#     --topic business-topic \
#     --partitions 24 \
#     --replication-factor 3 \
#     --config retention.ms=604800000 \
#     --config cleanup.policy=delete
#
#   # fasc-control-topic
#   kafka-topics.sh \
#     --bootstrap-server "$BOOTSTRAP" \
#     --command-config client.properties \
#     --create --if-not-exists \
#     --topic fasc-control-topic \
#     --partitions 1 \
#     --replication-factor 3 \
#     --config retention.ms=86400000 \
#     --config cleanup.policy=delete
#
# ── Kubernetes Job (Option B) ─────────────────────────────────────────────────
# A ready-to-use Helm chart / Kubernetes Job manifest is provided under
# ../../helm/msk-topic-init/  which runs the above commands from inside the
# cluster where MSK is reachable.
###############################################################################

# No Terraform resources in this file.
# All topic management is performed out-of-band as documented above.
