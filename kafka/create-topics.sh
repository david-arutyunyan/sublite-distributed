#!/usr/bin/env bash
set -euo pipefail

# Runs inside the kafka-init container (see docker-compose.yml), talking
# to the broker over the internal PLAINTEXT listener - kafka:19092, not
# localhost:9092 (that's the host-facing listener, unreachable from
# another container).
BOOTSTRAP="kafka:19092"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

# 3 partitions on every topic: enough to actually see partitioning/
# rebalancing behavior locally without any real load to justify more.
# replication-factor 1 because there's exactly one broker - anything
# higher would just fail to create.
#
# One DLQ per source topic, same naming convention Kafka itself uses
# internally (dot-suffixed) - the consumers that need retry+DLQ land in
# later steps, but topics are cheap to create now and match the schema
# in docs/architecture.md.
TOPICS=(
  subscription.events
  subscription.events.DLQ
  retention.events
  retention.events.DLQ
  billing.events
  billing.events.DLQ
  loyalty.events
  loyalty.events.DLQ
  notification.events
  notification.events.DLQ
)

for topic in "${TOPICS[@]}"; do
  echo "Creating topic: $topic"
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
done

echo "Topics:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
