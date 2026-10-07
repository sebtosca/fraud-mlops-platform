#!/bin/sh
# Creates the platform topics. Safe to re-run: existing topics are left unchanged.
set -eu

BOOTSTRAP="fraud-kafka-kafka-bootstrap:9092"
PREFIX="${TOPIC_PREFIX:?TOPIC_PREFIX must be set}"
TOPICS=/opt/kafka/bin/kafka-topics.sh

create() {
  "$TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists "$@"
}

# The partition count of transactions never changes: it decides which consumer owns
# each card's state (ARCHITECTURE invariants). retention.ms=-1 because the payload
# carries 2020 event times and wall-clock retention would delete it.
create --topic "$PREFIX.transactions" --partitions 6 --replication-factor 1 --config retention.ms=-1
create --topic "$PREFIX.labels" --partitions 6 --replication-factor 1 --config retention.ms=-1
create --topic "$PREFIX.sim-clock" --partitions 1 --replication-factor 1

"$TOPICS" --bootstrap-server "$BOOTSTRAP" --describe --topic "$PREFIX.transactions"
