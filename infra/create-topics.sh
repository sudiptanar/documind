#!/usr/bin/env bash
# Creates the Kafka topics. Safe to re-run. The services also declare these topics on startup,
# so this is mainly for a fresh broker you want to inspect before any service has run.
set -euo pipefail
export MSYS_NO_PATHCONV=1  # stops Git Bash on Windows rewriting /opt/... paths
cd "$(dirname "$0")"
for t in document.uploaded document.indexed document.failed document.uploaded.DLT; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic "$t" --partitions 3 --replication-factor 1
done
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
