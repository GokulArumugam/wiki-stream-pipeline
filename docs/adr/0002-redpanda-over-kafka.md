# ADR-0002: Redpanda over Apache Kafka

**Status:** Accepted · 2026-07-10

## Context
Need a Kafka-API broker inside Docker Compose on a laptop, alongside Spark, MinIO, Prometheus and Grafana competing for RAM.

## Decision
Redpanda: single binary, no ZooKeeper/KRaft coordination overhead, ~10x lighter at idle, ships `rpk` for topic ops. It implements the Kafka wire protocol, so every client, the Spark Kafka source, and all producer semantics (idempotence, acks) work unchanged.

## Alternatives
- **Apache Kafka (KRaft):** the incumbent; heavier locally, no API difference for this use case. Everything written here runs against it — and MSK — unmodified.
- **Redis Streams / NATS:** lighter still, but abandons the Kafka API and with it the transferability story.

## Consequences
- All code is Kafka-compatible; the cloud path targets MSK without changes.
- Interview positioning: "Kafka API" skills demonstrated; Redpanda-vs-Kafka trade-off itself becomes talkable material.
