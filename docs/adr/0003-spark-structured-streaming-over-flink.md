# ADR-0003: Spark Structured Streaming over Flink

**Status:** Accepted · 2026-07-10

## Context
The processing layer needs windowed aggregations, watermarking, a schema-validation gate, and transactional writes to Iceberg. Candidates: Apache Flink (true event-at-a-time streaming) and Spark Structured Streaming (micro-batch).

## Decision
Spark Structured Streaming.

1. **Iceberg maturity:** the Spark–Iceberg integration is the reference implementation — atomic commits, `MERGE`, compaction procedures all first-class. The Flink–Iceberg sink is workable but fussier around checkpoint/commit alignment.
2. **Latency requirement is honest:** the dashboard SLO is 2-minute freshness. Micro-batches of 10–30s clear that with an order of magnitude to spare; paying Flink's operational complexity for sub-second latency nobody consumes is resume-driven engineering.
3. **Depth over novelty:** author's production background is Spark; the portfolio should demonstrate depth (tuning, checkpoint semantics, skew handling) rather than a first-contact framework tour.

## Alternatives
- **Flink:** correct choice if the SLO were sub-second or per-event side effects were needed. Revisit if a CEP/alerting chapter is added.
- **Kafka Streams / ksqlDB:** JVM-app model doesn't fit the Iceberg-lakehouse sink requirement cleanly.

## Consequences
- Latency floor ≈ trigger interval; acceptable and documented on the dashboard.
- Exactly-once story leans on checkpoint + Iceberg atomic commits (ADR-0005).
- The trade-off writes itself up as blog material: "micro-batch was the right call, here's the math."
