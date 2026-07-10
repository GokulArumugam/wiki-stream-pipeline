# ADR-0005: Delivery semantics — effectively-once, stage by stage

**Status:** Accepted · 2026-07-10

## Context
"Exactly-once" is a per-stage claim, not a system-wide sticker. Each hop needs its own contract, and the honest composition is what a senior candidate should be able to defend.

## Decision
| Hop | Guarantee | Mechanism |
|---|---|---|
| Wikimedia SSE → ingest | At-least-once | Resume via `Last-Event-ID` on reconnect; overlap possible, gaps not (within stream retention) |
| Ingest → Redpanda | Exactly-once *per producer session* | Idempotent producer (`enable.idempotence`), `acks=all`; cross-restart dupes remain possible |
| Redpanda → Spark → Iceberg | Effectively exactly-once | Kafka offsets in Spark checkpoint + Iceberg atomic snapshot commit: a batch is either fully visible or not at all; replayed batches overwrite deterministically |
| Dedup | Keyed dedup in bronze→silver on `meta.id` (Wikimedia's unique event id) with a watermark-bounded state window | Catches SSE-overlap and producer-restart dupes |

Net: **effectively-once into gold tables**, with the residual risk (dupes older than the dedup watermark) quantified rather than hidden.

## Consequences
- The chaos demo has a precise claim to verify: kill the Spark job mid-batch, restart, show row counts and aggregates identical to an uninterrupted control run.
- Dedup state is bounded by the watermark window — memory cost is measurable and documented.
- Design doc gets a "where exactly-once actually lives" section; strong interview material.
