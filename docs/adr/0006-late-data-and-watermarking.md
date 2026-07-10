# ADR-0006: Watermarking and late data policy

**Status:** Accepted · 2026-07-10

## Context
Aggregations window on **event time** (`meta.dt` from Wikimedia), not processing time. Events arrive late for real reasons: SSE reconnect replay, ingest backpressure, upstream delays. A policy must say how late is tolerated and what happens past that.

## Decision
- **Watermark: 2 minutes** on event time for all windowed aggregates. Chosen from measured skew (`ingested_at - event_time`) during a 1-hour capture; will be revisited with the observed p99.9 and the analysis published in the design doc.
- Windows: 1-minute tumbling for rate panels; 10-minute for top-N panels.
- **Too-late events are not dropped silently:** a side count per wiki lands in `gold.late_arrivals` and is charted in Grafana. Dashboards tolerate ~2-minute-stale trends; the *volume* of late data is itself a monitored signal.
- Bronze keeps every event regardless of lateness — aggregates are rebuildable with a different watermark, and that rebuild is demonstrated once in the design doc.

## Consequences
- Freshness SLO (2 min) and watermark (2 min) compose to a worst-case ~4-minute end-to-end staleness for a fully-settled window; stated on the dashboard.
- Dedup state retention (ADR-0005) is tied to the same watermark, bounding memory.
- Trade-off documented: shorter watermark = fresher aggregates + more late-arrival exclusions; the late-arrivals panel makes that trade-off *visible* instead of asserted.
