# ADR-0004: Apache Iceberg on MinIO

**Status:** Accepted · 2026-07-10

## Context
The lakehouse layer needs ACID writes from a streaming job, schema evolution (Wikimedia payloads drift), time travel for debugging, and S3-API storage that maps 1:1 to AWS later.

## Decision
Iceberg tables on MinIO, with a REST catalog container (portable to Glue catalog on AWS).

- **Iceberg over Delta Lake:** engine-neutral (Spark, Trino, DuckDB all read it — DuckDB matters because the website queries exported artifacts), open spec governance, hidden partitioning avoids the partition-column foot-guns. Delta's tightest integration is Spark-only.
- **Iceberg over Hudi:** simpler mental model; Hudi's strengths (record-level upserts at scale) aren't the workload here.
- **MinIO over local FS:** exercises the real S3 code path (multipart uploads, path-style access), so the AWS migration is `endpoint` + credentials.

## Consequences
- Streaming appends create small files → scheduled compaction (`rewrite_data_files`) runs as part of the pipeline and gets its own README section — a deliberately showcased ops concern.
- Snapshot expiry must be scheduled or metadata grows unbounded; documented alongside compaction.
- Author's Hive-ACID production experience translates: same problems (small files, compaction), modern format.
