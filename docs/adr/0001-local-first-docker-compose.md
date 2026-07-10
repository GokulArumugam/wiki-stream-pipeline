# ADR-0001: Local-first on Docker Compose, cloud as a documented path

**Status:** Accepted · 2026-07-10

## Context
The pipeline must be demonstrable to anyone (recruiters, interviewers) without a standing cloud bill, and reproducible with one command. A portfolio pipeline that requires provisioned infra rots the week it stops being paid for.

## Decision
Run everything on a single machine with Docker Compose. Choose only components that speak cloud-standard APIs — Kafka API (Redpanda ↔ MSK), S3 API (MinIO ↔ S3) — so the AWS deployment is a configuration change documented in `docs/aws-deploy.md`, not a rewrite.

## Consequences
- `git clone && docker compose up` reproduces the whole system — reproducibility is itself a portfolio signal.
- No multi-node failure modes (broker quorum loss, network partitions) can be demonstrated; the chaos demo is limited to process-level kills.
- Throughput ceilings are laptop-bound; cost/scale sections extrapolate from measured local numbers and say so explicitly.
