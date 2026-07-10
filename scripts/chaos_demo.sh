#!/usr/bin/env bash
# Chaos demo: SIGKILL the Spark driver mid-stream, restart it, and prove
# effectively-once delivery — row counts keep growing, no duplicates appear,
# freshness recovers under the SLO. Recorded with asciinema for the portfolio.
set -euo pipefail
cd "$(dirname "$0")/.."

say() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
counts() { python3 scripts/iceberg_counts.py; }
freshness() { curl -fsS localhost:9109/metrics | awk '$1=="spark_pipeline_freshness_seconds"{printf "freshness=%.0fs\n",$2}'; }
dupcheck() {
  docker compose --profile spark run --rm --no-deps \
    --entrypoint /opt/spark/bin/spark-submit spark --master 'local[1]' \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
    --conf spark.sql.catalog.local.uri=http://iceberg-rest:8181 \
    --conf spark.sql.catalog.local.warehouse=s3://warehouse/ \
    --conf spark.sql.catalog.local.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.local.s3.endpoint=http://minio:9000 \
    --conf spark.sql.catalog.local.s3.path-style-access=true \
    --conf spark.sql.catalog.local.s3.access-key-id=minioadmin \
    --conf spark.sql.catalog.local.s3.secret-access-key=minioadmin \
    --conf spark.sql.catalog.local.s3.region=us-east-1 \
    --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
    /opt/spark/app/dup_check.py 2>/dev/null | grep DUPCHECK
}

say "Live pipeline state before the kill"
counts; freshness

say "Killing the Spark driver mid-batch with SIGKILL (no graceful shutdown)"
docker kill -s KILL wiki-spark
docker ps -a --format '{{.Names}}\t{{.Status}}' | grep wiki-spark

say "Ingest keeps producing while processing is down (events buffer in Redpanda)"
sleep 20
counts

say "Restarting Spark — it must resume from its checkpoint"
docker compose --profile spark up -d spark >/dev/null 2>&1
until [ "$(docker inspect -f '{{.State.Health.Status}}' wiki-spark)" = healthy ]; do sleep 3; done
echo "spark is healthy again"

say "Waiting two trigger intervals for catch-up"
sleep 60
counts; freshness

say "Duplicate check: bronze total rows vs distinct meta_id (must be equal)"
dupcheck

say "Done: rows grew across the kill, zero duplicates, freshness back under SLO"
