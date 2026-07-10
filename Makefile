.DEFAULT_GOAL := help

COMPOSE := docker compose
TOPIC := wiki.recentchange
BROKER := redpanda:9092

.PHONY: help up verify down nuke spark-up verify-spark spark-down

SPARK_PACKAGES := org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5
SPARK_CATALOG_CONF := --conf spark.jars.packages=$(SPARK_PACKAGES) --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.local.catalog-impl=org.apache.iceberg.rest.RESTCatalog --conf spark.sql.catalog.local.uri=http://iceberg-rest:8181 --conf spark.sql.catalog.local.warehouse=s3://warehouse/ --conf spark.sql.catalog.local.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.local.s3.endpoint=http://minio:9000 --conf spark.sql.catalog.local.s3.path-style-access=true --conf spark.sql.catalog.local.s3.access-key-id=minioadmin --conf spark.sql.catalog.local.s3.secret-access-key=minioadmin --conf spark.sql.catalog.local.s3.region=us-east-1
# Counts come from REST catalog snapshot summaries: a second spark-submit
# inside the 2g streaming container OOM-kills the driver.
SPARK_COUNT := python3 scripts/iceberg_counts.py

help:
	@printf '%s\n' 'Targets: up, verify, spark-up, verify-spark, spark-down, down, nuke'

up:
	$(COMPOSE) up -d --build
	@printf '%s\n' 'Waiting for required services to become healthy...'
	@for service in redpanda minio iceberg-rest ingest prometheus grafana; do \
		deadline=$$(( $$(date +%s) + 120 )); \
		while :; do \
			status=$$($(COMPOSE) ps --format json $$service 2>/dev/null | \
				python3 -c 'import json,sys; rows=[json.loads(l) for l in sys.stdin if l.strip()]; print(rows[0].get("Health") or rows[0].get("State", "") if rows else "")' 2>/dev/null || true); \
			if [ "$$status" = "healthy" ]; then break; fi; \
			if [ $$(date +%s) -ge $$deadline ]; then \
				echo "$$service did not become healthy (status: $$status)"; $(COMPOSE) ps; exit 1; \
			fi; \
			sleep 2; \
		done; \
		echo "$$service is healthy"; \
	done

verify:
	@printf '%s\n' 'Waiting up to 60 seconds for at least 100 events in $(TOPIC)...'
	@deadline=$$(( $$(date +%s) + 60 )); \
	while :; do \
		output=$$(mktemp); \
		$(COMPOSE) exec -T redpanda rpk topic consume $(TOPIC) --brokers=$(BROKER) --offset=start --num 100 --format '%v\n' > $$output 2>/dev/null & \
		consumer_pid=$$!; \
		consumer_deadline=$$(( $$(date +%s) + 4 )); \
		while kill -0 $$consumer_pid 2>/dev/null; do \
			if [ $$(date +%s) -ge $$consumer_deadline ]; then kill $$consumer_pid 2>/dev/null || true; fi; \
			sleep 1; \
		done; \
		wait $$consumer_pid 2>/dev/null || true; \
		count=$$(wc -l < $$output | tr -d ' '); rm -f $$output; \
		if [ "$$count" -ge 100 ]; then break; fi; \
		if [ $$(date +%s) -ge $$deadline ]; then \
			echo "FAIL: observed $$count events; expected at least 100 within 60 seconds."; \
			$(COMPOSE) logs --tail=80 ingest; exit 1; \
		fi; \
		sleep 2; \
	done; \
	echo "PASS: observed $$count events."; \
	echo 'Sample event:'; \
	$(COMPOSE) exec -T redpanda rpk topic consume $(TOPIC) --brokers=$(BROKER) --num 1 --format '%v\n'; \
	echo 'Per-partition counts:'; \
	$(COMPOSE) exec -T redpanda rpk topic describe $(TOPIC) --brokers=$(BROKER)

spark-up:
	$(COMPOSE) --profile spark up -d --build spark
	@printf '%s\n' 'Waiting for Spark metrics endpoint to become healthy...'
	@deadline=$$(( $$(date +%s) + 180 )); \
	while :; do \
		status=$$($(COMPOSE) ps --format json spark 2>/dev/null | \
			python3 -c 'import json,sys; rows=[json.loads(l) for l in sys.stdin if l.strip()]; print(rows[0].get("Health") or rows[0].get("State", "") if rows else "")' 2>/dev/null || true); \
		if [ "$$status" = "healthy" ]; then break; fi; \
		if [ $$(date +%s) -ge $$deadline ]; then \
			echo "spark did not become healthy (status: $$status)"; $(COMPOSE) logs --tail=120 spark; exit 1; \
		fi; \
		sleep 3; \
	done; \
	echo 'spark is healthy'

verify-spark:
	@count_for() { printf '%s\n' "$$1" | sed -n "s/.* $$2=\\([0-9][0-9]*\\).*/\\1/p"; }; \
	first=$$($(SPARK_COUNT) 2>/dev/null | awk '/^COUNTS / { line=$$0 } END { print line }'); \
	[ -n "$$first" ] || { echo 'FAIL: could not read initial Iceberg counts'; exit 1; }; \
	first_bronze=$$(count_for "$$first" bronze); \
	first_gold=$$(( $$(count_for "$$first" edits) + $$(count_for "$$first" bots) + $$(count_for "$$first" pages) + $$(count_for "$$first" late) )); \
	echo "First sample: $$first"; \
	echo 'Waiting 60 seconds for another Spark micro-batch...'; sleep 60; \
	second=$$($(SPARK_COUNT) 2>/dev/null | awk '/^COUNTS / { line=$$0 } END { print line }'); \
	[ -n "$$second" ] || { echo 'FAIL: could not read second Iceberg counts'; exit 1; }; \
	second_bronze=$$(count_for "$$second" bronze); \
	second_gold=$$(( $$(count_for "$$second" edits) + $$(count_for "$$second" bots) + $$(count_for "$$second" pages) + $$(count_for "$$second" late) )); \
	echo "Second sample: $$second"; \
	if [ "$$second_bronze" -le "$$first_bronze" ] || [ "$$second_gold" -le "$$first_gold" ]; then \
		echo "FAIL: expected bronze and gold growth (bronze $$first_bronze -> $$second_bronze; gold $$first_gold -> $$second_gold)"; exit 1; \
	fi; \
	freshness=$$(curl -fsS http://localhost:9109/metrics | awk '$$1 == "spark_pipeline_freshness_seconds" { print $$2; exit }'); \
	if ! awk -v value="$$freshness" 'BEGIN { exit !(value ~ /^[0-9.]+$$/ && value < 300) }'; then \
		echo "FAIL: freshness must be numeric and below 300 seconds (got: $$freshness)"; exit 1; \
	fi; \
	echo "PASS: bronze $$first_bronze -> $$second_bronze; gold $$first_gold -> $$second_gold; freshness=$${freshness}s"

spark-down:
	$(COMPOSE) --profile spark stop spark

down:
	$(COMPOSE) --profile spark down

nuke:
	$(COMPOSE) --profile spark down --volumes --remove-orphans
