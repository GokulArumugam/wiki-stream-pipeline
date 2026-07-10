.DEFAULT_GOAL := help

COMPOSE := docker compose
TOPIC := wiki.recentchange
BROKER := redpanda:9092

.PHONY: help up verify down nuke

help:
	@printf '%s\n' 'Targets: up, verify, down, nuke'

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

down:
	$(COMPOSE) down

nuke:
	$(COMPOSE) down --volumes --remove-orphans
