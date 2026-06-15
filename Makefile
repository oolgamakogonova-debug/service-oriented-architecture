# ──────────────────────────────────────────────────────────────────
# Удобные команды. Локально воспроизводят то, что делает CI.
# ──────────────────────────────────────────────────────────────────
.PHONY: help up up-ci down down-ci wait logs \
        test-unit test-integration test-e2e load slo ci-local seed

COMPOSE      ?= docker compose
CI_COMPOSE   ?= docker compose -f docker-compose.ci.yml

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## Поднять полный стенд (3-node Cassandra) — для разработки и защиты
	$(COMPOSE) up -d --build

up-ci: ## Поднять облегчённый CI-стенд (single-node Cassandra, RF=1)
	$(CI_COMPOSE) up -d --build

down: ## Остановить полный стенд и удалить тома
	$(COMPOSE) down -v

down-ci: ## Остановить CI-стенд
	$(CI_COMPOSE) down -v

wait: ## Дождаться готовности стенда
	bash scripts/wait_for_stack.sh

logs: ## Логи всех сервисов
	$(COMPOSE) logs -f --tail=100

test-unit: ## Unit-тесты (без инфраструктуры)
	pytest tests/unit/producer -v
	pytest tests/unit/consumer -v

test-integration: ## Интеграционные тесты (нужен поднятый стенд)
	pytest tests/integration -v

test-e2e: ## E2E-тесты (нужен поднятый стенд)
	pytest tests/e2e -v

load: ## Нагрузочный тест k6 (>=10 VU, >=30s)
	docker run --rm --network host -v "$$PWD":/work -w /work \
		-e BASE_URL=http://localhost:8080 -e VUS=15 -e DURATION=40s \
		grafana/k6:0.49.0 run load/k6/load_test.js

slo: ## Проверка SLO из Prometheus (падает при нарушении)
	python scripts/slo_check.py --prometheus http://localhost:9090 --out slo_report.json

seed: ## Засеять немного данных вручную (warm-up)
	$(CI_COMPOSE) run --rm -e MODE=demo wms-api || true

ci-local: up-ci wait test-integration test-e2e load slo down-ci ## Прогнать весь CI локально на CI-стенде
