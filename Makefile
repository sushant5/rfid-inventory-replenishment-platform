COMPOSE := docker compose -f docker-compose.yml

.PHONY: up migrate seed test demo down logs

up:
	$(COMPOSE) up --build -d

migrate:
	$(COMPOSE) run --build --rm migrate

seed:
	$(COMPOSE) run --rm api abacus-cli bootstrap-admin

test:
	$(COMPOSE) --profile test run --build --rm test

demo:
	$(COMPOSE) up --build -d --wait
	$(COMPOSE) run --rm api abacus-cli bootstrap-admin
	$(COMPOSE) run --rm api python scripts/run_architecture_demo.py --base-url http://api:8000

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs --tail=200 api catalog-worker event-worker
