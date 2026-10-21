.PHONY: up down test test-py test-api integration e2e fixtures docs

-include .env
PG_URL = postgresql://dmean:dmean@localhost:$(or $(POSTGRES_HOST_PORT),5432)/dmean
RABBIT_ADMIN = http://dmean:dmean@localhost:$(or $(RABBITMQ_MGMT_HOST_PORT),15672)

up:            ## build and start the whole stack
	cp -n .env.example .env 2>/dev/null || true
	docker compose up -d --build

down:
	docker compose down

test: test-py test-api   ## unit tests, no infrastructure needed

test-py:
	cd py && pytest --ignore=tests/integration --ignore=tests/e2e

test-api:
	cd api && npm test

docs:          ## render docs/design.md to docs/DistributedMean_DesignDoc.pdf (needs markdown and Chrome)
	python docs/render.py

fixtures:      ## regenerate the cross-language fixtures
	cd api && npm run fixture

integration:   ## needs: docker compose up -d postgres && docker compose run --rm migrate
	cd py && DATABASE_URL=$(PG_URL) pytest tests/integration -v

e2e:           ## needs: make up
	cd py && E2E=1 E2E_DATABASE_URL=$(PG_URL) E2E_RABBIT_ADMIN=$(RABBIT_ADMIN) pytest tests/e2e -v
