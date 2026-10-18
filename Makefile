.PHONY: up down test test-py test-api integration e2e fixtures

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

fixtures:      ## regenerate the cross-language fixtures
	cd api && npm run fixture

integration:   ## needs: docker compose up -d postgres && docker compose run --rm migrate
	cd py && DATABASE_URL=postgresql://dmean:dmean@localhost:5432/dmean pytest tests/integration -v

e2e:           ## needs: make up
	cd py && E2E=1 pytest tests/e2e -v
