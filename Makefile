# Everyday commands. Python and the tools come from .venv, see README.
# A PostgreSQL on another port: make db test-all CRAWLER_POSTGRES_PORT=55432

PYTHON ?= .venv/bin/python

.PHONY: test test-all lint format db check

test:  # the default tests: unit and integration, no internet or database needed
	$(PYTHON) -m pytest -q

test-all:  # every test, the network, postgres and browser ones too
	$(PYTHON) -m pytest -q -m ""

lint:
	$(PYTHON) -m ruff check src tests
	$(PYTHON) -m ruff format --check src tests

format:
	$(PYTHON) -m ruff format src tests

db:  # start the PostgreSQL of docker-compose.yml and wait until it is ready
	docker compose up -d --wait

check: lint test-all  # what a change must pass before a commit
