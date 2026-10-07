# Everyday commands. Python and the tools come from .venv, see README.
# A PostgreSQL on another port: make db test-all CRAWLER_POSTGRES_PORT=55432

PYTHON ?= .venv/bin/python

.PHONY: install test test-all test-docker lint format db docker-build check

install:  # the crawler, the tools of development and the browser that renders JavaScript
	$(PYTHON) -m pip install -e . -r requirements-dev.txt
	$(PYTHON) -m playwright install chromium

test:  # the default tests: unit and integration, no internet or database needed
	$(PYTHON) -m pytest -q

test-all:  # every test but the docker ones: the network, postgres and browser ones too
	$(PYTHON) -m pytest -q -m "not docker"

test-docker:  # build the images of the Dockerfile and crawl the test site in containers
	$(PYTHON) -m pytest -q -m docker

lint:
	$(PYTHON) -m ruff check src tests
	$(PYTHON) -m ruff format --check src tests

format:
	$(PYTHON) -m ruff format src tests

db:  # start the PostgreSQL of docker-compose.yml and wait until it is ready
	docker compose up -d --wait

docker-build:  # the image of the crawler, and the one with Chromium
	docker build -t async-web-crawler .
	docker build --target js -t async-web-crawler:js .

check: lint test-all  # what a change must pass before a commit
