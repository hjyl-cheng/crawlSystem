SHELL := /bin/bash
ENVIRONMENT ?= local
COMPOSE := ./scripts/compose.sh $(ENVIRONMENT)

.PHONY: bootstrap setup config build up down ps logs verify test

bootstrap:
	./scripts/bootstrap.sh $(ENVIRONMENT)

setup:
	./scripts/setup-dev.sh

config:
	$(COMPOSE) config --quiet

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=200

verify:
	./scripts/verify.sh

test:
	./scripts/test.sh
