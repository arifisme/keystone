GO ?= go
ENDPOINTS ?= 127.0.0.1:7101,127.0.0.1:7105,127.0.0.1:7109

.PHONY: build test sim bench chart docker up down chaos

build:
	$(GO) build ./...

test:
	$(GO) test -race ./...

sim:
	$(GO) test ./sim -seeds=10000 -chaos=3

bench:
	$(GO) run ./bench all -out bench/results

chart:
	$(GO) run ./bench chart -in bench/results -out bench/results

docker:
	docker build -t keystone .

up:
	docker compose up -d --build

down:
	docker compose down -v

# Partition one node, then add latency and loss to another, while writes
# keep flowing. Needs the compose cluster up and keystonectl built.
chaos: build
	ENDPOINTS=$(ENDPOINTS) ./hack/chaos.sh
