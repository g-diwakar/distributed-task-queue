BIN := bin
N   ?= 2

.PHONY: help dev server worker dashboard submit build up down logs scale clean tidy

help:
	@echo "Local dev"
	@echo "  make dev          run server + worker in one process (no Redis)"
	@echo "  make server       run HTTP server only"
	@echo "  make worker       run worker pool only"
	@echo "  make dashboard    run TUI dashboard"
	@echo "  make submit       submit sample jobs via scripts/submit.py"
	@echo ""
	@echo "Build"
	@echo "  make build        compile all binaries to ./bin"
	@echo "  make tidy         go mod tidy"
	@echo "  make clean        remove ./bin"
	@echo ""
	@echo "Docker"
	@echo "  make up           docker compose up (Redis + server + worker)"
	@echo "  make down         docker compose down"
	@echo "  make logs         follow compose logs"
	@echo "  make scale N=3    scale worker replicas (default 2)"

# ── Local ─────────────────────────────────────────────────────────────────────

dev:
	go run ./cmd/dev

server:
	go run ./cmd/server

worker:
	go run ./cmd/worker

dashboard:
	go run ./cmd/dashboard

submit:
	python3 scripts/submit.py

# ── Build ─────────────────────────────────────────────────────────────────────

build:
	@mkdir -p $(BIN)
	go build -o $(BIN)/server    ./cmd/server
	go build -o $(BIN)/worker    ./cmd/worker
	go build -o $(BIN)/dev       ./cmd/dev
	go build -o $(BIN)/dashboard ./cmd/dashboard

tidy:
	go mod tidy

clean:
	rm -rf $(BIN)

# ── Docker ────────────────────────────────────────────────────────────────────

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f

scale:
	docker compose up -d --scale worker=$(N)
