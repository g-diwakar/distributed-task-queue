# Distributed Task Queue

A production-inspired background job system written in Go. Jobs are submitted over HTTP, processed by a pool of workers, retried with exponential backoff on failure, and routed to a dead-letter queue after exhausting attempts. Supports both an in-memory backend for local development and Redis for distributed deployment.

## Architecture

```
                        ┌─────────────────────────────────────────────┐
                        │               HTTP API (:8080)               │
                        │  POST /jobs  GET /jobs  GET /jobs/:id        │
                        │             DELETE /jobs/:id                 │
                        └──────────────────┬──────────────────────────┘
                                           │  Enqueue (atomic)
                                           ▼
                        ┌─────────────────────────────────────────────┐
                        │                  Broker                      │
                        │   Priority queues: high → normal → low       │
                        │   Backend: Redis (BRPOP) │ Memory (channels) │
                        └──────────────────┬──────────────────────────┘
                                           │  Dequeue
                          ┌────────────────┼────────────────┐
                          ▼                ▼                 ▼
                     ┌─────────┐     ┌─────────┐      ┌─────────┐
                     │Worker 1 │     │Worker 2 │      │Worker N │  ← one Pool per node
                     └────┬────┘     └────┬────┘      └────┬────┘
                          │               │                 │
                          └───────────────┴─────────────────┘
                                          │
                          ┌───────────────┴───────────────┐
                          │                               │
                          ▼                               ▼
                   ┌─────────────┐               ┌──────────────┐
                   │    Store    │               │  Dead-Letter │
                   │(Redis/Mem)  │               │    Queue     │
                   └─────────────┘               └──────────────┘
                          ▲
                          │ polls /jobs HTTP API
                   ┌─────────────┐
                   │  Dashboard  │
                   │    (TUI)    │
                   └─────────────┘
```

.
├── cmd/
│   ├── dev/          # all-in-one binary (server + worker, shared memory)
│   ├── server/       # HTTP API server
│   ├── worker/       # worker pool
│   └── dashboard/    # TUI dashboard
├── config/           # env-var configuration
├── dashboard/        # bubbletea Model / Update / View
├── internal/
│   ├── api/          # chi router + handlers
│   ├── broker/       # Broker interface, Redis + memory implementations
│   ├── job/
│   │   ├── job.go        # Job struct, types, priorities, statuses
│   │   ├── validate.go   # payload specs per job type
│   │   ├── registry.go   # HandlerFunc registry
│   │   └── handlers/     # one file per job type
│   ├── retry/        # Policy interface + exponential backoff
│   ├── store/        # Store interface, Redis + memory implementations
│   └── worker/       # Worker, Pool, Metrics
├── scripts/
│   └── submit.py     # sample job submission script
├── Dockerfile
├── docker-compose.yml
└── Makefile
```

## Job types

| Type | Required payload fields | Description |
|---|---|---|
| `sleep_job` | — | Sleeps for `duration_seconds` (default 3) |
| `http_fetch` | `url` | HTTP GET, records `response_status` |
| `data_transform` | `input` (object) | Uppercases keys, trims string values |
| `image_resize` | `width`, `height` | Simulates CPU work proportional to pixel count |
| `send_email` | `to` | Simulates 300 ms I/O, logs the send |
| `fail_job` | — | Always fails — use to test retry and dead-letter queue |

## Setup

### Option A — Local dev (no dependencies)

Everything runs in a single process sharing one in-memory broker and store.

```bash
make dev
```

### Option B — Distributed with Redis

```bash
make up                   # starts Redis + server + worker via Docker Compose
make logs                 # follow logs
make scale N=3            # scale to 3 worker containers
make down                 # tear down
```

Or run each component separately against a local Redis:

```bash
docker run -d -p 6379:6379 redis:alpine

REDIS_ADDR=localhost:6379 make server
REDIS_ADDR=localhost:6379 make worker
REDIS_ADDR=localhost:6379 make dashboard
```

## Usage

### Submit jobs

```bash
# Submit all sample job types and poll until done
make submit

# Or with curl directly
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type":"sleep_job","priority":2,"payload":{"duration_seconds":5},"max_attempts":3}'
```

Submit script flags:

```bash
python3 scripts/submit.py                          # submit all + poll status
python3 scripts/submit.py --no-poll               # submit and exit
python3 scripts/submit.py --type http_fetch --priority high
python3 scripts/submit.py --list                  # list all jobs
```

### HTTP API

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Submit a job |
| `GET` | `/jobs` | List jobs (filter: `?status=`, `?type=`, `?limit=`) |
| `GET` | `/jobs/:id` | Get a single job |
| `DELETE` | `/jobs/:id` | Cancel a pending or running job |

### TUI Dashboard

```bash
make dashboard
```

Key bindings: `↑`/`↓` or `j`/`k` to navigate, `f` to cycle status filter, `r` to refresh, `?` for help, `q` to quit.

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
|---|---|---|
| `REDIS_ADDR` | _(empty)_ | Redis address e.g. `localhost:6379`. Leave empty to use in-memory backend |
| `REDIS_PASSWORD` | _(empty)_ | Redis password |
| `REDIS_DB` | `0` | Redis database number |
| `HTTP_ADDR` | `:8080` | Address the HTTP server listens on |
| `WORKER_COUNT` | `5` | Number of concurrent workers per pool |
| `POOL_ID` | `hostname-pid` | Unique identifier for this worker pool node |
| `RETRY_BASE_SECONDS` | `1` | Base delay for exponential backoff |
| `RETRY_MAX_SECONDS` | `30` | Maximum backoff delay |
| `API_URL` | `http://localhost:8080` | API URL used by the dashboard |

## Backends

### In-memory

Zero dependencies — suitable for local development and single-node deployments. State is lost on restart. Server and workers must run in the same process (`cmd/dev`).

### Redis

Production backend. Provides durability, multi-node worker pools, and priority queuing via `BRPOP`. Each job write is atomic (`MULTI/EXEC`). Key scheme:

```
dtq:job:{id}           — job JSON blob
dtq:jobs:all           — set of all job IDs
dtq:jobs:status:{s}    — set of job IDs per status
dtq:queue:high         — priority queue (list)
dtq:queue:normal
dtq:queue:low
dtq:queue:dead
```

## Make targets

```
make dev          run server + worker in one process (no Redis)
make server       run HTTP server only
make worker       run worker pool only
make dashboard    run TUI dashboard
make submit       submit sample jobs

make build        compile all binaries to ./bin
make tidy         go mod tidy
make clean        remove ./bin

make up           docker compose up (Redis + server + worker)
make down         docker compose down
make logs         follow compose logs
make scale N=3    scale worker replicas
```
