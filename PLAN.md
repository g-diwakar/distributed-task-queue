# Distributed Task Queue in Go — Project Plan

> **Goal:** Build a production-inspired distributed task queue (mini Celery/Bull) in Go that demonstrates concurrency patterns, fault tolerance, distributed systems thinking, and clean API design.

---


### 1. What jobs will it execute?

For a portfolio project, jobs should be **realistic but self-contained** — no external dependencies that break demos.

| Job Type | What it does | Why include it |
|---|---|---|
| `image_resize` | Resize a dummy image (or use a real lib) | CPU-bound, shows worker throttling |
| `send_email` | Simulate sending email (log + delay) | I/O-bound, common real-world use case |
| `http_fetch` | Fetch a URL and store response size | Network I/O, can actually run |
| `data_transform` | Parse JSON, transform fields, output result | Pure compute |
| `sleep_job` | Sleep for N seconds | Perfect for testing timeouts, retries |
| `fail_job` | Always fails after N attempts | Tests dead-letter queue logic |

These 6 types cover every category of real-world jobs and let you demo every feature of the system.

---

### 2. What are the workers — machines, nodes, or processes?

**For this project: multiple goroutines within a single process, but architected as if they were separate machines.**

```
What it ACTUALLY is:          What it's DESIGNED to support:
─────────────────────         ──────────────────────────────
1 binary running locally  →   N worker binaries on N machines
goroutines as workers     →   goroutines simulate distributed workers
Redis as the broker       →   Redis IS production-grade (it scales real)
```

The key insight: **Redis makes it genuinely distributed.** If you run 3 copies of your worker binary on 3 different machines pointing at the same Redis — it just works. That's the portfolio story. You build it as a single binary for the demo, but the architecture is real.

---

## Architecture Blueprint

```
┌─────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                         │
│         CLI tool  /  HTTP REST API  /  gRPC API             │
└────────────────────────┬────────────────────────────────────┘
                         │ Submit Job / Query Status
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                      BROKER LAYER                           │
│                                                             │
│   ┌─────────────┐  ┌─────────────┐  ┌──────────────────┐  │
│   │ High Queue  │  │ Normal Queue│  │   Low Queue      │  │
│   │ (Priority 1)│  │ (Priority 2)│  │   (Priority 3)   │  │
│   └─────────────┘  └─────────────┘  └──────────────────┘  │
│                                                             │
│   ┌─────────────────────────────────────────────────────┐  │
│   │              Dead Letter Queue (DLQ)                │  │
│   └─────────────────────────────────────────────────────┘  │
│                                                             │
│   ┌─────────────────────────────────────────────────────┐  │
│   │         Job State Store (Redis Hash/JSON)           │  │
│   └─────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────┘
                         │ BLPOP (blocking dequeue)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                      WORKER LAYER                           │
│                                                             │
│   WorkerPool                                                │
│   ├── Worker #1 (goroutine) → executes job → reports done  │
│   ├── Worker #2 (goroutine) → executes job → reports done  │
│   ├── Worker #3 (goroutine) → idle, waiting                │
│   └── Worker #N ...                                        │
│                                                             │
│   WorkerPool Manager                                        │
│   ├── Health checks each worker                            │
│   ├── Restarts crashed workers                             │
│   └── Exposes metrics (active, idle, failed counts)        │
└────────────────────────┬────────────────────────────────────┘
                         │ Publishes events
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    OBSERVABILITY LAYER                      │
│   Redis Pub/Sub → Event Bus → TUI Dashboard (bubbletea)    │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
distributed-task-queue/
│
├── cmd/
│   ├── server/         # gRPC + HTTP API server
│   │   └── main.go
│   ├── worker/         # Worker binary (run multiple instances)
│   │   └── main.go
│   └── dashboard/      # bubbletea TUI
│       └── main.go
│
├── internal/
│   ├── broker/
│   │   ├── broker.go           # Broker interface
│   │   ├── redis_broker.go     # Redis implementation
│   │   └── memory_broker.go    # In-memory (for testing)
│   │
│   ├── job/
│   │   ├── job.go              # Job struct, Status enum, Priority
│   │   ├── registry.go         # Maps job type → handler func
│   │   └── handlers/
│   │       ├── image_resize.go
│   │       ├── http_fetch.go
│   │       ├── data_transform.go
│   │       └── sleep_job.go
│   │
│   ├── worker/
│   │   ├── worker.go           # Single worker logic
│   │   ├── pool.go             # WorkerPool: spawn, supervise, drain
│   │   └── metrics.go          # Per-worker stats
│   │
│   ├── retry/
│   │   ├── policy.go           # RetryPolicy interface
│   │   └── exponential.go      # Exponential backoff implementation
│   │
│   ├── store/
│   │   ├── store.go            # JobStore interface
│   │   └── redis_store.go      # CRUD for job state in Redis
│   │
│   ├── api/
│   │   ├── http/
│   │   │   ├── server.go
│   │   │   └── handlers.go     # Submit, status, list, cancel
│   │   └── grpc/
│   │       ├── server.go
│   │       └── task.proto
│   │
│   └── events/
│       ├── bus.go              # Event types: JobQueued, Started, Done, Failed
│       └── redis_pubsub.go     # Publish/subscribe via Redis
│
├── dashboard/
│   ├── model.go                # bubbletea model
│   ├── view.go                 # Render TUI
│   └── update.go               # Handle events, key bindings
│
├── config/
│   └── config.go               # Viper-based config (YAML + env vars)
│
├── docker-compose.yml           # Redis + server + 3 workers
├── Makefile
└── README.md
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go 1.22+ |
| Broker / Store | Redis (`go-redis/v9`) |
| API | `net/http` + `chi` router, gRPC + protobuf |
| TUI Dashboard | `charmbracelet/bubbletea` |
| Config | `spf13/viper` |
| Logging | `uber-go/zap` |
| Metrics | Prometheus (`prometheus/client_golang`) |
| Testing | `testify`, `miniredis` (in-memory Redis for tests) |
| Dev tooling | `docker-compose`, `Makefile` |

---