package worker

import (
	"context"
	"fmt"
	"os"
	"sync"

	"go.uber.org/zap"

	"g-diwakar/distributed-task-queue/internal/broker"
	"g-diwakar/distributed-task-queue/internal/job"
	"g-diwakar/distributed-task-queue/internal/retry"
	"g-diwakar/distributed-task-queue/internal/store"
)

// Config configures a single pool instance (node).
// Deploy multiple pool binaries across servers for horizontal scale —
// each points at the same Redis and they coordinate automatically.
type Config struct {
	// PoolID uniquely identifies this node across the cluster.
	// Defaults to "<hostname>-<pid>" if empty.
	PoolID  string
	Workers int // goroutines on this node; number of workers = concurrency per node
	Broker  broker.Broker
	Store   store.Store
	Registry *job.Registry
	Policy  retry.Policy
	Log     *zap.Logger
}

type Pool struct {
	id      string
	workers []*Worker
	wg      sync.WaitGroup
	metrics *Metrics
}

func NewPool(cfg Config) *Pool {
	if cfg.PoolID == "" {
		cfg.PoolID = defaultPoolID()
	}

	m := &Metrics{}
	workers := make([]*Worker, cfg.Workers)
	for i := range workers {
		// e.g. "web-01-19283-worker-1" — unique across every node and process
		id := fmt.Sprintf("%s-worker-%d", cfg.PoolID, i+1)
		workers[i] = New(
			id,
			cfg.Broker,
			cfg.Store,
			cfg.Registry,
			cfg.Policy,
			m,
			cfg.Log.With(zap.String("worker_id", id), zap.String("pool_id", cfg.PoolID)),
		)
	}
	return &Pool{id: cfg.PoolID, workers: workers, metrics: m}
}

// defaultPoolID builds a stable node identity from hostname and PID.
// hostname identifies the server; PID distinguishes multiple pools on the same host.
func defaultPoolID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

// Start spawns all worker goroutines. They run until ctx is cancelled.
func (p *Pool) Start(ctx context.Context) {
	for _, w := range p.workers {
		p.wg.Add(1)
		w := w
		go func() {
			defer p.wg.Done()
			w.Run(ctx)
		}()
	}
}

// Wait blocks until every worker has exited cleanly.
func (p *Pool) Wait() {
	p.wg.Wait()
}

func (p *Pool) ID() string      { return p.id }
func (p *Pool) Metrics() *Metrics { return p.metrics }
