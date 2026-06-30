package worker

import (
	"context"
	"time"

	"go.uber.org/zap"

	"g-diwakar/distributed-task-queue/internal/broker"
	"g-diwakar/distributed-task-queue/internal/job"
	"g-diwakar/distributed-task-queue/internal/retry"
	"g-diwakar/distributed-task-queue/internal/store"
)

type Worker struct {
	id       string
	broker   broker.Broker
	store    store.Store
	registry *job.Registry
	policy   retry.Policy
	metrics  *Metrics
	log      *zap.Logger
}

func New(id string, b broker.Broker, s store.Store, r *job.Registry, p retry.Policy, m *Metrics, log *zap.Logger) *Worker {
	return &Worker{
		id:       id,
		broker:   b,
		store:    s,
		registry: r,
		policy:   p,
		metrics:  m,
		log:      log,
	}
}

// Run dequeues and executes one job at a time until ctx is cancelled.
// Concurrency is controlled by how many workers the pool spawns.
func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		j, err := w.broker.Dequeue(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.log.Error("dequeue failed", zap.String("worker_id", w.id), zap.Error(err))
			continue
		}
		if j == nil {
			continue // poll timeout — loop back immediately
		}

		w.execute(ctx, j)
	}
}

func (w *Worker) execute(ctx context.Context, j *job.Job) {
	start := time.Now()
	j.Status = job.StatusRunning
	j.StartedAt = &start
	j.WorkerID = w.id

	if err := w.store.Update(ctx, j); err != nil {
		w.log.Error("failed to mark job running", zap.String("job_id", j.ID), zap.Error(err))
		return
	}

	w.metrics.incActive()
	defer w.metrics.decActive()

	w.log.Info("job started",
		zap.String("worker_id", w.id),
		zap.String("job_id", j.ID),
		zap.String("type", string(j.Type)),
	)

	execErr := w.registry.Dispatch(ctx, j)

	fin := time.Now()
	j.FinishedAt = &fin

	if execErr != nil {
		w.handleFailure(ctx, j, execErr)
		return
	}

	j.Status = job.StatusCompleted
	w.metrics.incCompleted()
	w.log.Info("job completed",
		zap.String("job_id", j.ID),
		zap.Duration("duration", fin.Sub(start)),
	)

	if err := w.store.Update(ctx, j); err != nil {
		w.log.Error("failed to mark job completed", zap.String("job_id", j.ID), zap.Error(err))
	}
}

func (w *Worker) handleFailure(ctx context.Context, j *job.Job, execErr error) {
	j.Attempts++
	j.Error = execErr.Error()

	if j.MaxAttempts > 0 && j.Attempts < j.MaxAttempts {
		delay := w.policy.NextDelay(j.Attempts)
		nextRetry := time.Now().Add(delay)
		j.Status = job.StatusRetrying
		j.NextRetryAt = &nextRetry

		w.log.Warn("job failed, will retry",
			zap.String("job_id", j.ID),
			zap.Int("attempt", j.Attempts),
			zap.Int("max_attempts", j.MaxAttempts),
			zap.Duration("retry_in", delay),
			zap.Error(execErr),
		)

		_ = w.store.Update(ctx, j)

		// Backoff wait runs in the background so this worker can
		// immediately pick up the next job from the queue.
		go func() {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
			j.Status = job.StatusPending
			j.NextRetryAt = nil
			_ = w.store.Update(ctx, j)
			_ = w.broker.Enqueue(ctx, j)
		}()
		return
	}

	j.Status = job.StatusDead
	w.metrics.incFailed()
	w.log.Error("job exhausted retries, moving to DLQ",
		zap.String("job_id", j.ID),
		zap.Int("attempts", j.Attempts),
		zap.Error(execErr),
	)
	_ = w.store.Update(ctx, j)
	_ = w.broker.MoveToDeadLetter(ctx, j)
}
