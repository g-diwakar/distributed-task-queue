package broker

import (
	"context"
	"time"

	"g-diwakar/distributed-task-queue/internal/job"
	"g-diwakar/distributed-task-queue/internal/store"
)

// MemoryBroker is an in-process broker backed by buffered channels.
// It mirrors RedisBroker behaviour: Enqueue saves to the store and
// pushes to the queue together, so both brokers are used the same way.
type MemoryBroker struct {
	store  *store.MemoryStore
	high   chan *job.Job
	normal chan *job.Job
	low    chan *job.Job
	dead   chan *job.Job
}

func NewMemoryBroker(s *store.MemoryStore, bufSize int) *MemoryBroker {
	return &MemoryBroker{
		store:  s,
		high:   make(chan *job.Job, bufSize),
		normal: make(chan *job.Job, bufSize),
		low:    make(chan *job.Job, bufSize),
		dead:   make(chan *job.Job, bufSize),
	}
}

func (b *MemoryBroker) Enqueue(ctx context.Context, j *job.Job) error {
	if err := b.store.Save(ctx, j); err != nil {
		return err
	}

	var ch chan *job.Job
	switch j.Priority {
	case job.PriorityHigh:
		ch = b.high
	case job.PriorityMedium:
		ch = b.normal
	default:
		ch = b.low
	}

	select {
	case ch <- j:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Dequeue polls queues in strict priority order every 10ms until a job
// arrives or ctx is cancelled.
func (b *MemoryBroker) Dequeue(ctx context.Context) (*job.Job, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case j := <-b.high:
			return j, nil
		default:
		}
		select {
		case j := <-b.normal:
			return j, nil
		default:
		}
		select {
		case j := <-b.low:
			return j, nil
		default:
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *MemoryBroker) MoveToDeadLetter(_ context.Context, j *job.Job) error {
	b.dead <- j
	return nil
}

func (b *MemoryBroker) Len(_ context.Context, queue string) (int64, error) {
	switch queue {
	case QueueHigh:
		return int64(len(b.high)), nil
	case QueueNormal:
		return int64(len(b.normal)), nil
	case QueueLow:
		return int64(len(b.low)), nil
	case QueueDead:
		return int64(len(b.dead)), nil
	}
	return 0, nil
}
