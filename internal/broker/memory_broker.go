package broker

import (
	"context"
	"time"

	"g-diwakar/distributed-task-queue/internal/job"
)

// MemoryBroker is an in-process broker backed by buffered channels.
// Intended for testing — no persistence across restarts.
type MemoryBroker struct {
	high   chan *job.Job
	normal chan *job.Job
	low    chan *job.Job
	dead   chan *job.Job
}

func NewMemoryBroker(bufSize int) *MemoryBroker {
	return &MemoryBroker{
		high:   make(chan *job.Job, bufSize),
		normal: make(chan *job.Job, bufSize),
		low:    make(chan *job.Job, bufSize),
		dead:   make(chan *job.Job, bufSize),
	}
}

func (b *MemoryBroker) Enqueue(_ context.Context, j *job.Job) error {
	switch j.Priority {
	case job.PriorityHigh:
		b.high <- j
	case job.PriorityMedium:
		b.normal <- j
	default:
		b.low <- j
	}
	return nil
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