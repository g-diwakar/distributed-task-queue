package broker

import (
	"context"

	"g-diwakar/distributed-task-queue/internal/job"
)

const (
	QueueHigh   = "dtq:queue:high"
	QueueNormal = "dtq:queue:normal"
	QueueLow    = "dtq:queue:low"
	QueueDead   = "dtq:queue:dead"
)

// Broker is the queue backend used to enqueue and dequeue jobs.
type Broker interface {
	// Enqueue pushes a job onto the queue matching its priority.
	Enqueue(ctx context.Context, j *job.Job) error

	// Dequeue blocks until a job is available or ctx is cancelled.
	// Returns (nil, nil) on a polling timeout — callers should retry.
	Dequeue(ctx context.Context) (*job.Job, error)

	// MoveToDeadLetter moves a job that has exhausted retries to the DLQ.
	MoveToDeadLetter(ctx context.Context, j *job.Job) error

	// Len returns the number of jobs currently in the named queue.
	Len(ctx context.Context, queue string) (int64, error)
}

// QueueForPriority maps a job priority to its queue name.
func QueueForPriority(p job.Priority) string {
	switch p {
	case job.PriorityHigh:
		return QueueHigh
	case job.PriorityMedium:
		return QueueNormal
	default:
		return QueueLow
	}
}
