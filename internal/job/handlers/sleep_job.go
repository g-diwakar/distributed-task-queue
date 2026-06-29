package handlers

import (
	"context"
	"time"

	"g-diwakar/distributed-task-queue/internal/job"
)

func SleepJob(ctx context.Context, j *job.Job) error {
	duration := 5 * time.Second
	if d, ok := j.Payload["duration_seconds"].(float64); ok {
		duration = time.Duration(d) * time.Second
	}
	select {
	case <-time.After(duration):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
