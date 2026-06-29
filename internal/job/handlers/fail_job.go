package handlers

import (
	"context"
	"fmt"

	"g-diwakar/distributed-task-queue/internal/job"
)

func FailJob(_ context.Context, j *job.Job) error {
	return fmt.Errorf("intentional failure on attempt %d", j.Attempts)
}
