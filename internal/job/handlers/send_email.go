package handlers

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"g-diwakar/distributed-task-queue/internal/job"
)

// SendEmail simulates an I/O-bound email send (logs + artificial delay).
// Payload: to (string), subject (string), body (string, optional).
func SendEmail(ctx context.Context, j *job.Job) error {
	to, ok := j.Payload["to"].(string)
	if !ok || to == "" {
		return fmt.Errorf("payload must contain a 'to' address")
	}
	subject, _ := j.Payload["subject"].(string)

	select {
	case <-time.After(300 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	zap.L().Info("email sent",
		zap.String("job_id", j.ID),
		zap.String("to", to),
		zap.String("subject", subject),
	)
	return nil
}
