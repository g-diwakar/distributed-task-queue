package handlers

import (
	"context"
	"fmt"
	"time"

	"g-diwakar/distributed-task-queue/internal/job"
)

// ImageResize simulates a CPU-bound image resize operation.
// Payload: width (float64), height (float64), source (string, optional).
func ImageResize(ctx context.Context, j *job.Job) error {
	width, wOK := j.Payload["width"].(float64)
	height, hOK := j.Payload["height"].(float64)
	if !wOK || !hOK || width <= 0 || height <= 0 {
		return fmt.Errorf("payload must contain positive 'width' and 'height'")
	}

	// Simulate CPU-bound work proportional to output size.
	work := time.Duration(width*height/10000) * time.Millisecond
	if work < 50*time.Millisecond {
		work = 50 * time.Millisecond
	}

	select {
	case <-time.After(work):
	case <-ctx.Done():
		return ctx.Err()
	}

	j.Payload["resized_dimensions"] = fmt.Sprintf("%.0fx%.0f", width, height)
	return nil
}
