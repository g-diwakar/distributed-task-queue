package handlers

import (
	"context"
	"fmt"
	"strings"

	"g-diwakar/distributed-task-queue/internal/job"
)

// DataTransform uppercases all keys and trims whitespace from string values.
func DataTransform(_ context.Context, j *job.Job) error {
	input, ok := j.Payload["input"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("payload must contain an 'input' object")
	}

	output := make(map[string]interface{}, len(input))
	for k, v := range input {
		key := strings.ToUpper(k)
		if s, ok := v.(string); ok {
			output[key] = strings.TrimSpace(s)
		} else {
			output[key] = v
		}
	}

	j.Payload["output"] = output
	return nil
}
