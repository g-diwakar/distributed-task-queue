package handlers

import (
	"context"
	"fmt"
	"net/http"

	"g-diwakar/distributed-task-queue/internal/job"
)

func HTTPFetch(ctx context.Context, j *job.Job) error {
	url, ok := j.Payload["url"].(string)
	if !ok || url == "" {
		return fmt.Errorf("missing or empty url in payload")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", url, err)
	}
	defer resp.Body.Close()

	j.Payload["response_status"] = resp.StatusCode
	j.Payload["response_url"] = resp.Request.URL.String()
	return nil
}
