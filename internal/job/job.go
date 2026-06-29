package job

import (
	"time"
)

type Priority int

const (
	PriorityLow    Priority = 1
	PriorityMedium Priority = 2
	PriorityHigh   Priority = 3
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
	StatusDead      Status = "dead"
	StatusRetrying  Status = "retrying"
)

type JobType string

const (
	TypeSleepJob      JobType = "sleep_job"
	TypeFailJob       JobType = "fail_job"
	TypeHTTPFetch     JobType = "http_fetch"
	TypeDataTransform JobType = "data_transform"
	TypeImageResize   JobType = "image_resize"
	TypeSendEmail     JobType = "send_email"
)


type Job struct {
	ID       string   `json:"id"`
	Type     JobType  `json:"type"`
	Priority Priority `json:"priority"`
	Status   Status   `json:"status"`

	Payload     map[string]interface{} `json:"payload"`
	Attempts    int                    `json:"attempts"`
	MaxAttempts int                    `json:"max_attempts"`
	Error       string                 `json:"error,omitempty"`
	WorkerID    string                 `json:"worker_id,omitempty"`

	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	NextRetryAt *time.Time `json:"next_retry_at,omitempty"`
}
