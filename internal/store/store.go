package store

import (
	"context"
	"errors"

	"g-diwakar/distributed-task-queue/internal/job"
)

var ErrNotFound = errors.New("job not found")

// Filter controls which jobs List returns. Zero values mean "no filter".
type Filter struct {
	Status job.Status  // empty = all statuses
	Type   job.JobType // empty = all types
	Limit  int         // 0 = no limit
}

type Store interface {
	// Save persists a new job and adds it to the relevant indexes.
	Save(ctx context.Context, j *job.Job) error

	// Get fetches a job by ID. Returns ErrNotFound if it doesn't exist.
	Get(ctx context.Context, id string) (*job.Job, error)

	// Update overwrites a job and maintains status indexes on transition.
	Update(ctx context.Context, j *job.Job) error

	// List returns jobs matching the filter.
	List(ctx context.Context, f Filter) ([]*job.Job, error)

	// Delete removes a job and cleans up its index entries.
	Delete(ctx context.Context, id string) error
}
