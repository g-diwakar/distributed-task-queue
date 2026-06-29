package job

import (
	"context"
	"fmt"
)

type HandlerFunc func(ctx context.Context, job *Job) error

type Registry struct {
	handlers map[JobType]HandlerFunc
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[JobType]HandlerFunc)}
}

func (r *Registry) Register(jobType JobType, h HandlerFunc) {
	r.handlers[jobType] = h
}

func (r *Registry) Get(jobType JobType) (HandlerFunc, bool) {
	h, ok := r.handlers[jobType]
	return h, ok
}

// Dispatch looks up and runs the handler for the given job's type.
func (r *Registry) Dispatch(ctx context.Context, j *Job) error {
	h, ok := r.handlers[j.Type]
	if !ok {
		return fmt.Errorf("no handler registered for job type %q", j.Type)
	}
	return h(ctx, j)
}
