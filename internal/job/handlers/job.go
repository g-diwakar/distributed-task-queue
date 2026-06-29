package handlers

import "g-diwakar/distributed-task-queue/internal/job"

// DefaultRegistry returns a registry pre-loaded with all built-in job handlers.
func DefaultRegistry() *job.Registry {
	r := job.NewRegistry()
	r.Register(job.TypeSleepJob, job.HandlerFunc(SleepJob))
	r.Register(job.TypeFailJob, job.HandlerFunc(FailJob))
	r.Register(job.TypeHTTPFetch, job.HandlerFunc(HTTPFetch))
	r.Register(job.TypeDataTransform, job.HandlerFunc(DataTransform))
	r.Register(job.TypeImageResize, job.HandlerFunc(ImageResize))
	r.Register(job.TypeSendEmail, job.HandlerFunc(SendEmail))
	return r
}
