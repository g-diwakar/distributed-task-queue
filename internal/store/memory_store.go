package store

import (
	"context"
	"sync"

	"g-diwakar/distributed-task-queue/internal/job"
)

// MemoryStore is an in-process store backed by a map.
// Intended for testing — no persistence across restarts.
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]*job.Job
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: make(map[string]*job.Job)}
}

func (s *MemoryStore) Save(_ context.Context, j *job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *j
	s.jobs[j.ID] = &cp
	return nil
}

func (s *MemoryStore) Get(_ context.Context, id string) (*job.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *j
	return &cp, nil
}

func (s *MemoryStore) Update(_ context.Context, j *job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[j.ID]; !ok {
		return ErrNotFound
	}
	cp := *j
	s.jobs[j.ID] = &cp
	return nil
}

func (s *MemoryStore) List(_ context.Context, f Filter) ([]*job.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := make([]*job.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		if f.Status != "" && j.Status != f.Status {
			continue
		}
		if f.Type != "" && j.Type != f.Type {
			continue
		}
		cp := *j
		jobs = append(jobs, &cp)
		if f.Limit > 0 && len(jobs) >= f.Limit {
			break
		}
	}
	return jobs, nil
}

func (s *MemoryStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[id]; !ok {
		return ErrNotFound
	}
	delete(s.jobs, id)
	return nil
}
