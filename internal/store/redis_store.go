package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"g-diwakar/distributed-task-queue/internal/job"
)

// Redis key scheme:
//   dtq:job:{id}              → JSON blob for a single job
//   dtq:jobs:all              → Set of all job IDs
//   dtq:jobs:status:{status}  → Set of job IDs in that status

const (
	jobKeyPrefix   = "dtq:job:"
	indexAll       = "dtq:jobs:all"
	indexStatusFmt = "dtq:jobs:status:%s"
)

func jobKey(id string) string       { return jobKeyPrefix + id }
func statusKey(s job.Status) string { return fmt.Sprintf(indexStatusFmt, s) }

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client}
}

func (s *RedisStore) Save(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	_, err = s.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, jobKey(j.ID), data, 0)
		p.SAdd(ctx, indexAll, j.ID)
		p.SAdd(ctx, statusKey(j.Status), j.ID)
		return nil
	})
	return err
}

func (s *RedisStore) Get(ctx context.Context, id string) (*job.Job, error) {
	data, err := s.client.Get(ctx, jobKey(id)).Bytes()
	if err == redis.Nil {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", id, err)
	}
	var j job.Job
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &j, nil
}

// Update overwrites the job JSON and fixes the status index if the status changed.
func (s *RedisStore) Update(ctx context.Context, j *job.Job) error {
	old, err := s.Get(ctx, j.ID)
	if err != nil {
		return err
	}
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	_, err = s.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, jobKey(j.ID), data, 0)
		if old.Status != j.Status {
			p.SRem(ctx, statusKey(old.Status), j.ID)
			p.SAdd(ctx, statusKey(j.Status), j.ID)
		}
		return nil
	})
	return err
}

func (s *RedisStore) List(ctx context.Context, f Filter) ([]*job.Job, error) {
	indexKey := indexAll
	if f.Status != "" {
		indexKey = statusKey(f.Status)
	}

	ids, err := s.client.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("smembers %s: %w", indexKey, err)
	}

	jobs := make([]*job.Job, 0, len(ids))
	for _, id := range ids {
		j, err := s.Get(ctx, id)
		if err != nil {
			continue // stale index entry — skip
		}
		if f.Type != "" && j.Type != f.Type {
			continue
		}
		jobs = append(jobs, j)
		if f.Limit > 0 && len(jobs) >= f.Limit {
			break
		}
	}
	return jobs, nil
}

func (s *RedisStore) Delete(ctx context.Context, id string) error {
	j, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	_, err = s.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, jobKey(id))
		p.SRem(ctx, indexAll, id)
		p.SRem(ctx, statusKey(j.Status), id)
		return nil
	})
	return err
}
