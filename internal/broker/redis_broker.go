package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"g-diwakar/distributed-task-queue/internal/job"
	"g-diwakar/distributed-task-queue/internal/store"
)

// RedisBroker is a production broker backed by Redis lists.
// It holds a *store.RedisStore so Enqueue can persist the job and push
// it onto the queue in a single MULTI/EXEC transaction.
type RedisBroker struct {
	client      *redis.Client
	store       *store.RedisStore
	pollTimeout time.Duration
}

func NewRedisBroker(client *redis.Client, s *store.RedisStore) *RedisBroker {
	return &RedisBroker{
		client:      client,
		store:       s,
		pollTimeout: 2 * time.Second,
	}
}

// Enqueue persists the job via the store and pushes it onto the priority queue
// atomically — the store write and the LPUSH are in one MULTI/EXEC transaction.
func (b *RedisBroker) Enqueue(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	_, err = b.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		b.store.Pipelined(ctx, p, j, data)
		p.LPush(ctx, QueueForPriority(j.Priority), data)
		return nil
	})
	return err
}

// Dequeue blocks up to pollTimeout waiting for a job across all priority
// queues. Redis BRPOP checks each key in order, so high is always preferred.
// Returns (nil, nil) on timeout — the caller's loop should retry.
func (b *RedisBroker) Dequeue(ctx context.Context) (*job.Job, error) {
	queues := []string{QueueHigh, QueueNormal, QueueLow}
	result, err := b.client.BRPop(ctx, b.pollTimeout, queues...).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("brpop: %w", err)
	}
	var j job.Job
	if err := json.Unmarshal([]byte(result[1]), &j); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}
	return &j, nil
}

func (b *RedisBroker) MoveToDeadLetter(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	return b.client.LPush(ctx, QueueDead, data).Err()
}

func (b *RedisBroker) Len(ctx context.Context, queue string) (int64, error) {
	n, err := b.client.LLen(ctx, queue).Result()
	if err != nil {
		return 0, fmt.Errorf("llen %s: %w", queue, err)
	}
	return n, nil
}
