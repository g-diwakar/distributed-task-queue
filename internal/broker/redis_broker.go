package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"g-diwakar/distributed-task-queue/internal/job"
)

// RedisBroker is a production broker backed by Redis lists.
// Enqueue uses LPUSH; Dequeue uses BRPOP across priority queues in order.
type RedisBroker struct {
	client      *redis.Client
	pollTimeout time.Duration
}

func NewRedisBroker(client *redis.Client) *RedisBroker {
	return &RedisBroker{
		client:      client,
		pollTimeout: 2 * time.Second,
	}
}

func (b *RedisBroker) Enqueue(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	return b.client.LPush(ctx, QueueForPriority(j.Priority), data).Err()
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
	// result[0] = queue name, result[1] = serialised job
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
