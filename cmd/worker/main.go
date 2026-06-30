package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"g-diwakar/distributed-task-queue/config"
	"g-diwakar/distributed-task-queue/internal/broker"
	"g-diwakar/distributed-task-queue/internal/job/handlers"
	"g-diwakar/distributed-task-queue/internal/retry"
	"g-diwakar/distributed-task-queue/internal/store"
	"g-diwakar/distributed-task-queue/internal/worker"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("init logger: %v", err)
	}
	defer logger.Sync()

	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	jobBroker, jobStore := buildBackend(ctx, cfg, logger)

	pool := worker.NewPool(worker.Config{
		PoolID:   cfg.Worker.PoolID,
		Workers:  cfg.Worker.Workers,
		Broker:   jobBroker,
		Store:    jobStore,
		Registry: handlers.DefaultRegistry(),
		Policy:   retry.NewExponential(cfg.Worker.RetryBase, cfg.Worker.RetryMax),
		Log:      logger,
	})

	logger.Info("worker pool starting",
		zap.String("pool_id", pool.ID()),
		zap.Int("workers", cfg.Worker.Workers),
	)

	pool.Start(ctx)
	<-ctx.Done()

	logger.Info("draining workers...")
	pool.Wait()
	logger.Info("shutdown complete")
}

func buildBackend(ctx context.Context, cfg config.Config, logger *zap.Logger) (broker.Broker, store.Store) {
	if cfg.Redis.Enabled() {
		rdb := goredis.NewClient(&goredis.Options{
			Addr:     cfg.Redis.Addr,
			Password: cfg.Redis.Password,
			DB:       cfg.Redis.DB,
		})
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Fatal("redis unreachable", zap.String("addr", cfg.Redis.Addr), zap.Error(err))
		}
		logger.Info("using Redis backend", zap.String("addr", cfg.Redis.Addr))
		rs := store.NewRedisStore(rdb)
		return broker.NewRedisBroker(rdb, rs), rs
	}

	logger.Warn("REDIS_ADDR not set — using in-memory backend (state is lost on restart)")
	ms := store.NewMemoryStore()
	return broker.NewMemoryBroker(ms, 256), ms
}
