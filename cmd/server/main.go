package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"g-diwakar/distributed-task-queue/config"
	"g-diwakar/distributed-task-queue/internal/api"
	"g-diwakar/distributed-task-queue/internal/broker"
	"g-diwakar/distributed-task-queue/internal/store"
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

	srv := api.NewServer(cfg.Server.Addr, jobBroker, jobStore, logger)

	go func() {
		if err := srv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", zap.Error(err))
	}
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
