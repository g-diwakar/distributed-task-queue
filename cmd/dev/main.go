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

	"go.uber.org/zap"

	"g-diwakar/distributed-task-queue/config"
	"g-diwakar/distributed-task-queue/internal/api"
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

	// Single shared in-memory backend — server and workers use the same instance.
	ms := store.NewMemoryStore()
	mb := broker.NewMemoryBroker(ms, 256)

	logger.Info("dev mode: in-memory backend, server + worker in one process",
		zap.String("addr", cfg.Server.Addr),
		zap.Int("workers", cfg.Worker.Workers),
	)

	// HTTP server
	srv := api.NewServer(cfg.Server.Addr, mb, ms, logger)
	go func() {
		if err := srv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", zap.Error(err))
		}
	}()

	// Worker pool sharing the same broker and store
	pool := worker.NewPool(worker.Config{
		PoolID:   cfg.Worker.PoolID,
		Workers:  cfg.Worker.Workers,
		Broker:   mb,
		Store:    ms,
		Registry: handlers.DefaultRegistry(),
		Policy:   retry.NewExponential(cfg.Worker.RetryBase, cfg.Worker.RetryMax),
		Log:      logger,
	})
	pool.Start(ctx)

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", zap.Error(err))
	}
	pool.Wait()
	logger.Info("shutdown complete")
}
