package api

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"

	"g-diwakar/distributed-task-queue/internal/broker"
	"g-diwakar/distributed-task-queue/internal/store"
)

type Server struct {
	broker broker.Broker
	store  store.Store
	log    *zap.Logger
	srv    *http.Server
}

func NewServer(addr string, b broker.Broker, s store.Store, log *zap.Logger) *Server {
	h := &Server{broker: b, store: s, log: log}

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)

	r.Post("/jobs", h.submitJob)
	r.Get("/jobs", h.listJobs)
	r.Get("/jobs/{id}", h.getJob)
	r.Delete("/jobs/{id}", h.cancelJob)

	h.srv = &http.Server{Addr: addr, Handler: r}
	return h
}

func (s *Server) Start() error {
	s.log.Info("http server listening", zap.String("addr", s.srv.Addr))
	return s.srv.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
