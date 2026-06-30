package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"g-diwakar/distributed-task-queue/internal/job"
	"g-diwakar/distributed-task-queue/internal/store"
)

type submitRequest struct {
	Type        job.JobType            `json:"type"`
	Priority    job.Priority           `json:"priority"`
	Payload     map[string]any `json:"payload"`
	MaxAttempts int                    `json:"max_attempts"`
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if req.Payload == nil {
		req.Payload = make(map[string]any)
	}
	if err := req.Type.ValidatePayload(req.Payload); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if req.Priority == 0 {
		req.Priority = job.PriorityMedium
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = 3
	}

	j := &job.Job{
		ID:          newID(),
		Type:        req.Type,
		Priority:    req.Priority,
		Status:      job.StatusPending,
		Payload:     req.Payload,
		MaxAttempts: req.MaxAttempts,
		CreatedAt:   time.Now(),
	}

	if err := s.broker.Enqueue(r.Context(), j); err != nil {
		s.log.Error("enqueue failed", zap.String("job_id", j.ID), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to submit job")
		return
	}

	writeJSON(w, http.StatusCreated, j)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	j, err := s.store.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	f := store.Filter{}
	if v := r.URL.Query().Get("status"); v != "" {
		f.Status = job.Status(v)
	}
	if v := r.URL.Query().Get("type"); v != "" {
		f.Type = job.JobType(v)
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			f.Limit = n
		}
	}

	jobs, err := s.store.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	j, err := s.store.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}

	switch j.Status {
	case job.StatusCompleted, job.StatusDead, job.StatusCancelled:
		writeError(w, http.StatusConflict, "job is already in a terminal state")
		return
	}

	j.Status = job.StatusCancelled
	if err := s.store.Update(r.Context(), j); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to cancel job")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func newID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
