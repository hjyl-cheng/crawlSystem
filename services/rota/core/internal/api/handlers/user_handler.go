package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/go-chi/chi/v5"
)

// ProxyUserStore is the persistence interface exercised by UserHandler.
type ProxyUserStore interface {
	List(ctx context.Context) ([]models.ProxyUser, error)
	GetByID(ctx context.Context, id int) (*models.ProxyUser, error)
	Create(ctx context.Context, req models.CreateProxyUserRequest) (*models.ProxyUser, error)
	Update(ctx context.Context, id int, req models.UpdateProxyUserRequest) (*models.ProxyUser, error)
	Delete(ctx context.Context, id int) error
}

// UserHandler handles proxy user management endpoints
type UserHandler struct {
	userRepo     ProxyUserStore
	poolRepo     *repository.PoolRepository
	logger       *logger.Logger
	onUserChange func(username string)
}

// NewUserHandler creates a new UserHandler
func NewUserHandler(
	userRepo ProxyUserStore,
	poolRepo *repository.PoolRepository,
	log *logger.Logger,
) *UserHandler {
	return &UserHandler{userRepo: userRepo, poolRepo: poolRepo, logger: log}
}

// SetOnChange registers the synchronous cache invalidation hook used after a
// Proxy User mutation has been persisted.
func (h *UserHandler) SetOnChange(fn func(username string)) {
	h.onUserChange = fn
}

func (h *UserHandler) notifyUserChange(username string) {
	if h.onUserChange != nil {
		h.onUserChange(username)
	}
}

// List returns all proxy users
func (h *UserHandler) List(w http.ResponseWriter, r *http.Request) {
	users, err := h.userRepo.List(r.Context())
	if err != nil {
		h.logger.Error("list users failed", "error", err)
		http.Error(w, `{"error":"failed to list users"}`, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"users": users})
}

// Get returns a single user (no password)
func (h *UserHandler) Get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}
	u, err := h.userRepo.GetByID(r.Context(), id)
	if err != nil || u == nil {
		http.Error(w, `{"error":"user not found"}`, http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, u)
}

// Create adds a new proxy user
func (h *UserHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req models.CreateProxyUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}
	if req.Username == "" || req.Password == "" {
		http.Error(w, `{"error":"username and password are required"}`, http.StatusBadRequest)
		return
	}
	if req.MaxRetries <= 0 {
		req.MaxRetries = 5
	}

	u, err := h.userRepo.Create(r.Context(), req)
	if err != nil {
		h.logger.Error("create user failed", "error", err)
		http.Error(w, `{"error":"failed to create user: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	h.notifyUserChange(u.Username)
	writeJSON(w, http.StatusCreated, u)
}

// Update modifies an existing user
func (h *UserHandler) Update(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}
	var req models.UpdateProxyUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}
	u, err := h.userRepo.Update(r.Context(), id, req)
	if err != nil || u == nil {
		http.Error(w, `{"error":"user not found or update failed"}`, http.StatusNotFound)
		return
	}
	h.notifyUserChange(u.Username)
	writeJSON(w, http.StatusOK, u)
}

// Delete removes a user
func (h *UserHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}
	user, err := h.userRepo.GetByID(r.Context(), id)
	if err != nil {
		http.Error(w, `{"error":"failed to load user"}`, http.StatusInternalServerError)
		return
	}
	if err := h.userRepo.Delete(r.Context(), id); err != nil {
		http.Error(w, `{"error":"failed to delete user"}`, http.StatusInternalServerError)
		return
	}
	if user != nil {
		h.notifyUserChange(user.Username)
	} else {
		h.notifyUserChange("")
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
