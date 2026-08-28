package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/alpkeskin/rota/core/internal/proxycontrol"
	"github.com/go-chi/chi/v5"
)

const maxProxyControlBody = 64 * 1024

func ProxyControlTokenMiddleware(expected string) func(http.Handler) http.Handler {
	expectedBytes := []byte(expected)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provided := extractBearerToken(r)
			if provided == "" || len(provided) != len(expected) ||
				subtle.ConstantTimeCompare([]byte(provided), expectedBytes) != 1 {
				writeControlJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid proxy control token"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type ProxyControlHandler struct {
	control proxycontrol.Interface
}

func NewProxyControlHandler(control proxycontrol.Interface) *ProxyControlHandler {
	return &ProxyControlHandler{control: control}
}

func (h *ProxyControlHandler) Claim(w http.ResponseWriter, r *http.Request) {
	h.command(w, r, func() (any, error) {
		var request proxycontrol.ClaimRequest
		if err := decodeControlJSON(r, &request); err != nil {
			return nil, err
		}
		return h.control.Claim(r.Context(), request)
	})
}

func (h *ProxyControlHandler) Renew(w http.ResponseWriter, r *http.Request) {
	h.command(w, r, func() (any, error) {
		var request proxycontrol.RenewRequest
		if err := decodeControlJSON(r, &request); err != nil {
			return nil, err
		}
		return h.control.Renew(r.Context(), request)
	})
}

func (h *ProxyControlHandler) BeginTask(w http.ResponseWriter, r *http.Request) {
	h.command(w, r, func() (any, error) {
		var request proxycontrol.BeginTaskRequest
		if err := decodeControlJSON(r, &request); err != nil {
			return nil, err
		}
		return h.control.BeginTask(r.Context(), request)
	})
}

func (h *ProxyControlHandler) Observe(w http.ResponseWriter, r *http.Request) {
	h.command(w, r, func() (any, error) {
		var request proxycontrol.ObserveRequest
		if err := decodeControlJSON(r, &request); err != nil {
			return nil, err
		}
		return h.control.Observe(r.Context(), request)
	})
}

func (h *ProxyControlHandler) CompleteTask(w http.ResponseWriter, r *http.Request) {
	h.command(w, r, func() (any, error) {
		var request proxycontrol.CompleteTaskRequest
		if err := decodeControlJSON(r, &request); err != nil {
			return nil, err
		}
		return h.control.CompleteTask(r.Context(), request)
	})
}

func (h *ProxyControlHandler) Report(w http.ResponseWriter, r *http.Request) {
	h.command(w, r, func() (any, error) {
		var request proxycontrol.ReportRequest
		if err := decodeControlJSON(r, &request); err != nil {
			return nil, err
		}
		return h.control.Report(r.Context(), request)
	})
}

func (h *ProxyControlHandler) Release(w http.ResponseWriter, r *http.Request) {
	h.command(w, r, func() (any, error) {
		var request proxycontrol.ReleaseRequest
		if err := decodeControlJSON(r, &request); err != nil {
			return nil, err
		}
		return h.control.Release(r.Context(), request)
	})
}

func (h *ProxyControlHandler) Capacity(w http.ResponseWriter, r *http.Request) {
	result, err := h.control.Capacity(r.Context())
	if err != nil {
		writeControlError(w, err)
		return
	}
	writeControlJSON(w, http.StatusOK, result)
}

func (h *ProxyControlHandler) BusinessRunBudget(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.control == nil {
		writeControlError(w, proxycontrol.ErrDisabled)
		return
	}
	result, err := h.control.BusinessRunBudget(
		r.Context(),
		chi.URLParam(r, "businessRunID"),
	)
	if err != nil {
		writeControlError(w, err)
		return
	}
	writeControlJSON(w, http.StatusOK, result)
}

func (h *ProxyControlHandler) command(
	w http.ResponseWriter,
	r *http.Request,
	command func() (any, error),
) {
	if h == nil || h.control == nil {
		writeControlError(w, proxycontrol.ErrDisabled)
		return
	}
	result, err := command()
	if err != nil {
		writeControlError(w, err)
		return
	}
	writeControlJSON(w, http.StatusOK, result)
}

func decodeControlJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxProxyControlBody+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%w: invalid JSON body: %v", proxycontrol.ErrInvalidInput, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: request must contain one JSON object", proxycontrol.ErrInvalidInput)
	}
	return nil
}

func writeControlError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "PROXY_CONTROL_REQUEST_FAILED"
	switch {
	case errors.Is(err, proxycontrol.ErrInvalidInput):
		status = http.StatusBadRequest
		code = "INVALID_REQUEST"
	case errors.Is(err, proxycontrol.ErrLeaseConflict):
		status = http.StatusConflict
		code = "LEASE_CONFLICT"
	case errors.Is(err, proxycontrol.ErrLeaseGone):
		status = http.StatusGone
		code = "LEASE_GONE"
	case errors.Is(err, proxycontrol.ErrRouteNotReady):
		status = http.StatusConflict
		code = "ROUTE_NOT_READY"
	case errors.Is(err, proxycontrol.ErrPolicyRejected):
		status = http.StatusForbidden
		code = "POLICY_REJECTED"
	case errors.Is(err, proxycontrol.ErrIdempotencyConflict):
		status = http.StatusConflict
		code = "IDEMPOTENCY_KEY_REUSED"
	case errors.Is(err, proxycontrol.ErrJobExecutionConflict):
		status = http.StatusConflict
		code = "JOB_EXECUTION_ID_CONFLICT"
	case errors.Is(err, proxycontrol.ErrExecutionBudget):
		status = http.StatusConflict
		code = "EXECUTION_ROUTE_BUDGET_EXHAUSTED"
	case errors.Is(err, proxycontrol.ErrBusinessRunBudget):
		status = http.StatusConflict
		code = "BUSINESS_RUN_BUDGET_EXHAUSTED"
	case errors.Is(err, proxycontrol.ErrBusinessRunNotFound):
		status = http.StatusNotFound
		code = "BUSINESS_RUN_NOT_FOUND"
	case errors.Is(err, proxycontrol.ErrTaskConflict):
		status = http.StatusConflict
		code = "TASK_FENCE_CONFLICT"
	case errors.Is(err, proxycontrol.ErrTaskCompleted):
		status = http.StatusConflict
		code = "TASK_ALREADY_COMPLETED"
	case errors.Is(err, proxycontrol.ErrAttemptNotQuiesced):
		status = http.StatusConflict
		code = "ATTEMPT_NOT_QUIESCED"
	case errors.Is(err, proxycontrol.ErrCompletionConflict):
		status = http.StatusConflict
		code = "TASK_COMPLETION_CONFLICT"
	case errors.Is(err, proxycontrol.ErrObservationReference):
		status = http.StatusUnprocessableEntity
		code = "OBSERVATION_REFERENCE_INVALID"
	case errors.Is(err, proxycontrol.ErrDisabled):
		status = http.StatusServiceUnavailable
		code = "PROXY_CONTROL_DISABLED"
	}
	message := "proxy control request failed"
	if status != http.StatusInternalServerError {
		message = err.Error()
	}
	writeControlJSON(w, status, map[string]string{"code": code, "error": message})
}

func writeControlJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
