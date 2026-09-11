package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/alpkeskin/rota/core/internal/proxycontrol"
)

type RemoteRouteReader interface {
	ReadRemoteRoute(context.Context, proxycontrol.RemoteRouteRequest) (proxycontrol.RemoteRoute, error)
}

// Separate from the ordinary Worker control token: only the center's remote
// coordinator may read upstream credentials. This handler is not mounted by
// the production server's default constructor or ordinary proxy-control routes.
func NewRemoteRouteHandler(reader RemoteRouteReader, token string) (http.Handler, error) {
	if reader == nil || len(token) < 32 || strings.TrimSpace(token) != token {
		return nil, errors.New("dedicated remote route token required")
	}
	return ProxyControlTokenMiddleware(token)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeControlJSON(w, 405, map[string]string{"code": "METHOD_NOT_ALLOWED"})
			return
		}
		var request proxycontrol.RemoteRouteRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			writeControlError(w, proxycontrol.ErrInvalidInput)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			writeControlError(w, proxycontrol.ErrInvalidInput)
			return
		}
		result, err := reader.ReadRemoteRoute(r.Context(), request)
		if err != nil {
			writeControlError(w, err)
			return
		}
		// Explicit credential exposure only on this protected internal interface.
		writeControlJSON(w, 200, struct {
			proxycontrol.RemoteRoute
			Upstream map[string]string `json:"upstream"`
		}{result, map[string]string{"protocol": result.Upstream.Protocol, "address": result.Upstream.Address,
			"username": result.Upstream.Username, "password": result.Upstream.Password}})
	})), nil
}

// EnableRemoteRouteRead must be called explicitly during assembly, before the
// router starts serving. Existing cmd/server never calls it. Deployments must
// additionally keep this endpoint on the center's protected internal network.
func (s *Server) EnableRemoteRouteRead(token string) error {
	if !s.proxyControlEnabled || s.proxyControl == nil {
		return proxycontrol.ErrDisabled
	}
	if token == s.proxyControlToken {
		return errors.New("remote route token must differ from worker control token")
	}
	handler, err := NewRemoteRouteHandler(s.proxyControl, token)
	if err != nil {
		return err
	}
	s.router.Method(http.MethodPost, "/internal/v1/remote-route", handler)
	return nil
}
