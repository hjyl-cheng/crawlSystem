package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alpkeskin/rota/core/internal/api/handlers"
	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/proxycontrol"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/go-chi/chi/v5"
)

type refreshProxyServerStub struct {
	username string
}

func (s *refreshProxyServerStub) ReloadSettings(context.Context) error { return nil }

func (s *refreshProxyServerStub) RefreshProxyUser(username string) {
	s.username = username
}

func (*refreshProxyServerStub) RetireProxyUser(context.Context, string) error { return nil }

func (*refreshProxyServerStub) RequireRouteActivationRegistry() {}

func (*refreshProxyServerStub) RebuildRouteActivationRegistry(
	context.Context,
	[]proxycontrol.RouteActivationRegistryEntry,
) error {
	return nil
}

func (*refreshProxyServerStub) BeginProxyUserActivation(
	context.Context,
	string,
	string,
	int,
	string,
	string,
) (proxycontrol.RouteActivationBeginResult, error) {
	return proxycontrol.RouteActivationBeginResult{}, nil
}

func (*refreshProxyServerStub) CommitProxyUserActivation(context.Context, string, string) error {
	return nil
}

func (*refreshProxyServerStub) RetireProxyUserIfClaim(
	context.Context,
	string,
	string,
) (bool, error) {
	return false, nil
}

func TestRefreshProxyUserInvalidatesOneNamedUser(t *testing.T) {
	proxyServer := &refreshProxyServerStub{}
	server := &Server{proxyServer: proxyServer}
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/proxy-users/refresh",
		strings.NewReader(`{"username":"bullmq-channel-04"}`),
	)
	response := httptest.NewRecorder()

	server.RefreshProxyUser(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if proxyServer.username != "bullmq-channel-04" {
		t.Fatalf("refreshed username = %q", proxyServer.username)
	}
}

func TestRefreshProxyUserRejectsMissingUsername(t *testing.T) {
	server := &Server{proxyServer: &refreshProxyServerStub{}}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/proxy-users/refresh", strings.NewReader(`{}`))
	response := httptest.NewRecorder()

	server.RefreshProxyUser(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

type proxyUserStoreStub struct {
	user *models.ProxyUser
}

func (*proxyUserStoreStub) List(context.Context) ([]models.ProxyUser, error) { return nil, nil }
func (s *proxyUserStoreStub) GetByID(context.Context, int) (*models.ProxyUser, error) {
	return s.user, nil
}
func (s *proxyUserStoreStub) Create(_ context.Context, req models.CreateProxyUserRequest) (*models.ProxyUser, error) {
	s.user = &models.ProxyUser{ID: 1, Username: req.Username, Enabled: req.Enabled}
	return s.user, nil
}
func (s *proxyUserStoreStub) Update(context.Context, int, models.UpdateProxyUserRequest) (*models.ProxyUser, error) {
	return s.user, nil
}
func (*proxyUserStoreStub) Delete(context.Context, int) error { return nil }

func TestProxyUserCreateInvalidatesAuthCacheBeforeResponding(t *testing.T) {
	proxyServer := &refreshProxyServerStub{}
	userHandler := handlers.NewUserHandler(&proxyUserStoreStub{}, nil, logger.New("error"))
	server := &Server{userHandler: userHandler}
	server.SetProxyServer(proxyServer)

	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/proxy-users",
		strings.NewReader(`{"username":"new-user","password":"secret","enabled":true}`),
	)
	response := httptest.NewRecorder()
	userHandler.Create(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if proxyServer.username != "new-user" {
		t.Fatalf("cache invalidated for %q", proxyServer.username)
	}
}

func TestProxyUserUpdateAndDeleteInvalidateNamedAuthCache(t *testing.T) {
	store := &proxyUserStoreStub{user: &models.ProxyUser{ID: 7, Username: "existing-user", Enabled: true}}
	proxyServer := &refreshProxyServerStub{}
	userHandler := handlers.NewUserHandler(store, nil, logger.New("error"))
	server := &Server{userHandler: userHandler}
	server.SetProxyServer(proxyServer)

	updateRequest := requestWithProxyUserID(
		http.MethodPut,
		"/api/v1/proxy-users/7",
		"7",
		`{"enabled":false}`,
	)
	updateResponse := httptest.NewRecorder()
	userHandler.Update(updateResponse, updateRequest)
	if updateResponse.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", updateResponse.Code, updateResponse.Body.String())
	}
	if proxyServer.username != "existing-user" {
		t.Fatalf("update invalidated cache for %q", proxyServer.username)
	}

	proxyServer.username = ""
	deleteRequest := requestWithProxyUserID(http.MethodDelete, "/api/v1/proxy-users/7", "7", "")
	deleteResponse := httptest.NewRecorder()
	userHandler.Delete(deleteResponse, deleteRequest)
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleteResponse.Code, deleteResponse.Body.String())
	}
	if proxyServer.username != "existing-user" {
		t.Fatalf("delete invalidated cache for %q", proxyServer.username)
	}
}

func requestWithProxyUserID(method, target, id, body string) *http.Request {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("id", id)
	return request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))
}
