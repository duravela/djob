package console

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/duravela/djob/pkg/consolesession"
	"github.com/stretchr/testify/require"
)

func TestSharedSessionPreservesControllerAdmissionStatus(t *testing.T) {
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 429, map[string]string{"error": "session probe budget exhausted"})
	}))
	defer controller.Close()
	app, err := New(Config{Origin: testOrigin, Controllers: []string{controller.URL}, SharedSessions: true})
	require.NoError(t, err)
	defer app.Close()
	token, err := consolesession.NewCredential()
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodGet, testOrigin+api+"session", nil)
	request.AddCookie(&http.Cookie{Name: app.cookieName, Value: token})
	response := httptest.NewRecorder()
	app.ServeHTTP(response, request)
	require.Equal(t, 429, response.Code)
}

func TestSharedLoginPreservesControllerRejectionAndUnavailability(t *testing.T) {
	for _, code := range []int{403, 503} {
		controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, code, map[string]string{"error": "identity rejected or unavailable"})
		}))
		app, err := New(Config{Origin: testOrigin, Controllers: []string{controller.URL}, SharedSessions: true})
		require.NoError(t, err)
		request := httptest.NewRequest(http.MethodPost, testOrigin+api+"session", bytes.NewBufferString(`{"token":"`+testToken+`"}`))
		request.Header.Set("Origin", testOrigin)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		app.ServeHTTP(response, request)
		require.Equal(t, code, response.Code)
		app.Close()
		controller.Close()
	}
}
