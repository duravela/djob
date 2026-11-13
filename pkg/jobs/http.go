package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/duravela/djob/pkg/db"
	httpdriver "github.com/duravela/djob/pkg/integrations/http"
)

type HTTPOperation = httpdriver.HTTPOperation
type HTTPBinding = httpdriver.HTTPBinding

// HTTPFactory preserves the embedded API for the optional first-party driver.
func HTTPFactory(client *http.Client) DriverFactory { return httpdriver.NewFactory(client) }

type Callback struct {
	RunID   string `json:"run_id"`
	EventID string `json:"event_id"`
	Key     string `json:"key"`
	Data    []byte `json:"data,omitempty"`
}

// CallbackHandler is an embedding helper, not a route on the legacy API. Its
// authenticator derives tenant scope from credentials, never from callback data.
// The event is acknowledged only after Raft durably applies its inbox record.
func CallbackHandler(backend Backend, authenticate func(*http.Request) (string, error)) (http.Handler, error) {
	if backend == nil || authenticate == nil {
		return nil, fmt.Errorf("callback backend and tenant authenticator are required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		tenant, err := authenticate(request)
		if err != nil || tenant == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var callback Callback
		decoder := json.NewDecoder(http.MaxBytesReader(w, request.Body, 2<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&callback); err != nil {
			http.Error(w, "invalid callback", http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "invalid callback", http.StatusBadRequest)
			return
		}
		id, err := newID()
		if err != nil {
			http.Error(w, "callback unavailable", http.StatusServiceUnavailable)
			return
		}
		command := db.StateCommand{RequestID: "callback/" + id, At: time.Now().UTC(), Job: &db.JobCommand{Signal: &db.SignalJob{TenantID: tenant, RunID: callback.RunID, EventID: callback.EventID, Key: callback.Key, Data: callback.Data}}}
		_, err = propose(request.Context(), backend, 2*time.Second, command)
		if err != nil {
			status := http.StatusServiceUnavailable
			var rejection *db.CommandRejection
			if errors.As(err, &rejection) {
				switch rejection.Code {
				case "quota_exceeded", "backpressure":
					status = http.StatusTooManyRequests
				case "not_found":
					status = http.StatusNotFound
				case "invalid":
					status = http.StatusBadRequest
				default:
					status = http.StatusConflict
				}
			}
			http.Error(w, http.StatusText(status), status)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}), nil
}
