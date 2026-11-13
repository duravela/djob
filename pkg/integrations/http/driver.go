// Package httpdriver supplies the optional resource-free HTTP job driver.
package httpdriver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/duravela/djob/sdk/go"
	"io"
	"net/http"
	"net/url"
)

type HTTPOperation struct {
	Name                string `json:"name"`
	Method              string `json:"method"`
	URL                 string `json:"url"`
	ContentType         string `json:"content_type,omitempty"`
	SupportsIdempotency bool   `json:"supports_idempotency,omitempty"`
}
type HTTPBinding struct {
	Operations []HTTPOperation `json:"operations"`
}

// HTTPFactory maps semantic action names to configured HTTP requests. A caller
// can supply an authenticated client without putting secrets into Raft state.
// Redirects are disabled. Failed/uncertain requests have no workflow-level retry.
func NewFactory(client *http.Client) djob.DriverFactory {
	return func(run djob.JobRun) (djob.Driver, error) {
		// An HTTP response can acknowledge asynchronous work. It cannot attest
		// that a reserved device/bus or remote execution slot is quiescent.
		// Resource-backed HTTP integrations need their own recovery contract.
		if len(run.Resources) != 0 {
			return nil, fmt.Errorf("the supplied HTTP driver requires a resource-free run")
		}
		if run.Binding == nil {
			return nil, fmt.Errorf("HTTP driver requires a binding")
		}
		var config HTTPBinding
		if err := json.Unmarshal(run.Binding.Config, &config); err != nil {
			return nil, err
		}
		operations := make(map[string]HTTPOperation)
		for _, op := range config.Operations {
			u, err := url.Parse(op.URL)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || op.Name == "" || op.Method == "" {
				return nil, fmt.Errorf("invalid HTTP operation binding")
			}
			if _, duplicate := operations[op.Name]; duplicate {
				return nil, fmt.Errorf("duplicate HTTP operation")
			}
			operations[op.Name] = op
		}
		if len(operations) == 0 {
			return nil, fmt.Errorf("HTTP binding has no operations")
		}
		base := http.DefaultClient
		if client != nil {
			base = client
		}
		copy := *base
		copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		return &httpDriver{client: &copy, operations: operations}, nil
	}
}

type httpDriver struct {
	client     *http.Client
	operations map[string]HTTPOperation
	uncertain  bool
}

func (d *httpDriver) Execute(ctx context.Context, action djob.Action) djob.Effect {
	op, ok := d.operations[action.Operation]
	if !ok {
		return djob.Effect{Status: djob.EffectFailed, Error: "semantic operation has no HTTP binding"}
	}
	request, err := http.NewRequestWithContext(ctx, op.Method, op.URL, bytes.NewReader(action.Input))
	if err != nil {
		return djob.Effect{Status: djob.EffectFailed, Error: err.Error()}
	}
	if op.ContentType != "" {
		request.Header.Set("Content-Type", op.ContentType)
	}
	// Go's transport can retry requests with this header. Set it only when the
	// configured target actually guarantees idempotency for the supplied key.
	if op.SupportsIdempotency {
		request.Header.Set("Idempotency-Key", action.IdempotencyKey)
	}
	response, err := d.client.Do(request)
	if err != nil {
		d.uncertain = true
		return djob.Effect{Status: djob.EffectUnknown, Error: err.Error()}
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	closeErr := response.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil || len(data) > 1<<20 {
		d.uncertain = true
		if err == nil {
			err = fmt.Errorf("HTTP response exceeds 1 MiB")
		}
		return djob.Effect{Status: djob.EffectUnknown, Error: err.Error()}
	}
	status := djob.EffectSucceeded
	message := ""
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		status, message = djob.EffectFailed, response.Status
	}
	return djob.Effect{Status: status, Data: data, Error: message}
}

func (d *httpDriver) Quiesce(context.Context) error {
	if d.uncertain {
		return fmt.Errorf("HTTP peer completion is uncertain")
	}
	return nil
}
func (d *httpDriver) Close(context.Context) error { return nil } // Response bodies are closed by Execute; the client can be shared.
