package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"
)

type soakRoundTripFunc func(*http.Request) (*http.Response, error)

func (f soakRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func soakHTTPResponse(req *http.Request, code int, body []byte) *http.Response {
	return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Request: req}
}

func uncertainAckJSON() []byte {
	data, _ := json.Marshal(object{"code": 4, "message": uncertainCommitMessage})
	return data
}

func mutationClientFixture(t *testing.T, transport http.RoundTripper, budget time.Duration) *soakReport {
	return &soakReport{h: &harness{t: t, ctx: context.Background(), urls: [3]string{"http://scheduler.invalid"}, tokens: map[string]string{"writer": "test-credential"}, client: &http.Client{Transport: transport, Timeout: 4 * time.Second}}, Config: soakConfig{MaxLatency: budget}, MutationMaxAttempts: soakMutationAttempts}
}

func TestSoakMutationRetriesFrozenRequestAndOriginalAcknowledgement(t *testing.T) {
	command := object{"request_id": "original", "at": "2026-11-09T00:00:00Z"}
	body := object{"command": command, "launch": object{"run_id": "one-job", "definition_id": "hash", "definition_revision": "1"}}
	var payloads [][]byte
	var original object
	seen := make(map[string]object)
	effects := 0
	r := mutationClientFixture(t, soakRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != "POST" || req.URL.Path != "/api/jobs/v1/mutate" || req.Header.Get("Authorization") != "Bearer test-credential" {
			return nil, errors.New("request route or credentials changed")
		}
		data, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, data)
		var sent struct {
			Command struct {
				RequestID string `json:"request_id"`
			}
		}
		if err := json.Unmarshal(data, &sent); err != nil {
			return nil, err
		}
		ack, ok := seen[sent.Command.RequestID]
		if !ok {
			effects++
			ack = object{"request_id": sent.Command.RequestID, "fingerprint": "first-effect"}
			seen[sent.Command.RequestID] = ack
		}
		if len(payloads) == 1 {
			original = ack
			// Input changes between attempts must not change the retained bytes.
			command["request_id"], command["at"] = "changed", "2026-11-10T00:00:00Z"
			return soakHTTPResponse(req, 504, uncertainAckJSON()), nil
		}
		encoded, _ := json.Marshal(ack)
		return soakHTTPResponse(req, 200, encoded), nil
	}), time.Second)
	code, ack, err := r.callMutation(0, "writer", body)
	if err != nil || code != 200 || !reflect.DeepEqual(ack, original) {
		t.Fatalf("code=%d ack=%v err=%v", code, ack, err)
	}
	if len(payloads) != 2 || !bytes.Equal(payloads[0], payloads[1]) || effects != 1 {
		t.Fatalf("changed retry identity or repeated effect: requests=%d effects=%d", len(payloads), effects)
	}
	if r.RetriedMutationCalls != 1 || r.MutationRetries != 1 {
		t.Fatalf("incorrect retry accounting: %d/%d", r.RetriedMutationCalls, r.MutationRetries)
	}
}

func TestSoakMutationPreservesAdmissionAndProtocolFailures(t *testing.T) {
	for _, tc := range []struct {
		name        string
		code        int
		body        string
		decodeError bool
	}{
		{"storage hold", 503, `{"code":14,"message":"storage_pressure"}`, false},
		{"revision conflict", 409, `{"code":10,"message":"revision_conflict"}`, false},
		{"unrecognized timeout", 504, `{"code":4,"message":"different failure"}`, false},
		{"invalid response", 504, `not JSON`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			r := mutationClientFixture(t, soakRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				return soakHTTPResponse(req, tc.code, []byte(tc.body)), nil
			}), time.Second)
			code, _, err := r.callMutation(0, "writer", object{"command": object{"request_id": "one"}})
			if code != tc.code || (err != nil) != tc.decodeError || calls != 1 || r.MutationRetries != 0 {
				t.Fatalf("failure was retried or changed: code=%d err=%v calls=%d retries=%d", code, err, calls, r.MutationRetries)
			}
		})
	}
}

func TestSoakMutationBoundsAttemptsAndLatencyBudget(t *testing.T) {
	for _, budget := range []time.Duration{time.Second, 20 * time.Millisecond} {
		t.Run(budget.String(), func(t *testing.T) {
			calls := 0
			r := mutationClientFixture(t, soakRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				return soakHTTPResponse(req, 504, uncertainAckJSON()), nil
			}), budget)
			code, _, err := r.callMutation(0, "writer", object{"command": object{"request_id": "one"}})
			if budget == time.Second {
				if err != nil || code != 504 || calls != soakMutationAttempts || r.MutationRetries != 2 || r.RetriedMutationCalls != 1 {
					t.Fatalf("unbounded or incorrect attempts: code=%d err=%v calls=%d", code, err, calls)
				}
			} else if !errors.Is(err, context.DeadlineExceeded) || calls != 1 || r.MutationRetries != 0 {
				t.Fatalf("latency budget ignored: err=%v calls=%d retries=%d", err, calls, r.MutationRetries)
			}
		})
	}
}

// Withhold one real, successful acknowledgement after the controller commits.
// Keep TLS and the actual three-member scheduler/worker/gateway path intact.
type lostMutationAck struct {
	base      http.RoundTripper
	requestID string
	mu        sync.Mutex
	payloads  [][]byte
	original  object
	injected  bool
}

func (l *lostMutationAck) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != "POST" || req.URL.Path != "/api/jobs/v1/mutate" {
		return l.base.RoundTrip(req)
	}
	if req.GetBody == nil {
		return nil, errors.New("mutation body cannot be replayed")
	}
	copy, err := req.GetBody()
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(copy)
	copy.Close()
	if err != nil {
		return nil, err
	}
	var sent struct {
		Command struct {
			RequestID string `json:"request_id"`
		}
	}
	if err := json.Unmarshal(data, &sent); err != nil {
		return nil, err
	}
	if sent.Command.RequestID != l.requestID {
		return l.base.RoundTrip(req)
	}
	l.mu.Lock()
	l.payloads = append(l.payloads, data)
	l.mu.Unlock()
	response, err := l.base.RoundTrip(req)
	if err != nil || response.StatusCode != 200 {
		return response, err
	}
	l.mu.Lock()
	inject := !l.injected
	l.injected = true
	l.mu.Unlock()
	if !inject {
		return response, nil
	}
	ack, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return nil, err
	}
	var original object
	if err := json.Unmarshal(ack, &original); err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.original = original
	l.mu.Unlock()
	return soakHTTPResponse(req, 504, uncertainAckJSON()), nil
}

func TestLostMutationAcknowledgementDoesNotReplayGatewayIO(t *testing.T) {
	h := newHarness(t, false, func(h *harness) { h.remote = true })
	for _, tenant := range []string{"alpha", "beta"} {
		h.mutate(0, "platform", tenant, "put_tenant", object{"max_running": "4"})
	}
	endpoint, gateway := h.gateway(true)
	h.configure("ack-bus", "modbus-rtu-tcp", endpoint, "acme.meter", "measure")
	h.definition("alpha", "measure", "acme.check-measurement", "ack-bus", object{"maximum": 100})
	h.startWorker("alpha", "ack-worker", "/company-worker", 1)
	meta := h.meta()
	interceptor := &lostMutationAck{base: h.client.Transport, requestID: fmt.Sprint(meta["request_id"])}
	h.client.Transport = interceptor
	defer func() { h.client.Transport = interceptor.base }()
	r := &soakReport{h: h, Config: soakConfig{MaxLatency: 30 * time.Second}, MutationMaxAttempts: soakMutationAttempts}
	body := object{"command": meta, "launch": object{"definition_id": "measure", "definition_revision": "1", "run_id": "lost-ack"}}
	ack := r.requestMutation(1, "alpha-writer", body)
	h.run(1, "alpha", "lost-ack", "succeeded")
	interceptor.mu.Lock()
	original, payloads, injected := interceptor.original, interceptor.payloads, interceptor.injected
	interceptor.mu.Unlock()
	if !injected || len(payloads) != 2 || !bytes.Equal(payloads[0], payloads[1]) || !reflect.DeepEqual(ack, original) || r.MutationRetries != 1 {
		t.Fatalf("original acknowledgement or request identity changed: injected=%v requests=%d retries=%d", injected, len(payloads), r.MutationRetries)
	}
	if stats := gateway.call("GET", "/stats"); stats["requests"] != float64(1) || stats["maximum"] != float64(1) || stats["active"] != float64(0) {
		t.Fatalf("acknowledgement retry repeated or overlapped gateway I/O: %v", stats)
	}
}
