// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package worker_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/worker"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
)

func tlsController(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewTLSServer(handler)
	t.Cleanup(s.Close)
	return s
}

func supervisedConfig(t *testing.T, servers ...*httptest.Server) worker.Config {
	t.Helper()
	pool := x509.NewCertPool()
	for _, s := range servers {
		pool.AddCert(s.Certificate())
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	cfg := worker.Config{Transport: worker.TransportHTTPSV1, ServerURL: servers[0].URL, WorkerID: "test-worker", TenantID: "alpha", Token: strings.Repeat("s", 32), Client: &http.Client{Transport: transport, Timeout: time.Second}, Registry: djob.Registry{
		Handlers: map[djob.Version]djob.Handler{{Name: "test", Version: "1"}: func(djob.View) (djob.Decision, error) {
			return djob.Decision{Kind: djob.JobComplete, Status: djob.JobSucceeded}, nil
		}},
	}}
	for _, s := range servers[1:] {
		cfg.FallbackURLs = append(cfg.FallbackURLs, s.URL)
	}
	return cfg
}

func smallBackoff() worker.ReconnectConfig {
	return worker.ReconnectConfig{MinBackoff: 100 * time.Millisecond, MaxBackoff: 200 * time.Millisecond}
}

func registered(w http.ResponseWriter, id string) {
	_ = json.NewEncoder(w).Encode(wire.Registered{Protocol: wire.Version, SessionID: id, TenantID: "alpha", HeartbeatEvery: 20 * time.Millisecond})
}

func TestServeTriesExplicitFallbackAfterTransientRegistrationFailure(t *testing.T) {
	var primaryCalls, fallbackCalls atomic.Int32
	primary := tlsController(t, func(w http.ResponseWriter, r *http.Request) { primaryCalls.Add(1); w.WriteHeader(503) })
	fallback := tlsController(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/register") {
			fallbackCalls.Add(1)
		}
		(&controller{}).handler(w, r)
	})
	cfg := supervisedConfig(t, primary, fallback)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := worker.Serve(ctx, cfg, smallBackoff()); err != nil {
		t.Fatal(err)
	}
	if primaryCalls.Load() != 1 || fallbackCalls.Load() != 1 {
		t.Fatalf("registrations: primary=%d fallback=%d", primaryCalls.Load(), fallbackCalls.Load())
	}
}

type cleanupDriver struct {
	*device
	quiesceErr, closeErr error
}

func (d *cleanupDriver) Quiesce(ctx context.Context) error {
	d.device.Quiesce(ctx)
	return d.quiesceErr
}
func (d *cleanupDriver) Close(ctx context.Context) error { d.device.Close(ctx); return d.closeErr }

// A report can reach the controller while its HTTP acknowledgement is lost.
// The next session receives fresh work, never that old report or action again.
func TestServeCleansUpBeforeFreshRegistrationAfterLostResult(t *testing.T) {
	for _, cleanupFailure := range []string{"", "quiesce", "close"} {
		t.Run(cleanupFailure, func(t *testing.T) {
			var registrations, effects, factories, firstPolls, secondPolls, firstResults, secondResults atomic.Int32
			d := &cleanupDriver{device: &device{execute: func(context.Context, djob.Action) djob.Effect {
				effects.Add(1)
				return djob.Effect{Status: djob.EffectSucceeded}
			}}}
			if cleanupFailure == "quiesce" {
				d.quiesceErr = errors.New("cannot establish device idle")
			}
			if cleanupFailure == "close" {
				d.closeErr = errors.New("cannot close local session")
			}
			firstID, secondID := strings.Repeat("a", 64), strings.Repeat("b", 64)
			s := tlsController(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/register"):
					n := registrations.Add(1)
					if n == 1 {
						registered(w, firstID)
					} else {
						if d.quiesced.Load() != 1 || d.closed.Load() != 1 {
							t.Error("registered before local cleanup finished")
						}
						registered(w, secondID)
					}
				case strings.HasSuffix(r.URL.Path, "/heartbeat"):
					var q wire.Heartbeat
					_ = json.NewDecoder(r.Body).Decode(&q)
					_ = json.NewEncoder(w).Encode(wire.HeartbeatReply{CommandID: q.CommandID, Budget: time.Second})
				case strings.Contains(r.URL.Path, firstID+"/poll"):
					if firstPolls.Add(1) != 1 {
						t.Error("polled a lost session again")
					}
					_ = json.NewEncoder(w).Encode(action(1))
				case strings.Contains(r.URL.Path, firstID+"/result"):
					firstResults.Add(1)
					w.WriteHeader(503)
				case strings.Contains(r.URL.Path, secondID+"/poll"):
					if secondPolls.Add(1) == 1 {
						_ = json.NewEncoder(w).Encode(wire.Command{ID: strings.Repeat("c", 64), Sequence: 1, Kind: wire.Evaluate, Budget: time.Second, Timeout: time.Second, View: &djob.View{Run: djob.JobRun{TenantID: "alpha", ID: "unrelated", Definition: djob.JobDefinition{Handler: "test", HandlerVersion: "1"}}}})
					} else {
						_ = json.NewEncoder(w).Encode(wire.Command{Kind: wire.Drained})
					}
				case strings.Contains(r.URL.Path, secondID+"/result"):
					secondResults.Add(1)
					w.WriteHeader(204)
				case strings.HasSuffix(r.URL.Path, "/disconnect"):
					w.WriteHeader(204)
				default:
					w.WriteHeader(404)
				}
			})
			cfg := supervisedConfig(t, s)
			cfg.Registry.Drivers = map[djob.Version]djob.DriverFactory{{Name: "device", Version: "1"}: func(djob.JobRun) (djob.Driver, error) { factories.Add(1); return d, nil }}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := worker.Serve(ctx, cfg, smallBackoff())
			if cleanupFailure != "" {
				if !errors.Is(err, worker.ErrCleanupFailed) || registrations.Load() != 1 {
					t.Fatalf("unsafe reconnect after cleanup failure: registrations=%d err=%v", registrations.Load(), err)
				}
			} else if err != nil || registrations.Load() != 2 || secondResults.Load() != 1 {
				t.Fatalf("fresh session: registrations=%d results=%d err=%v", registrations.Load(), secondResults.Load(), err)
			}
			if effects.Load() != 1 || factories.Load() != 1 || firstResults.Load() != 1 {
				t.Fatalf("old effect or result repeated: effects=%d factories=%d reports=%d", effects.Load(), factories.Load(), firstResults.Load())
			}
		})
	}
}

func TestServeStopsOnPermanentRegistrationOrProtocolErrors(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 409, 426, 200, 204} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var requests, retries atomic.Int32
			s := tlsController(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(status)
				_, _ = w.Write([]byte("secret-marker invalid response"))
			})
			cfg := supervisedConfig(t, s)
			retry := smallBackoff()
			retry.OnRetry = func(worker.RetryObservation) { retries.Add(1) }
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := worker.Serve(ctx, cfg, retry)
			if err == nil || errors.Is(err, worker.ErrSessionLost) || strings.Contains(err.Error(), "secret-marker") || requests.Load() != 1 || retries.Load() != 0 {
				t.Fatalf("permanent failure retried or leaked: requests=%d retries=%d err=%v", requests.Load(), retries.Load(), err)
			}
		})
	}
}

func TestServePreservesPermanentHeartbeatFailureWhileCancelingPoll(t *testing.T) {
	for _, status := range []int{401, 200, 204} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var registrations, retries atomic.Int32
			s := tlsController(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/register"):
					registrations.Add(1)
					registered(w, strings.Repeat("a", 64))
				case strings.HasSuffix(r.URL.Path, "/heartbeat"):
					w.WriteHeader(status)
					_, _ = w.Write([]byte("invalid heartbeat"))
				case strings.HasSuffix(r.URL.Path, "/poll"):
					_, _ = io.Copy(io.Discard, r.Body)
					select {
					case <-r.Context().Done():
					case <-time.After(time.Second):
						w.WriteHeader(204)
					}
				case strings.HasSuffix(r.URL.Path, "/disconnect"):
					w.WriteHeader(204)
				default:
					w.WriteHeader(404)
				}
			})
			cfg := supervisedConfig(t, s)
			retry := smallBackoff()
			retry.OnRetry = func(worker.RetryObservation) { retries.Add(1) }
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := worker.Serve(ctx, cfg, retry)
			if err == nil || errors.Is(err, worker.ErrSessionLost) || errors.Is(err, context.DeadlineExceeded) || retries.Load() != 0 || registrations.Load() != 1 {
				t.Fatalf("heartbeat failure became a retry: registrations=%d retries=%d err=%v", registrations.Load(), retries.Load(), err)
			}
		})
	}
}

type responseTransport func(*http.Request) (*http.Response, error)

func (f responseTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestServeDoesNotHideFatalHeartbeatBehindALateTransientPollResponse(t *testing.T) {
	s := tlsController(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			registered(w, strings.Repeat("a", 64))
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			w.WriteHeader(401)
		case strings.HasSuffix(r.URL.Path, "/disconnect"):
			w.WriteHeader(204)
		default:
			t.Error("unexpected controller request")
			w.WriteHeader(404)
		}
	})
	cfg := supervisedConfig(t, s)
	base := cfg.Client.Transport
	cfg.Client.Transport = responseTransport(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/poll") {
			<-r.Context().Done()
			return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
		}
		return base.RoundTrip(r)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var retries int
	retry := smallBackoff()
	retry.OnRetry = func(worker.RetryObservation) { retries++ }
	if err := worker.Serve(ctx, cfg, retry); err == nil || errors.Is(err, worker.ErrSessionLost) || retries != 0 {
		t.Fatalf("terminal heartbeat was hidden by a late response: retries=%d err=%v", retries, err)
	}
}

func TestServeBackoffIsBoundedAndCancellationStopsRegistration(t *testing.T) {
	var registrations atomic.Int32
	handler := func(w http.ResponseWriter, r *http.Request) { registrations.Add(1); w.WriteHeader(503) }
	a, b := tlsController(t, handler), tlsController(t, handler)
	cfg := supervisedConfig(t, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	retry := smallBackoff()
	retry.ResetAfter = time.Hour
	var observations []worker.RetryObservation
	retry.OnRetry = func(o worker.RetryObservation) {
		observations = append(observations, o)
		if len(observations) == 3 {
			cancel()
		}
	}
	if err := worker.Serve(ctx, cfg, retry); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel backoff: %v", err)
	}
	if registrations.Load() != 3 || len(observations) != 3 {
		t.Fatalf("registrations continued after cancellation: registrations=%d retries=%d", registrations.Load(), len(observations))
	}
	for i, o := range observations {
		lower, upper := 50*time.Millisecond, 100*time.Millisecond
		if i > 0 {
			lower, upper = 100*time.Millisecond, 200*time.Millisecond
		}
		if o.Delay < lower || o.Delay >= upper || o.ControllerIndex != (i+1)%2 {
			t.Fatalf("unbounded delay or wrong controller: %+v", o)
		}
	}
}

func TestServeResetsBackoffAfterAStableSession(t *testing.T) {
	var registrations atomic.Int32
	s := tlsController(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			if registrations.Add(1) == 1 {
				w.WriteHeader(503)
			} else {
				registered(w, strings.Repeat("a", 64))
			}
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			var q wire.Heartbeat
			_ = json.NewDecoder(r.Body).Decode(&q)
			_ = json.NewEncoder(w).Encode(wire.HeartbeatReply{CommandID: q.CommandID, Budget: time.Second})
		case strings.HasSuffix(r.URL.Path, "/poll"):
			if registrations.Load() == 2 {
				time.Sleep(1100 * time.Millisecond)
				w.WriteHeader(410)
			} else {
				_ = json.NewEncoder(w).Encode(wire.Command{Kind: wire.Drained})
			}
		case strings.HasSuffix(r.URL.Path, "/disconnect"):
			w.WriteHeader(204)
		default:
			w.WriteHeader(404)
		}
	})
	cfg := supervisedConfig(t, s)
	cfg.Client.Timeout = 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	retry := smallBackoff()
	retry.ResetAfter = time.Second
	var delays []time.Duration
	retry.OnRetry = func(o worker.RetryObservation) { delays = append(delays, o.Delay) }
	if err := worker.Serve(ctx, cfg, retry); err != nil {
		t.Fatal(err)
	}
	if len(delays) != 2 || delays[1] >= 100*time.Millisecond {
		t.Fatalf("stable session did not reset backoff: %v", delays)
	}
}

func TestServeDoesNotResetBackoffForSlowUnsuccessfulRegistration(t *testing.T) {
	var registrations atomic.Int32
	s := tlsController(t, func(w http.ResponseWriter, r *http.Request) {
		registrations.Add(1)
		time.Sleep(1100 * time.Millisecond)
		w.WriteHeader(503)
	})
	cfg := supervisedConfig(t, s)
	cfg.Client.Timeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	retry := smallBackoff()
	retry.ResetAfter = time.Second
	var delays []time.Duration
	retry.OnRetry = func(o worker.RetryObservation) {
		delays = append(delays, o.Delay)
		if len(delays) == 2 {
			cancel()
		}
	}
	if err := worker.Serve(ctx, cfg, retry); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if registrations.Load() != 2 || len(delays) != 2 || delays[1] < 100*time.Millisecond {
		t.Fatalf("failed registration reset backoff without establishing a session: registrations=%d delays=%v", registrations.Load(), delays)
	}
}

func TestServeStopsWhenTheServerRejectsClientTLS(t *testing.T) {
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unauthenticated TLS client reached the API") }))
	s.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert}
	s.StartTLS()
	t.Cleanup(s.Close)
	cfg := supervisedConfig(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var retries int
	retry := smallBackoff()
	retry.OnRetry = func(worker.RetryObservation) { retries++ }
	if err := worker.Serve(ctx, cfg, retry); err == nil || errors.Is(err, worker.ErrSessionLost) || retries != 0 {
		t.Fatalf("server TLS refusal became a retry: retries=%d err=%v", retries, err)
	}
}

func TestServeRejectsInvalidTrustAndFallbackConfigurationBeforeRetry(t *testing.T) {
	s := tlsController(t, func(w http.ResponseWriter, r *http.Request) { t.Error("invalid configuration reached the API") })
	cfg := supervisedConfig(t, s)
	for _, fallbacks := range [][]string{{"http://secret-marker"}, {"https://secret-marker@example.com"}, {s.URL + "/"}, {"https://example.com?secret-marker"}, {"https://:8080"}, {"https://example.com:0"}, {"https://example.com:65536"}, make([]string, 8)} {
		candidate := cfg
		candidate.FallbackURLs = fallbacks
		if err := worker.Serve(context.Background(), candidate, smallBackoff()); err == nil || strings.Contains(err.Error(), "secret-marker") {
			t.Fatalf("fallback accepted or leaked: %v", err)
		}
	}
	cfg.Client = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool()}}, Timeout: time.Second}
	var retries int
	retry := smallBackoff()
	retry.OnRetry = func(worker.RetryObservation) { retries++ }
	if err := worker.Serve(context.Background(), cfg, retry); err == nil || errors.Is(err, worker.ErrSessionLost) || retries != 0 {
		t.Fatalf("TLS verification failure retried: %v", err)
	}
	for _, retry := range []worker.ReconnectConfig{{MinBackoff: -1}, {MinBackoff: time.Millisecond}, {MaxBackoff: 2 * time.Minute}, {ResetAfter: -1}} {
		if err := worker.Serve(context.Background(), cfg, retry); err == nil {
			t.Fatal("invalid retry settings accepted")
		}
	}
}

func TestLoadConfigRetainsTrustedFallbackOrigins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.json")
	data := `{"server_url":"https://member-1:8080","fallback_urls":["https://member-2:8080","https://member-3:8080"],"worker_id":"test-worker","tenant_id":"alpha","token":"` + strings.Repeat("s", 32) + `"}`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := worker.LoadConfig(path)
	if err != nil || len(cfg.FallbackURLs) != 2 || cfg.FallbackURLs[1] != "https://member-3:8080" {
		t.Fatalf("fallback configuration: %+v, %v", cfg.FallbackURLs, err)
	}
}

func TestServeDoesNotPollIfShutdownRacesWithSuccessfulRegistration(t *testing.T) {
	s := tlsController(t, func(http.ResponseWriter, *http.Request) { t.Error("mock transport reached the network") })
	cfg := supervisedConfig(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var registrations, disconnects int
	cfg.Client.Transport = responseTransport(func(r *http.Request) (*http.Response, error) {
		status := 204
		var body strings.Builder
		switch {
		case strings.HasSuffix(r.URL.Path, "/register"):
			registrations++
			cancel()
			status = 200
			_ = json.NewEncoder(&body).Encode(wire.Registered{Protocol: wire.Version, SessionID: strings.Repeat("a", 64), TenantID: "alpha", HeartbeatEvery: 20 * time.Millisecond})
		case strings.HasSuffix(r.URL.Path, "/disconnect"):
			disconnects++
		default:
			t.Error("shutdown admitted a poll, heartbeat or new session")
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body.String())), Request: r}, nil
	})
	if err := worker.Serve(ctx, cfg, smallBackoff()); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown lost cancellation: %v", err)
	}
	if registrations != 1 || disconnects != 1 {
		t.Fatalf("unexpected shutdown lifecycle: registrations=%d disconnects=%d", registrations, disconnects)
	}
}
