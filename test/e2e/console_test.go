package e2e_test

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestManagementConsole(t *testing.T)               { testManagementConsole(t, false) }
func TestManagementConsoleSharedSessions(t *testing.T) { testManagementConsole(t, true) }

func testManagementConsole(t *testing.T, shared bool) {
	h := newHarness(t, false, func(h *harness) {
		h.remote = true
		if shared {
			h.controllerArgs = append(h.controllerArgs, "--console-sessions")
		}
	})
	if shared {
		h.request(0, "POST", "/api/jobs/v1/console-sessions/activate", "platform", object{"Policy": object{"MaxRecords": 128, "MaxCiphertextBytes": 64 << 20, "Limits": object{"MaxCiphertextBytes": 4 << 20, "MaxLifetime": int64(24 * time.Hour)}}}, 200)
	}
	h.mutate(0, "platform", "alpha", "put_tenant", object{"max_running": "2"})
	h.mutate(0, "platform", "beta", "put_tenant", object{"max_running": "2"})
	h.startWorker("alpha", "console-alpha-worker", "/djob-worker", 0)
	h.startWorker("beta", "private-beta-worker", "/djob-worker", 1)
	ca := newAuthority(t, time.Hour)
	cert, key := ca.leaf(t, "console-e2e")
	var files []testcontainers.ContainerFile
	for name, data := range map[string][]byte{"console.pem": cert, "console-key.pem": key, "controller-ca.pem": h.apiCA} {
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		files = append(files, testcontainers.ContainerFile{HostFilePath: path, ContainerFilePath: "/" + name, FileMode: 0600})
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca.roots, ServerName: "console-e2e", MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}
	waitConsole := func(endpoint string) {
		// A published Docker port may accept TCP before its backend is ready
		// for TLS. Require the actual console readiness response before tests.
		poll(t, "console HTTPS readiness", func() (bool, error) {
			request, err := http.NewRequestWithContext(h.ctx, http.MethodGet, endpoint+"/readyz", nil)
			if err != nil {
				return false, err
			}
			request.Host = "console-e2e"
			response, err := client.Do(request)
			if err != nil {
				return false, err
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				return false, fmt.Errorf("readiness status %d", response.StatusCode)
			}
			return true, nil
		})
	}
	startConsole := func(name string) (testcontainers.Container, string) {
		t.Helper()
		args := []string{"--controllers", "https://member-1:8080,https://member-2:8080,https://member-3:8080", "--controller-tls-ca", "/controller-ca.pem", "--listen", "0.0.0.0:8090", "--public-url", "https://console-e2e", "--tls-cert", "/console.pem", "--tls-key", "/console-key.pem"}
		if shared {
			args = append(args, "--shared-sessions")
		}
		container, err := testcontainers.Run(h.ctx, h.image, testcontainers.WithEntrypoint("/djob-console"), testcontainers.WithCmd(args...), testcontainers.WithFiles(files...), testcontainers.WithExposedPorts("8090/tcp"), network.WithNetwork([]string{name}, h.network), testcontainers.WithWaitStrategy(wait.ForListeningPort("8090/tcp").SkipInternalCheck()))
		h.own(container, name)
		if err != nil {
			t.Fatal(err)
		}
		endpoint, err := container.PortEndpoint(h.ctx, "8090/tcp", "https")
		if err != nil {
			t.Fatal(err)
		}
		waitConsole(endpoint)
		return container, endpoint
	}
	container, url := startConsole("console")
	var second testcontainers.Container
	var secondURL string
	if shared {
		second, secondURL = startConsole("console-second")
	}
	var err error
	for path, marker := range map[string]string{"/forms.js": "export function buildCompanyMutation", "/references.js": "export class ReleaseReferences", "/recovery.js": "export function buildRecoveryMutation", "/routing.js": "export function readRoute", "/usage.js": "export class UsageInspector", "/enrollments.js": "export function enrollmentRecord"} {
		asset, err := http.NewRequestWithContext(h.ctx, http.MethodGet, url+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		asset.Host = "console-e2e"
		module, err := client.Do(asset)
		if err != nil {
			t.Fatal(err)
		}
		moduleBytes, readErr := io.ReadAll(io.LimitReader(module.Body, 128<<10))
		_ = module.Body.Close()
		if readErr != nil || module.StatusCode != 200 || module.Header.Get("Content-Type") != "text/javascript; charset=utf-8" || module.Header.Get("Cache-Control") != "no-store" || !bytes.Contains(moduleBytes, []byte(marker)) {
			t.Fatalf("packaged console %s unavailable: status=%d read=%v", path, module.StatusCode, readErr)
		}
	}
	var cookie *http.Cookie
	var csrf string
	var nextConsoleCall time.Time
	call := func(method, path string, body any, want int) object {
		t.Helper()
		if shared {
			if delay := time.Until(nextConsoleCall); delay > 0 {
				time.Sleep(delay)
			}
			nextConsoleCall = time.Now().Add(100 * time.Millisecond)
		}
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r, err := http.NewRequestWithContext(h.ctx, method, url+"/api/console/v1/"+path, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		r.Host = "console-e2e"
		r.Header.Set("Origin", "https://console-e2e")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Djob-CSRF", csrf)
		if cookie != nil {
			r.AddCookie(cookie)
		}
		response, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ = io.ReadAll(io.LimitReader(response.Body, 8<<20))
		if response.StatusCode != want {
			t.Fatalf("console %s: HTTP %d, want %d: %s", path, response.StatusCode, want, data)
		}
		for _, c := range response.Cookies() {
			cookie = c
		}
		var result object
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	login := func(actor string) {
		t.Helper()
		result := call(http.MethodPost, "session", object{"token": h.tokens[actor]}, 200)
		csrf = result["csrf"].(string)
		if cookie == nil || !cookie.Secure || !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Name != "__Host-djob-console" {
			t.Fatal("console did not issue a protected browser session")
		}
	}
	swappedReview, swappedConfig := false, false
	mutate := func(operation string, value any) object {
		t.Helper()
		pending := call(http.MethodPost, "mutations", object{"tenant_id": "alpha", operation: value}, 200)
		id := pending["request_id"].(string)
		stoppedLeader := -1
		if shared && !swappedReview {
			if err := container.Stop(h.ctx, nil); err != nil {
				t.Fatal(err)
			}
			url = secondURL
			transport.CloseIdleConnections()
			swappedReview = true
			stoppedLeader = h.leader()
			h.stopMember(stoppedLeader)
			h.ready((stoppedLeader + 1) % len(h.members))
		}
		result := call(http.MethodPost, "mutations/commit", object{"request_id": id}, 200)
		if result["state"] != "confirmed" {
			t.Fatalf("console mutation not confirmed: %v", result)
		}
		if stoppedLeader >= 0 {
			if err := h.members[stoppedLeader].Start(h.ctx); err != nil {
				t.Fatal(err)
			}
			h.refreshURL(stoppedLeader)
			h.ready(stoppedLeader)
		}
		duplicate := call(http.MethodPost, "mutations/commit", object{"request_id": id}, 200)
		if fmt.Sprint(duplicate) != fmt.Sprint(result) {
			t.Fatal("duplicate console commit changed its acknowledged result")
		}
		call(http.MethodDelete, "mutations/current", object{"request_id": id}, 200)
		return result
	}
	login("platform")
	mutate("put_limits", object{"limits": object{"max_pending_runs": "8", "max_stored_runs": "64", "max_inbox_bytes": "4096"}})
	mutate("put_configuration_limits", object{"limits": object{"max_definitions": "8", "max_triggers": "8", "max_payload_bytes": "4096"}})
	// A custom company handler stays queued until a worker registers with a
	// dynamically enrolled credential. Neither controller nor console is rebuilt.
	endpoint, gateway := h.gateway(false)
	h.configure("enrollment-bus", "modbus-tcp", endpoint, "acme.meter", "measure")
	h.definition("alpha", "enrolled-job", "acme.measure-twice", "enrollment-bus", object{"maximum": 100})
	h.launch(0, "alpha", "enrolled-job", "enrolled-first")
	h.run(0, "alpha", "enrolled-first", "ready")
	if gateway.call("GET", "/stats")["requests"] != float64(0) {
		t.Fatal("custom job executed before its company worker registered")
	}
	manageWorker := func(action, revision string) object {
		t.Helper()
		q := object{"tenant_id": "alpha", "worker_id": "enrolled-company", "action": action, "expected_revision": revision, "reason": "company rollout"}
		if action == "enroll" {
			q["subject"] = "company/measurement"
		}
		pending := call(http.MethodPost, "mutations/worker-enrollment", q, 200)
		id := object{"request_id": pending["request_id"]}
		result := call(http.MethodPost, "mutations/commit", id, 200)
		if result["state"] != "confirmed" {
			t.Fatalf("enrollment action %s failed: %v", action, result)
		}
		var config object
		if action == "enroll" || action == "rotate" {
			if shared && !swappedConfig {
				if err := container.Start(h.ctx); err != nil {
					t.Fatal(err)
				}
				url, err = container.PortEndpoint(h.ctx, "8090/tcp", "https")
				if err != nil {
					t.Fatal(err)
				}
				if err := second.Stop(h.ctx, nil); err != nil {
					t.Fatal(err)
				}
				transport.CloseIdleConnections()
				swappedConfig = true
			}
			config = call(http.MethodPost, "mutations/worker-config", id, 200)
			if config["transport"] != "grpc" {
				t.Fatal("downloaded worker configuration must explicitly select gRPC")
			}
			token, ok := config["token"].(string)
			if !ok || !strings.HasPrefix(token, "djobw1.") {
				t.Fatal("missing private worker credential")
			}
			serialized, _ := json.Marshal(result)
			if bytes.Contains(serialized, []byte(token)) {
				t.Fatal("raw worker credential leaked into mutation review")
			}
			config["ca_file"] = "/api-ca.pem"
		}
		call(http.MethodDelete, "mutations/current", id, 200)
		return config
	}
	settings := manageWorker("enroll", "0")
	settings["server_url"], settings["fallback_urls"] = "https://member-2:8080", []string{"https://member-1:8080", "https://member-3:8080"}
	h.startWorkerConfig("enrolled-deployment-one", "/company-worker", settings)
	waiting := h.run(1, "alpha", "enrolled-first", "waiting")
	if !strings.Contains(waiting["owner_id"].(string), "remote-worker/2/enrolled-company/") {
		t.Fatal("enrollment was not usable on another controller")
	}
	manageWorker("drain", "1")
	h.mutate(2, "alpha-callback", "", "signal", object{"run_id": "enrolled-first", "event_id": "continue", "key": "continue"})
	h.run(1, "alpha", "enrolled-first", "succeeded")
	manageWorker("resume", "2")
	settings = manageWorker("rotate", "3")
	settings["server_url"], settings["fallback_urls"] = "https://member-3:8080", []string{"https://member-1:8080", "https://member-2:8080"}
	h.startWorkerConfig("enrolled-deployment-two", "/company-worker", settings)
	h.launch(0, "alpha", "enrolled-job", "enrolled-second")
	h.run(2, "alpha", "enrolled-second", "waiting")
	manageWorker("revoke", "4")
	h.mutate(0, "alpha-callback", "", "signal", object{"run_id": "enrolled-second", "event_id": "continue", "key": "continue"})
	h.run(2, "alpha", "enrolled-second", "succeeded")
	h.launch(0, "alpha", "enrolled-job", "enrolled-revoked")
	h.run(0, "alpha", "enrolled-revoked", "ready")
	if gateway.call("GET", "/stats")["requests"] != float64(4) {
		t.Fatal("enrollment lifecycle lost or repeated company device effects")
	}
	if err := h.members[1].Stop(h.ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.members[1].Start(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.refreshURL(1)
	h.ready(1)
	value := h.request(1, http.MethodPost, "/api/jobs/v1/worker-enrollments/get", "platform", object{"tenant_id": "alpha", "worker_id": "enrolled-company"}, 200)
	if value["state"] != "revoked" || value["revision"] != "5" {
		t.Fatalf("enrollment did not survive controller restart: %v", value)
	}
	call(http.MethodDelete, "session", object{}, 200)
	cookie, csrf = nil, ""
	login("alpha-writer")
	call(http.MethodPost, "mutations", object{"tenant_id": "alpha", "put_limits": object{"limits": object{}}}, 403)
	call(http.MethodPost, "query/list", object{"kind": "RUN", "tenant_id": "beta"}, 403)
	call(http.MethodPost, "workers", object{"tenant_id": "beta"}, 403)
	poll(t, "console observes tenant-scoped independent worker code", func() (bool, error) {
		result := call(http.MethodPost, "workers", object{"tenant_id": "alpha"}, 200)
		data, _ := json.Marshal(result)
		if bytes.Contains(data, []byte("private-beta-worker")) {
			t.Fatal("worker discovery crossed tenant scope")
		}
		controllers := result["controllers"].([]any)
		if len(controllers) != 3 {
			t.Fatalf("expected observations from three configured controllers: %s", data)
		}
		workers := 0
		stillDraining := false
		for _, source := range controllers {
			source := source.(map[string]any)
			v, ok := source["inventory"].(map[string]any)
			if !ok {
				t.Fatalf("controller metadata unavailable: %s", data)
			}
			for _, info := range v["workers"].([]any) {
				w := info.(map[string]any)
				if w["worker_id"] == "enrolled-company" {
					stillDraining = true
					continue
				}
				if w["worker_id"] != "console-alpha-worker" || w["tenant_id"] != "alpha" || w["mode"] != "remote" {
					t.Fatalf("unexpected worker observation: %s", data)
				}
				workers++
			}
		}
		return !stillDraining && workers == 1 && bytes.Contains(data, []byte(`"name":"sha256","version":"1"`)), nil
	})
	mutate("put_definition", object{"definition": object{"id": "console-hash", "runtime": "steps/v1", "handler": "sha256", "handler_version": "1", "input": "SGVsbG8="}})
	mutate("launch", object{"definition_id": "console-hash", "definition_revision": "1", "run_id": "console-immediate"})
	deadline := time.Now().Add(10 * time.Second)
	for {
		r := call(http.MethodPost, "query/get", object{"reference": object{"kind": "RUN", "tenant_id": "alpha", "id": "console-immediate"}}, 200)
		if r["run"].(map[string]any)["status"] == "succeeded" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("console-launched compute job did not finish")
		}
		delay := 25 * time.Millisecond
		if shared {
			delay = 150 * time.Millisecond
		}
		time.Sleep(delay)
	}
	mutate("put_trigger", object{"definition": object{"id": "console-once", "definition_id": "console-hash", "definition_revision": "1", "max_pending": 1, "timing": object{"kind": "once", "start_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), "missed_policy": "coalesce"}}})
	mutate("pause_trigger", object{"id": "console-once", "expected_revision": "1", "paused": true})
	mutate("pause_trigger", object{"id": "console-once", "expected_revision": "2", "paused": false})
	call(http.MethodDelete, "session", object{}, 200)
	cookie, csrf = nil, ""
	login("alpha-reader")
	for _, kind := range []string{"LIMITS", "USAGE", "CONFIGURATION_LIMITS", "CONFIGURATION_USAGE"} {
		call(http.MethodPost, "query/get", object{"reference": object{"kind": kind, "tenant_id": "alpha"}}, 200)
		call(http.MethodPost, "query/get", object{"reference": object{"kind": kind, "tenant_id": "beta"}}, 403)
	}
	call(http.MethodPost, "workers", object{"tenant_id": "alpha"}, 200)
	call(http.MethodPost, "query/list", object{"kind": "DEFINITION", "tenant_id": "alpha", "page_size": 32}, 200)
	for _, kind := range []string{"ATTEMPT", "ACTION", "EVENT"} {
		page := call(http.MethodPost, "query/list", object{"kind": kind, "tenant_id": "alpha", "run_id": "console-immediate", "page_size": 32}, 200)
		records := page["records"].([]any)
		if kind == "ATTEMPT" && len(records) == 0 {
			t.Fatal("completed independent work has no attempt history")
		}
		for _, record := range records {
			ref := record.(map[string]any)["reference"].(map[string]any)
			if ref["tenant_id"] != "alpha" || ref["run_id"] != "console-immediate" || ref["kind"] != kind {
				t.Fatalf("history record escaped its run scope: %v", ref)
			}
			call(http.MethodPost, "query/get", object{"reference": ref}, 200)
		}
		call(http.MethodPost, "query/list", object{"kind": kind, "tenant_id": "beta", "run_id": "console-immediate", "page_size": 32}, 403)
	}
	call(http.MethodPost, "mutations", object{"tenant_id": "alpha", "launch": object{"definition_id": "console-hash", "definition_revision": "1"}}, 403)
	if err := container.Stop(h.ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := container.Start(h.ctx); err != nil {
		t.Fatal(err)
	}
	url, err = container.PortEndpoint(h.ctx, "8090/tcp", "https")
	if err != nil {
		t.Fatal(err)
	}
	transport.CloseIdleConnections()
	waitConsole(url)
	if shared {
		call(http.MethodGet, "session", nil, 200)
	} else {
		call(http.MethodGet, "session", nil, 401)
	}
	login("alpha-reader")
	call(http.MethodPost, "query/get", object{"reference": object{"kind": "RUN", "tenant_id": "alpha", "id": "console-immediate"}}, 200)
	if err := h.members[2].Stop(h.ctx, nil); err != nil {
		t.Fatal(err)
	}
	partial := call(http.MethodPost, "workers", object{"tenant_id": "alpha"}, 200)
	sources := partial["controllers"].([]any)
	if len(sources) != 3 || sources[2].(map[string]any)["error"] == nil || sources[0].(map[string]any)["inventory"] == nil {
		t.Fatalf("unreachable controller did not remain an explicit partial observation: %v", partial)
	}
	// The console lifecycle changes sessions, never the controller-owned run.
}
