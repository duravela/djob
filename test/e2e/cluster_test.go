package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

type object = map[string]any
type harness struct {
	t                     *testing.T
	ctx                   context.Context
	network               *testcontainers.DockerNetwork
	raftNetwork           *testcontainers.DockerNetwork
	image                 string
	members               [3]*testcontainers.DockerContainer
	urls                  [3]string
	client                *http.Client
	tokens                map[string]string
	nextID                int
	bootstrap             *testcontainers.DockerContainer
	mqttFiles             map[string][]byte
	remote                bool
	apiCA                 []byte
	volumes               [3]string
	material              [3]map[string][]byte
	memberArgs            [3][]string
	entrypoint            string
	controllerEnv         map[string]string
	controllerArgs        []string
	controllerFiles       map[string][]byte
	controllerMemoryLimit int64
}

func newHarness(t *testing.T, company bool, setup ...func(*harness)) *harness {
	return newHarnessWithTimeout(t, company, 5*time.Minute, setup...)
}

func newHarnessWithTimeout(t *testing.T, company bool, timeout time.Duration, setup ...func(*harness)) *harness {
	t.Helper()
	if os.Getenv("DJOB_E2E_BUILD_DIR") == "" || testing.Short() {
		t.Skip("run scripts/test-e2e.sh with Docker to execute container E2E tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupNetwork(t, nw)
	h := &harness{t: t, ctx: ctx, network: nw, tokens: make(map[string]string)}
	// Build a binary-only context. Private source, local credentials and unrelated
	// workspace files are never sent to the Docker daemon.
	fixture, err := testcontainers.Run(ctx, "", testcontainers.WithDockerfile(testcontainers.FromDockerfile{Context: os.Getenv("DJOB_E2E_BUILD_DIR")}), testcontainers.WithEntrypoint("/gateway"), testcontainers.WithExposedPorts("8080/tcp"), network.WithNetwork([]string{"bootstrap-fixture"}, nw), testcontainers.WithWaitStrategy(wait.ForListeningPort("8080/tcp").SkipInternalCheck()))
	h.own(fixture, "bootstrap-fixture")
	if err != nil {
		t.Fatal(err)
	}
	info, err := fixture.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h.image = info.Config.Image
	h.bootstrap = fixture
	for _, prepare := range setup {
		prepare(h)
	}
	apiCA, peerCA := newAuthority(t, timeout+time.Hour), newAuthority(t, timeout+time.Hour)
	h.apiCA = apiCA.pem
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: apiCA.roots, ServerName: "djob-e2e", MinVersion: tls.VersionTLS12}}
	h.client = &http.Client{Transport: transport, Timeout: 4 * time.Second}
	t.Cleanup(transport.CloseIdleConnections)
	var credentials []object
	for _, actor := range []string{"platform", "monitor", "alpha-writer", "alpha-reader", "alpha-callback", "alpha-worker", "beta-writer", "beta-reader", "beta-callback", "beta-worker"} {
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			t.Fatal(err)
		}
		h.tokens[actor] = base64.RawURLEncoding.EncodeToString(secret)
		row := object{"subject": actor, "role": "operator", "token": h.tokens[actor]}
		if actor == "monitor" {
			row["role"] = "monitor"
		} else if actor != "platform" {
			parts := strings.SplitN(actor, "-", 2)
			row["tenant_id"], row["role"] = parts[0], parts[1]
		}
		credentials = append(credentials, row)
	}
	auth, _ := json.Marshal(object{"credentials": credentials})
	apiCert, apiKey := apiCA.leaf(t, "djob-e2e", "member-1", "member-2", "member-3")
	peers := "https://member-1:9021,https://member-2:9021,https://member-3:9021"
	if h.raftNetwork != nil {
		peers = "https://raft-member-1:9021,https://raft-member-2:9021,https://raft-member-3:9021"
	}
	for i := range h.members {
		alias := fmt.Sprintf("member-%d", i+1)
		peerAlias := alias
		if h.raftNetwork != nil {
			peerAlias = "raft-" + alias
		}
		cert, key := peerCA.leaf(t, peerAlias)
		material := map[string][]byte{"/auth.json": auth, "/api.pem": apiCert, "/api-key.pem": apiKey, "/ca.pem": peerCA.pem, "/peer.pem": cert, "/peer-key.pem": key}
		for path, data := range h.mqttFiles {
			material[path] = data
		}
		for path, data := range h.controllerFiles {
			material[path] = data
		}
		h.material[i] = material
		args := []string{"--id", fmt.Sprint(i + 1), "--data-dir", "/data", "--peers", peers, "--http", "0.0.0.0:8080", "--grpc", "0.0.0.0:10987", "--api-auth-file", "/auth.json", "--api-tls-cert", "/api.pem", "--api-tls-key", "/api-key.pem", "--raft-tls-ca", "/ca.pem", "--raft-tls-cert", "/peer.pem", "--raft-tls-key", "/peer-key.pem"}
		entrypoint := "/djob"
		if company {
			entrypoint = "/company-node"
		} else if h.remote {
			// These members coordinate jobs only. No company code or protocol
			// integration is enabled in the controller process.
			args = append(args, "--remote-workers", "--startup-timeout", "60s", "--jobs-operation-timeout", "2s")
		} else {
			args = append(args, "--jobs", "--modbus-tcp", "--modbus-rtu-tcp", "--startup-timeout", "60s")
			if len(h.mqttFiles) != 0 {
				args = append(args, "--jobs-mqtt", "--mqtt-profiles", "/mqtt.json", "--jobs-operation-timeout", "2s")
			}
		}
		h.memberArgs[i], h.entrypoint = args, entrypoint
		h.createMember(i)
	}
	h.startAll()
	for i := range h.members {
		url, err := h.members[i].PortEndpoint(ctx, "8080/tcp", "https")
		if err != nil {
			t.Fatal(err)
		}
		h.urls[i] = url
	}
	for i := range h.members {
		h.ready(i)
	}
	return h
}

func (h *harness) createMember(i int) {
	h.t.Helper()
	alias := fmt.Sprintf("member-%d", i+1)
	peerAlias := alias
	if h.raftNetwork != nil {
		peerAlias = "raft-" + alias
	}
	var files []testcontainers.ContainerFile
	dir := h.t.TempDir()
	for path, data := range h.material[i] {
		source := filepath.Join(dir, filepath.Base(path))
		if err := os.WriteFile(source, data, 0600); err != nil {
			h.t.Fatal(err)
		}
		files = append(files, testcontainers.ContainerFile{HostFilePath: source, ContainerFilePath: path, FileMode: 0600})
	}
	options := []testcontainers.ContainerCustomizer{testcontainers.WithNoStart(), testcontainers.WithEntrypoint(h.entrypoint), testcontainers.WithCmd(append(append([]string{}, h.memberArgs[i]...), h.controllerArgs...)...), testcontainers.WithEnv(h.controllerEnv), testcontainers.WithExposedPorts("8080/tcp"), testcontainers.WithFiles(files...), network.WithNetwork([]string{alias}, h.network), testcontainers.WithWaitStrategy(wait.ForListeningPort("8080/tcp").SkipInternalCheck().WithStartupTimeout(time.Minute))}
	if h.controllerMemoryLimit > 0 {
		options = append(options, testcontainers.WithHostConfigModifier(func(config *dockercontainer.HostConfig) {
			config.Memory = h.controllerMemoryLimit
			config.MemorySwap = h.controllerMemoryLimit
		}))
	}
	if h.raftNetwork != nil {
		options = append(options, network.WithNetwork([]string{peerAlias}, h.raftNetwork), testcontainers.WithEndpointSettingsModifier(func(settings map[string]*dockernetwork.EndpointSettings) {
			// Prefer the application gateway where Docker supports priority.
			// Older daemons can remap native published ports; partition helpers
			// refresh those URLs while internal API/worker paths stay attached.
			settings[h.network.Name].GwPriority = 1
		}))
	}
	if h.volumes[i] != "" {
		options = append(options, testcontainers.WithMounts(testcontainers.VolumeMount(h.volumes[i], "/data")))
	}
	member, err := testcontainers.Run(h.ctx, h.image, options...)
	h.own(member, alias)
	if err != nil {
		h.t.Fatal(err)
	}
	h.members[i] = member
}

func (h *harness) own(c testcontainers.Container, name string) {
	h.t.Helper()
	if c == nil {
		return
	}
	testcontainers.CleanupContainer(h.t, c)
	h.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		reader, err := c.Logs(ctx)
		if err != nil {
			h.t.Logf("%s logs: %v", name, err)
			return
		}
		defer reader.Close()
		data, _ := io.ReadAll(io.LimitReader(reader, 4<<20))
		if dir := os.Getenv("DJOB_TEST_ARTIFACT_DIR"); dir != "" {
			if err := os.MkdirAll(dir, 0700); err != nil {
				h.t.Error(err)
			} else if err := os.WriteFile(filepath.Join(dir, strings.ReplaceAll(h.t.Name(), "/", "-")+"-"+name+".log"), data, 0600); err != nil {
				h.t.Error(err)
			}
		}
		if h.t.Failed() {
			if len(data) > 6000 {
				data = data[len(data)-6000:]
			}
			h.t.Logf("%s:\n%s", name, data)
		}
	})
}

func (h *harness) startAll() {
	h.t.Helper()
	errors := make(chan error, 3)
	for _, member := range h.members {
		go func(c *testcontainers.DockerContainer) { errors <- c.Start(h.ctx) }(member)
	}
	for range h.members {
		if err := <-errors; err != nil {
			h.t.Error(err)
		}
	}
	if h.t.Failed() {
		h.t.FailNow()
	}
	for i := range h.members {
		h.refreshURL(i)
	}
}

func (h *harness) refreshURL(i int) {
	h.t.Helper()
	url, err := h.members[i].PortEndpoint(h.ctx, "8080/tcp", "https")
	if err != nil {
		h.t.Fatal(err)
	}
	h.urls[i] = url
	// Older Docker daemons can remap automatically published ports when the
	// default network endpoint changes. Drop obsolete native HTTP idle sockets.
	if h.client != nil {
		h.client.CloseIdleConnections()
	}
}

func poll(t *testing.T, why string, probe func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		ok, err := probe()
		if ok && err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %v", why, err)
		}
		time.Sleep(40 * time.Millisecond)
	}
}

func encode(value any) string {
	data, _ := json.Marshal(value)
	return base64.StdEncoding.EncodeToString(data)
}

func (h *harness) call(i int, method, path, actor string, body any) (int, object, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}
	return h.callBytes(h.ctx, i, method, path, actor, data)
}

func (h *harness) callBytes(ctx context.Context, i int, method, path, actor string, data []byte) (int, object, error) {
	req, err := http.NewRequestWithContext(ctx, method, h.urls[i]+path, bytes.NewReader(data))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if actor != "" {
		req.Header.Set("Authorization", "Bearer "+h.tokens[actor])
	}
	response, err := h.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	var result object
	err = json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&result)
	return response.StatusCode, result, err
}

func (h *harness) request(i int, method, path, actor string, body any, want int) object {
	h.t.Helper()
	status, result, err := h.call(i, method, path, actor, body)
	if err != nil || status != want {
		h.t.Fatalf("member %d %s %s: status %d, error %v, result %v", i+1, method, path, status, err, result)
	}
	return result
}

func (h *harness) meta() object {
	h.nextID++
	return object{"request_id": fmt.Sprintf("container-test-%d", h.nextID), "at": time.Now().UTC().Format(time.RFC3339Nano)}
}

func (h *harness) mutate(i int, actor, tenant, operation string, value any) object {
	h.t.Helper()
	body := object{"command": h.meta(), operation: value}
	if tenant != "" {
		body["tenant_id"] = tenant
	}
	return h.request(i, "POST", "/api/jobs/v1/mutate", actor, body, 200)
}

func (h *harness) ready(i int) {
	h.t.Helper()
	poll(h.t, "member quorum readiness", func() (bool, error) {
		status, result, err := h.call(i, "GET", "/readyz", "", nil)
		return status == 200 && result["ready"] == true, err
	})
}

func (h *harness) run(i int, tenant, id, want string) object {
	h.t.Helper()
	var run object
	poll(h.t, id+" becomes "+want, func() (bool, error) {
		status, result, err := h.call(i, "POST", "/api/jobs/v1/get", tenant+"-reader", object{"reference": object{"kind": "RUN", "id": id}})
		if err != nil || status != 200 {
			return false, fmt.Errorf("read run: status %d, error %v", status, err)
		}
		run, _ = result["run"].(map[string]any)
		if run["status"] == want {
			return true, nil
		}
		return false, fmt.Errorf("observed state %v, error %v, attempt %v", run["status"], run["error"], run["last_attempt_id"])
	})
	return run
}

func (h *harness) definition(tenant, id, handler, binding string, input any) {
	h.t.Helper()
	definition := object{"id": id, "runtime": "steps/v1", "handler": handler, "handler_version": "1", "binding_id": binding}
	if input != nil {
		definition["input"] = encode(input)
	}
	h.mutate(0, tenant+"-writer", "", "put_definition", object{"definition": definition})
}

func (h *harness) launch(i int, tenant, definition, id string) {
	h.t.Helper()
	h.mutate(i, tenant+"-writer", "", "launch", object{"definition_id": definition, "definition_revision": "1", "run_id": id})
}
