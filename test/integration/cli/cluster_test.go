package cli_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/node/util/testutil"
)

type object = map[string]any

type member struct {
	cmd        *exec.Cmd
	done       chan struct{}
	exit       error // Published by closing done.
	url        string
	log        *os.File
	logPath    string
	generation int
}

type cluster struct {
	t           *testing.T
	ctx         context.Context
	cancel      context.CancelFunc
	binary      string
	root        string
	logs        string
	peers       []string
	tls         []*base.RaftTLSConfig
	ca          *testutil.PeerCA
	members     [3]*member
	tokens      map[string]string
	auth        string
	client      *http.Client
	nextID      int
	pollTimeout time.Duration
}

var readyLine = regexp.MustCompile(`HTTP (http://127\.0\.0\.1:[0-9]+),`)

func cliBinary(t *testing.T) string {
	t.Helper()
	if path := os.Getenv("DJOB_TEST_BINARY"); path != "" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			t.Fatal(err)
		}
		return absolute
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repository go.work not found")
		}
		dir = parent
	}
	path := filepath.Join(t.TempDir(), "djob")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-o", path, "./cmd/djob")
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	return path
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	if testing.Short() {
		t.Skip("three-process CLI fault test")
	}
	return newClusterWithBinary(t, cliBinary(t))
}

func newClusterWithBinary(t *testing.T, binary string) *cluster {
	t.Helper()
	return newClusterInDir(t, binary, t.TempDir())
}

// Hardware qualification retains member state for offline investigation and
// recovery. Ordinary integration tests continue to use automatically removed dirs.
func newClusterInDir(t *testing.T, binary, root string) *cluster {
	t.Helper()
	if testing.Short() {
		t.Skip("three-process CLI test")
	}
	c := &cluster{t: t, binary: binary, root: root, tokens: make(map[string]string), client: &http.Client{Timeout: 3 * time.Second}}
	// Leave time to reap child processes before go test's hard timeout exits.
	if deadline, ok := t.Deadline(); ok {
		c.ctx, c.cancel = context.WithDeadline(context.Background(), deadline.Add(-5*time.Second))
	} else {
		c.ctx, c.cancel = context.WithCancel(context.Background())
	}
	c.logs = filepath.Join(c.root, "logs")
	if dir := os.Getenv("DJOB_TEST_ARTIFACT_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		var err error
		c.logs, err = os.MkdirTemp(dir, "cli-cluster-")
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(c.logs, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer c.cancel()
		for i := range c.members {
			c.stop(i, true)
		}
		c.client.CloseIdleConnections()
		files, _ := filepath.Glob(filepath.Join(c.logs, "*.log"))
		for _, path := range files {
			data, _ := os.ReadFile(path)
			if bytes.Contains(data, []byte("WARNING: DATA RACE")) {
				t.Errorf("child process race: %s", filepath.Base(path))
			}
			if t.Failed() {
				if len(data) > 4096 {
					data = data[len(data)-4096:]
				}
				t.Logf("%s:\n%s", filepath.Base(path), data)
			}
		}
	})
	credentials := []object{}
	for _, actor := range []string{"platform", "alpha-writer", "alpha-reader", "alpha-callback", "beta-writer", "beta-reader"} {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			t.Fatal(err)
		}
		c.tokens[actor] = base64.RawURLEncoding.EncodeToString(key)
		row := object{"subject": actor, "role": "operator", "token": c.tokens[actor]}
		if actor != "platform" {
			parts := strings.SplitN(actor, "-", 2)
			row["tenant_id"], row["role"] = parts[0], parts[1]
		}
		credentials = append(credentials, row)
	}
	c.auth = filepath.Join(c.root, "auth.json")
	writeJSON(t, c.auth, object{"credentials": credentials})
	c.ca = testutil.NewPeerCA(t)
	// Keep the credentials next to retained hardware member state. The shared
	// TLS helper's temporary directories disappear when the test returns.
	tlsDir := filepath.Join(c.root, "peer-tls")
	if err := os.MkdirAll(tlsDir, 0700); err != nil {
		t.Fatal(err)
	}
	copyCredential := func(source, name string) string {
		t.Helper()
		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(tlsDir, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	c.ca.File = copyCredential(c.ca.File, "ca.pem")
	// Reserve all peer ports together so the OS cannot reuse a just-closed port
	// for another member. CLI binding still requires releasing them before start.
	var reservations []net.Listener
	defer func() {
		for _, ln := range reservations {
			_ = ln.Close()
		}
	}()
	for range 3 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, held := range reservations {
				_ = held.Close()
			}
			t.Fatal(err)
		}
		reservations = append(reservations, ln)
		c.peers = append(c.peers, "https://"+ln.Addr().String())
		peerTLS := c.ca.Member(t, []string{"127.0.0.1"})
		memberID := len(c.tls) + 1
		peerTLS.CertFile = copyCredential(peerTLS.CertFile, fmt.Sprintf("member-%d.pem", memberID))
		peerTLS.KeyFile = copyCredential(peerTLS.KeyFile, fmt.Sprintf("member-%d-key.pem", memberID))
		c.tls = append(c.tls, peerTLS)
	}
	for _, ln := range reservations {
		_ = ln.Close()
	}
	for i := range c.members {
		c.start(i)
	}
	for i := range c.members {
		c.waitStarted(i)
		c.ready(i)
	}
	return c
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func (c *cluster) start(i int) {
	c.t.Helper()
	generation := 1
	if old := c.members[i]; old != nil {
		select {
		case <-old.done:
		default:
			c.t.Fatalf("member %d is still running", i+1)
		}
		generation = old.generation + 1
	}
	path := filepath.Join(c.logs, fmt.Sprintf("member-%d-%d.log", i+1, generation))
	log, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		c.t.Fatal(err)
	}
	peerTLS := c.tls[i]
	args := []string{"--id", strconv.Itoa(i + 1), "--data-dir", c.dataDir(i), "--peers", strings.Join(c.peers, ","),
		"--http", "127.0.0.1:0", "--grpc", "127.0.0.1:0", "--api-auth-file", c.auth,
		"--raft-tls-ca", peerTLS.CAFile, "--raft-tls-cert", peerTLS.CertFile, "--raft-tls-key", peerTLS.KeyFile,
		"--jobs", "--jobs-http", "--modbus-tcp", "--modbus-rtu-tcp", "--jobs-concurrency", "4", "--jobs-per-tenant", "2",
		"--startup-timeout", "20s", "--shutdown-timeout", "5s", "--readiness-timeout", "250ms",
		"--queue-sample-interval", "100ms", "--queue-sample-limit", "1", "--state-sample-interval", "100ms", "--state-sample-limit", "64", "--storage-sample-interval", "100ms", "--clock-sample-interval", "50ms"}
	writeJSON(c.t, filepath.Join(c.root, fmt.Sprintf("member-%d-start.json", i+1)), object{"binary": c.binary, "args": args})
	m := &member{cmd: exec.CommandContext(c.ctx, c.binary, args...), done: make(chan struct{}), log: log, logPath: path, generation: generation}
	m.cmd.Stdout, m.cmd.Stderr = log, log
	m.cmd.Env = append(os.Environ(), "GORACE=atexit_sleep_ms=0")
	if err := m.cmd.Start(); err != nil {
		_ = log.Close()
		c.t.Fatalf("start member %d: %v", i+1, err)
	}
	c.members[i] = m
	go func() { m.exit = m.cmd.Wait(); close(m.done) }()
}

func (c *cluster) dataDir(i int) string { return filepath.Join(c.root, fmt.Sprintf("node-%d", i+1)) }

func (c *cluster) stop(i int, kill bool) {
	c.t.Helper()
	m := c.members[i]
	if m == nil {
		return
	}
	select {
	case <-m.done:
		if !kill && m.exit != nil {
			c.t.Errorf("member %d exited unexpectedly: %v (log %s)", i+1, m.exit, m.logPath)
		}
		_ = m.log.Close()
		return
	default:
	}
	if kill {
		_ = m.cmd.Process.Kill()
	} else {
		_ = m.cmd.Process.Signal(os.Interrupt)
	}
	select {
	case <-m.done:
		if !kill && m.exit != nil {
			c.t.Errorf("member %d shutdown: %v (log %s)", i+1, m.exit, m.logPath)
		}
	case <-time.After(8 * time.Second):
		_ = m.cmd.Process.Kill()
		<-m.done
		c.t.Errorf("member %d did not stop within its shutdown budget", i+1)
	}
	_ = m.log.Close()
}

func eventually(t *testing.T, why string, probe func() (bool, error)) {
	t.Helper()
	eventuallyWithin(t, 15*time.Second, why, probe)
}

func (c *cluster) eventually(why string, probe func() (bool, error)) {
	c.t.Helper()
	timeout := c.pollTimeout
	if timeout == 0 {
		timeout = 15 * time.Second
	}
	eventuallyWithin(c.t, timeout, why, probe)
}

func eventuallyWithin(t *testing.T, timeout time.Duration, why string, probe func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for {
		ok, err := probe()
		if ok && err == nil {
			return
		}
		last = err
		if time.Now().After(deadline) {
			t.Fatalf("%s: %v", why, last)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (c *cluster) waitStarted(i int) {
	c.t.Helper()
	m := c.members[i]
	eventually(c.t, fmt.Sprintf("member %d startup", i+1), func() (bool, error) {
		select {
		case <-m.done:
			c.t.Fatalf("member %d exited during startup: %v", i+1, m.exit)
		default:
		}
		data, err := os.ReadFile(m.logPath)
		if err != nil {
			return false, err
		}
		match := readyLine.FindSubmatch(data)
		if match == nil {
			return false, nil
		}
		m.url = string(match[1])
		return true, nil
	})
}

func (c *cluster) call(i int, method, path, actor string, body any) (int, []byte, error) {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
	}
	request, err := http.NewRequest(method, c.members[i].url+path, bytes.NewReader(data))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	if actor != "" {
		request.Header.Set("Authorization", "Bearer "+c.tokens[actor])
	}
	response, err := c.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err = io.ReadAll(io.LimitReader(response.Body, 2<<20))
	return response.StatusCode, data, err
}

func (c *cluster) request(i int, method, path, actor string, body any, status int) object {
	c.t.Helper()
	code, data, err := c.call(i, method, path, actor, body)
	if err != nil || code != status {
		c.t.Fatalf("member %d %s %s: status %d, error %v, body %s", i+1, method, path, code, err, data)
	}
	var result object
	if len(data) != 0 {
		if err := json.Unmarshal(data, &result); err != nil {
			c.t.Fatal(err)
		}
	}
	return result
}

func (c *cluster) ready(i int) {
	c.t.Helper()
	eventually(c.t, fmt.Sprintf("member %d quorum readiness", i+1), func() (bool, error) {
		code, data, err := c.call(i, "GET", "/readyz", "", nil)
		if err != nil || code != 200 {
			return false, err
		}
		var result struct {
			Ready     bool
			Confirmed uint64 `json:"confirmed_index,string"`
			Applied   uint64 `json:"applied_index,string"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return false, err
		}
		return result.Ready && result.Applied >= result.Confirmed, nil
	})
}

func (c *cluster) leader(exclude int) int {
	c.t.Helper()
	leader := -1
	eventually(c.t, "leader election", func() (bool, error) {
		leaders := []int{}
		for i, m := range c.members {
			if i == exclude || m == nil {
				continue
			}
			select {
			case <-m.done:
				continue
			default:
			}
			code, data, err := c.call(i, "GET", "/healthz", "", nil)
			if err != nil || code != 200 {
				continue
			}
			var state struct{ Leader bool }
			if err := json.Unmarshal(data, &state); err == nil && state.Leader {
				leaders = append(leaders, i)
			}
		}
		if len(leaders) != 1 {
			return false, nil
		}
		leader = leaders[0]
		return true, nil
	})
	return leader
}

func (c *cluster) meta() object {
	c.nextID++
	return object{"request_id": fmt.Sprintf("cli-test-%d", c.nextID), "at": time.Now().UTC().Format(time.RFC3339Nano)}
}

func (c *cluster) mutate(i int, actor, tenant, operation string, value any) object {
	body := object{"command": c.meta(), operation: value}
	if tenant != "" {
		body["tenant_id"] = tenant
	}
	return c.request(i, "POST", "/api/jobs/v1/mutate", actor, body, 200)
}

func encoded(value any) string {
	if data, ok := value.([]byte); ok {
		return base64.StdEncoding.EncodeToString(data)
	}
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(data)
}

func (c *cluster) definition(i int, tenant, id, handler, binding string, input any) {
	c.t.Helper()
	definition := object{"id": id, "runtime": "steps/v1", "handler": handler, "handler_version": "1", "binding_id": binding}
	if input != nil {
		definition["input"] = encoded(input)
	}
	c.mutate(i, tenant+"-writer", "", "put_definition", object{"definition": definition})
}

func (c *cluster) launch(i int, tenant, definition, id string) object {
	c.t.Helper()
	return c.mutate(i, tenant+"-writer", "", "launch", object{"definition_id": definition, "definition_revision": "1", "run_id": id})
}

func (c *cluster) run(i int, tenant, id string) object {
	c.t.Helper()
	return c.request(i, "POST", "/api/jobs/v1/get", tenant+"-reader", object{"reference": object{"kind": "RUN", "id": id}}, 200)["run"].(map[string]any)
}

func (c *cluster) awaitRun(i int, tenant, id, status string) object {
	c.t.Helper()
	var result object
	c.eventually(id+" becomes "+status, func() (bool, error) {
		code, data, err := c.call(i, "POST", "/api/jobs/v1/get", tenant+"-reader", object{"reference": object{"kind": "RUN", "id": id}})
		if err != nil || code != 200 {
			return false, err
		}
		var response struct{ Run object }
		if err := json.Unmarshal(data, &response); err != nil {
			return false, err
		}
		result = response.Run
		if state, _ := result["status"].(string); state != status && (state == "failed" || state == "unknown" || state == "cancelled") {
			return false, fmt.Errorf("terminal state %s: %v", state, result)
		}
		return result["status"] == status, nil
	})
	return result
}
