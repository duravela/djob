package e2e_test

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
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

	dockerclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// This drill uses real CLI commands, process restarts and independent Docker
// volumes. Faults are confined to the fixture; no host clock or disk is changed.
func TestDeploymentRehearsal(t *testing.T) {
	h := newHarnessWithTimeout(t, false, 12*time.Minute, func(h *harness) {
		for i := range h.volumes {
			h.volumes[i] = h.newVolume()
		}
	})
	report := object{"started_at": time.Now().UTC(), "environment": "isolated Linux Docker", "stages": []string{}, "synthetic_faults": []string{"configured storage admission threshold", "persisted clock hold marker"}}
	t.Cleanup(func() {
		report["passed"], report["finished_at"] = !t.Failed(), time.Now().UTC()
		if dir := os.Getenv("DJOB_TEST_ARTIFACT_DIR"); dir != "" {
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Error(err)
				return
			}
			data, _ := json.MarshalIndent(report, "", "  ")
			if err := os.WriteFile(filepath.Join(dir, "deployment-rehearsal-report.json"), append(data, '\n'), 0600); err != nil {
				t.Error(err)
			}
		}
	})
	stage := func(name string) {
		report["stages"] = append(report["stages"].([]string), name)
		t.Log("Rehearsed:", name)
	}
	code, reader, err := h.members[0].Exec(h.ctx, []string{"/djob", "version"}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		t.Fatalf("binary identity: %d %v", code, err)
	}
	identity, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	var build object
	if err := json.Unmarshal(identity, &build); err != nil {
		t.Fatal(err)
	}
	report["binary"] = build
	if data, err := os.ReadFile(filepath.Join(os.Getenv("DJOB_E2E_BUILD_DIR"), "release-manifest.json")); err == nil {
		var manifest object
		if err := json.Unmarshal(data, &manifest); err != nil {
			t.Fatal(err)
		}
		report["release_manifest"] = manifest
	}
	initial := h.newVolume()
	h.maintenance(initial, "", 0, "init-member", "--dynamic-membership", "--id", "1", "--data-dir", "/data/initial", "--peers", "http://127.0.0.1:9021")
	stage("one-time dynamic member initialization without public APIs")
	h.mutate(0, "platform", "alpha", "put_tenant", object{"max_running": "4"})
	h.definition("alpha", "compute", "sha256", "", object{"rehearsal": true})
	h.launch(0, "alpha", "compute", "before-backup")
	original := h.run(0, "alpha", "before-backup", "succeeded")
	sum := sha256.Sum256([]byte(`{"rehearsal":true}`))
	if original["result"] != base64.StdEncoding.EncodeToString(sum[:]) {
		t.Fatal("invalid initial result")
	}
	for i := range h.members {
		h.ready(i)
		h.run(i, "alpha", "before-backup", "succeeded")
	}
	monitor := h.startRehearsalPrometheus()
	monitor.alert("DjobMemberUnavailable", false)
	// A single stopped member is backed up while the other two maintain quorum.
	artifacts := h.newVolume()
	source := h.followersFirst()[0]
	h.stopMember(source)
	h.maintenance(h.volumes[source], artifacts, 0, "backup", "--data-dir", "/data", "--id", fmt.Sprint(source+1), "--out", "/artifacts/checkpoint")
	h.maintenance("", artifacts, 0, "verify-backup", "--backup", "/artifacts/checkpoint")
	monitor.alert("DjobMemberUnavailable", true)
	if err := h.members[source].Start(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.refreshURL(source)
	h.ready(source)
	monitor.alert("DjobMemberUnavailable", false)
	stage("stopped-member backup, verification, availability alert and recovery")

	// Load new API credentials during rolling restarts, retaining old credentials
	// during overlap. Then retire the old token and verify every member rejects it.
	oldToken := h.tokens["alpha-reader"]
	newToken := oldToken + "-rotated"
	var auth struct {
		Credentials []object `json:"credentials"`
	}
	if err := json.Unmarshal(h.material[0]["/auth.json"], &auth); err != nil {
		t.Fatal(err)
	}
	auth.Credentials = append(auth.Credentials, object{"subject": "alpha-reader-new", "role": "reader", "tenant_id": "alpha", "token": newToken})
	rotateAuth := func() {
		data, _ := json.Marshal(auth)
		for _, i := range h.followersFirst() {
			h.material[i]["/auth.json"] = data
			h.replaceMember(i)
		}
	}
	rotateAuth()
	h.tokens["rotated-reader"] = newToken
	for i := range h.members {
		h.request(i, "POST", "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": "RUN", "id": "before-backup"}}, 200)
		h.request(i, "POST", "/api/jobs/v1/get", "rotated-reader", object{"reference": object{"kind": "RUN", "id": "before-backup"}}, 200)
	}
	var retained []object
	for _, row := range auth.Credentials {
		if row["token"] != oldToken {
			retained = append(retained, row)
		}
	}
	auth.Credentials = retained
	rotateAuth()
	for i := range h.members {
		h.request(i, "POST", "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": "RUN", "id": "before-backup"}}, 401)
	}
	h.tokens["alpha-reader"] = newToken
	stage("API bearer overlap and retirement across a rolling restart")

	// API trust overlap precedes certificate replacement. Prometheus also receives
	// both roots before any leaf changes; it keeps its metrics-only credential.
	apiCA, peerCA := newAuthority(t, time.Hour), newAuthority(t, time.Hour)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(h.apiCA)
	roots.AppendCertsFromPEM(apiCA.pem)
	transport := h.client.Transport.(*http.Transport)
	transport.CloseIdleConnections()
	transport.TLSClientConfig.RootCAs = roots
	monitor.trust(append(append([]byte{}, h.apiCA...), apiCA.pem...))
	cert, key := apiCA.leaf(t, "djob-e2e", "member-1", "member-2", "member-3")
	for _, i := range h.followersFirst() {
		h.material[i]["/api.pem"], h.material[i]["/api-key.pem"] = cert, key
		h.replaceMember(i)
	}
	transport.CloseIdleConnections()
	transport.TLSClientConfig.RootCAs = apiCA.roots
	h.apiCA = apiCA.pem
	monitor.trust(apiCA.pem)
	// Raft: expand CA trust everywhere, rotate identities, then remove old trust.
	for _, i := range h.followersFirst() {
		h.material[i]["/ca.pem"] = append(append([]byte{}, h.material[i]["/ca.pem"]...), peerCA.pem...)
		h.replaceMember(i)
	}
	for _, i := range h.followersFirst() {
		h.material[i]["/peer.pem"], h.material[i]["/peer-key.pem"] = peerCA.leaf(t, fmt.Sprintf("member-%d", i+1))
		h.replaceMember(i)
	}
	for _, i := range h.followersFirst() {
		h.material[i]["/ca.pem"] = peerCA.pem
		h.replaceMember(i)
	}
	for i := range h.members {
		h.ready(i)
		h.run(i, "alpha", "before-backup", "succeeded")
	}
	stage("API CA rotation and three-phase Raft CA rotation with quorum preserved")

	// A configured database ceiling triggers the ordinary pressure path without
	// consuming disk. Reads continue and a local new-work mutation is refused.
	affected := h.followersFirst()[0]
	saved := append([]string{}, h.memberArgs[affected]...)
	h.memberArgs[affected] = append(h.memberArgs[affected], "--max-database-bytes", "1")
	h.replaceMemberHeld(affected)
	h.awaitHold(affected, "storage_pressure")
	monitor.alert("DjobStoragePressure", true)
	h.run(affected, "alpha", "before-backup", "succeeded")
	h.rejectNewWork(affected)
	h.memberArgs[affected] = saved
	h.replaceMember(affected)
	monitor.alert("DjobStoragePressure", false)
	stage("storage pressure admission, readable history, alert firing and clearing")

	h.stopMember(affected)
	if err := h.members[affected].CopyToContainer(h.ctx, []byte("synthetic persisted hold for rehearsal\n"), "/data/.djob-clock-hold", 0600); err != nil {
		t.Fatal(err)
	}
	if err := h.members[affected].Start(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.refreshURL(affected)
	h.awaitHold(affected, "clock_untrusted")
	monitor.alert("DjobClockHold", true)
	h.rejectNewWork(affected)
	h.maintenance(h.volumes[affected], "", 1, "clock-reset", "--data-dir", "/data", "--reference-time", time.Now().UTC().Format(time.RFC3339Nano), "--reason", "synthetic drill; independent test host reference")
	h.stopMember(affected)
	h.maintenance(h.volumes[affected], "", 0, "clock-reset", "--data-dir", "/data", "--reference-time", time.Now().UTC().Format(time.RFC3339Nano), "--reason", "synthetic drill; independent test host reference")
	if err := h.members[affected].Start(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.refreshURL(affected)
	h.ready(affected)
	monitor.alert("DjobClockHold", false)
	stage("persisted clock hold, online reset refusal, offline reset and alert clearing")

	// Fence every old incarnation before preparing a fresh recovery cohort.
	for _, member := range h.members {
		if err := member.Terminate(h.ctx); err != nil {
			t.Fatal(err)
		}
	}
	recoveryCA := newAuthority(t, time.Hour)
	for i := range h.members {
		h.volumes[i] = h.newVolume()
		h.maintenance(h.volumes[i], artifacts, 0, "recover", "--backup", "/artifacts/checkpoint", "--data-dir", "/data/recovered", "--id", fmt.Sprint(i+1), "--peers", "https://member-1:9021,https://member-2:9021,https://member-3:9021", "--old-access-fenced")
		h.material[i]["/ca.pem"] = recoveryCA.pem
		h.material[i]["/peer.pem"], h.material[i]["/peer-key.pem"] = recoveryCA.leaf(t, fmt.Sprintf("member-%d", i+1))
		h.memberArgs[i] = withoutExecutionFlags(h.memberArgs[i])
		for at, arg := range h.memberArgs[i] {
			if arg == "--data-dir" {
				h.memberArgs[i][at+1] = "/data/recovered"
			}
		}
		h.createMember(i)
	}
	h.startAll()
	for i := range h.members {
		h.awaitHold(i, "recovery_hold")
		restored := h.run(i, "alpha", "before-backup", "succeeded")
		if restored["result"] != original["result"] {
			t.Fatal("recovery changed durable result")
		}
		h.rejectNewWork(i)
	}
	monitor.alert("DjobRecoveryHold", true)
	for i := range h.members {
		h.stopMember(i)
	}
	for i := range h.members {
		h.maintenance(h.volumes[i], "", 0, "recovery-release", "--data-dir", "/data/recovered", "--id", fmt.Sprint(i+1), "--reason", "old containers terminated; completed-only fixture reconciled")
	}
	for i := range h.members {
		h.memberArgs[i] = append(h.memberArgs[i], "--jobs")
		if err := h.members[i].Terminate(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.createMember(i)
	}
	h.startAll()
	for i := range h.members {
		h.ready(i)
	}
	monitor.alert("DjobRecoveryHold", false)
	h.launch(0, "alpha", "compute", "after-recovery")
	h.run(0, "alpha", "after-recovery", "succeeded")
	stage("fresh three-member disaster recovery, durable history, guarded release and resumed work")
}

func withoutExecutionFlags(args []string) []string {
	var result []string
	for _, arg := range args {
		if arg != "--jobs" && arg != "--modbus-tcp" && arg != "--modbus-rtu-tcp" {
			result = append(result, arg)
		}
	}
	return result
}
func (h *harness) newVolume() string {
	h.t.Helper()
	client, err := testcontainers.NewDockerClientWithOpts(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer client.Close()
	volume, err := client.VolumeCreate(h.ctx, dockerclient.VolumeCreateOptions{Labels: map[string]string{"djob.fixture": "deployment-rehearsal"}})
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		c, err := testcontainers.NewDockerClientWithOpts(ctx)
		if err != nil {
			h.t.Error(err)
			return
		}
		defer c.Close()
		if _, err := c.VolumeRemove(ctx, volume.Volume.Name, dockerclient.VolumeRemoveOptions{}); err != nil {
			h.t.Error(err)
		}
	})
	return volume.Volume.Name
}
func (h *harness) maintenance(data, artifacts string, want int, args ...string) {
	h.t.Helper()
	options := []testcontainers.ContainerCustomizer{testcontainers.WithEntrypoint("/djob"), testcontainers.WithCmd(args...), testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(time.Minute))}
	var mounts []testcontainers.ContainerMount
	if data != "" {
		mounts = append(mounts, testcontainers.VolumeMount(data, "/data"))
	}
	if artifacts != "" {
		mounts = append(mounts, testcontainers.VolumeMount(artifacts, "/artifacts"))
	}
	options = append(options, testcontainers.WithMounts(mounts...))
	c, err := testcontainers.Run(h.ctx, h.image, options...)
	if c != nil {
		defer c.Terminate(context.Background())
	}
	if err != nil {
		h.t.Fatal(err)
	}
	state, err := c.State(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	reader, err := c.Logs(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer reader.Close()
	logs, _ := io.ReadAll(io.LimitReader(reader, 1<<20))
	if want != 0 && !strings.Contains(strings.ToLower(string(logs)), "lock") {
		h.t.Fatalf("expected stopped-member lock rejection: %s", logs)
	}
	if (want == 0 && state.ExitCode != 0) || (want != 0 && state.ExitCode == 0) {
		h.t.Fatalf("%s: exit %d, want success=%t: %s", args[0], state.ExitCode, want == 0, logs)
	}
}
func (h *harness) stopMember(i int) {
	h.t.Helper()
	if err := h.members[i].Stop(h.ctx, nil); err != nil {
		h.t.Fatal(err)
	}
}
func (h *harness) replaceMemberHeld(i int) {
	h.t.Helper()
	if err := h.members[i].Terminate(h.ctx); err != nil {
		h.t.Fatal(err)
	}
	for other := range h.members {
		if other != i {
			h.ready(other)
		}
	}
	h.createMember(i)
	if err := h.members[i].Start(h.ctx); err != nil {
		h.t.Fatal(err)
	}
	h.refreshURL(i)
}
func (h *harness) replaceMember(i int) { h.t.Helper(); h.replaceMemberHeld(i); h.ready(i) }
func (h *harness) followersFirst() []int {
	h.t.Helper()
	var order []int
	leader := -1
	for i := range h.members {
		value, err := h.metric(i, "djob_raft_leader")
		if err != nil {
			h.t.Fatal(err)
		}
		if value == 1 {
			leader = i
		} else {
			order = append(order, i)
		}
	}
	if leader < 0 || len(order) != 2 {
		h.t.Fatal("expected one leader before rolling restart")
	}
	return append(order, leader)
}
func (h *harness) awaitHold(i int, reason string) {
	h.t.Helper()
	poll(h.t, reason, func() (bool, error) {
		code, value, err := h.call(i, "GET", "/readyz", "", nil)
		return code == 503 && value["reason"] == reason, err
	})
}
func (h *harness) rejectNewWork(i int) {
	h.t.Helper()
	code, value, err := h.call(i, "POST", "/api/jobs/v1/mutate", "alpha-writer", object{"command": h.meta(), "launch": object{"definition_id": "compute", "definition_revision": "1", "run_id": fmt.Sprintf("held-%d", h.nextID)}})
	if err != nil || (code != 503 && code != 409) {
		h.t.Fatalf("held mutation status=%d error=%v response=%v", code, err, value)
	}
}

type rehearsalPrometheus struct {
	h         *harness
	container *testcontainers.DockerContainer
	url       string
}

func (h *harness) startRehearsalPrometheus() *rehearsalPrometheus {
	h.t.Helper()
	rules, err := os.ReadFile(filepath.Join("..", "..", "deploy", "prometheus", "alerts.yml"))
	if err != nil {
		h.t.Fatal(err)
	}
	config := "global:\n  scrape_interval: 1s\n  evaluation_interval: 1s\nrule_files: [/etc/prometheus/djob-alerts.yml]\nscrape_configs:\n  - job_name: djob\n    scheme: https\n    authorization:\n      credentials_file: /etc/prometheus/token\n    tls_config:\n      ca_file: /etc/prometheus/ca.pem\n      server_name: djob-e2e\n    static_configs:\n      - targets: [member-1:8080, member-2:8080, member-3:8080]\n"
	var files []testcontainers.ContainerFile
	for name, data := range map[string][]byte{"prometheus.yml": []byte(config), "djob-alerts.yml": rules, "ca.pem": h.apiCA, "token": []byte(h.tokens["monitor"])} {
		file := filepath.Join(h.t.TempDir(), name)
		if err := os.WriteFile(file, data, 0600); err != nil {
			h.t.Fatal(err)
		}
		files = append(files, testcontainers.ContainerFile{HostFilePath: file, ContainerFilePath: "/etc/prometheus/" + name, FileMode: 0644})
	}
	c, err := testcontainers.Run(h.ctx, "prom/prometheus:v3.16.0", testcontainers.WithFiles(files...), testcontainers.WithExposedPorts("9090/tcp"), network.WithNetwork([]string{"prometheus"}, h.network), testcontainers.WithWaitStrategy(wait.ForHTTP("/-/ready").WithPort("9090/tcp")))
	h.own(c, "prometheus")
	if err != nil {
		h.t.Fatal(err)
	}
	url, err := c.PortEndpoint(h.ctx, "9090/tcp", "http")
	if err != nil {
		h.t.Fatal(err)
	}
	p := &rehearsalPrometheus{h, c, url}
	poll(h.t, "Prometheus scrapes all members", func() (bool, error) {
		result, err := p.get("/api/v1/query?query=count(up%7Bjob%3D%22djob%22%7D%20%3D%3D%201)")
		return strings.Contains(string(result), `"3"`), err
	})
	return p
}
func (p *rehearsalPrometheus) get(path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(p.h.ctx, "GET", p.url+path, nil)
	if err != nil {
		return nil, err
	}
	response, err := (&http.Client{Timeout: 4 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("Prometheus status %d", response.StatusCode)
	}
	return data, err
}
func (p *rehearsalPrometheus) alert(name string, firing bool) {
	p.h.t.Helper()
	poll(p.h.t, name+" firing="+fmt.Sprint(firing), func() (bool, error) {
		data, err := p.get("/api/v1/alerts")
		if err != nil {
			return false, err
		}
		var result struct {
			Data struct {
				Alerts []struct {
					Labels map[string]string
					State  string
				}
			}
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return false, err
		}
		found := false
		for _, alert := range result.Data.Alerts {
			if alert.Labels["alertname"] == name && alert.State == "firing" {
				found = true
			}
		}
		return found == firing, nil
	})
}
func (p *rehearsalPrometheus) trust(ca []byte) {
	p.h.t.Helper()
	if err := p.container.Stop(p.h.ctx, nil); err != nil {
		p.h.t.Fatal(err)
	}
	if err := p.container.CopyToContainer(p.h.ctx, ca, "/etc/prometheus/ca.pem", 0644); err != nil {
		p.h.t.Fatal(err)
	}
	if err := p.container.Start(p.h.ctx); err != nil {
		p.h.t.Fatal(err)
	}
	url, err := p.container.PortEndpoint(p.h.ctx, "9090/tcp", "http")
	if err != nil {
		p.h.t.Fatal(err)
	}
	p.url = url
}
