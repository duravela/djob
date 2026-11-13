package mqttdriver

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"github.com/duravela/djob/pkg/db"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestProfilesValidateBeforeBrokerIOAndNeverEchoCredentials(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mqtt.json")
	data := []byte(`{"connections":{"lab":{"broker":"tcp://127.0.0.1:1883","tenants":["alpha"],"username":"user","password":"secret-marker"}}}`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProfiles(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProfiles(path); err == nil {
		t.Fatal("accepted public credentials")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "alias")
	if err := os.Symlink(path, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProfiles(alias); err == nil {
		t.Fatal("accepted a symlinked profile file")
	}
	for _, broker := range []string{"tcp://user:secret-marker@127.0.0.1:1883", "tcp://remote:1883", "tcp://127.0.0.1", "tls://127.0.0.1:1883?token=secret-marker"} {
		profile := Profile{Broker: broker, Tenants: []string{"alpha"}, Username: "u", Password: "secret-marker"}
		_, err := NewFactory(map[string]Profile{"lab": profile}, Options{})
		if err == nil || bytes.Contains([]byte(err.Error()), []byte("secret-marker")) {
			t.Fatalf("unsafe profile or secret disclosure: %v", err)
		}
	}
	if _, err := NewFactory(map[string]Profile{"lab": {Broker: "tls://localhost:8883", Tenants: []string{"alpha"}, TLSConfig: &tls.Config{InsecureSkipVerify: true}}}, Options{}); err == nil {
		t.Fatal("accepted unverified TLS")
	}
}

func TestManagedMQTTProfilesAcceptOnlyReadOnlyProjectedSecrets(t *testing.T) {
	root := t.TempDir()
	version := filepath.Join(root, "..generation-1")
	if err := os.Mkdir(version, 0700); err != nil {
		t.Fatal(err)
	}
	actual := filepath.Join(version, "mqtt.json")
	if err := os.WriteFile(actual, []byte(`{"connections":{"lab":{"broker":"tcp://127.0.0.1:1883","tenants":["alpha"],"username":"worker","password":"private-value"}}}`), 0440); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..generation-1", filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "mqtt.json")
	if err := os.Symlink("..data/mqtt.json", path); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DJOB_WORKER_DEPLOYMENT", "line-one")
	t.Setenv("DJOB_WORKER_GENERATION", "1")
	t.Setenv("DJOB_WORKER_CREDENTIAL_DIRECTORY", root)
	profiles, err := LoadProfiles(path)
	if err != nil || profiles["lab"].Password != "private-value" {
		t.Fatalf("projected MQTT profile: %v", err)
	}
	if err := os.Chmod(actual, 0640); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProfiles(path); err == nil {
		t.Fatal("accepted a writable projected MQTT credential")
	}
}

func TestPacketLimitRejectsUnboundedAllocationBeforeReadingBody(t *testing.T) {
	client, broker := net.Pipe()
	defer client.Close()
	defer broker.Close()
	finished := make(chan struct{})
	go func() { defer close(finished); _, _ = broker.Write([]byte{0x30, 0xff, 0xff, 0x7f}) }() // More than 2 MiB, no body needed.
	reader := &packetConn{Conn: client}
	if _, err := io.ReadAll(reader); err == nil {
		t.Fatal("oversized packet was accepted")
	}
	<-finished
}

func TestPacketReaderPreservesValidFrames(t *testing.T) {
	client, broker := net.Pipe()
	defer client.Close()
	frames := []byte{0xd0, 0x00, 0x30, 0x03, 1, 2, 3, 0xd0, 0x00}
	go func() { defer broker.Close(); _, _ = broker.Write(frames) }()
	data, err := io.ReadAll(&packetConn{Conn: client})
	if err != nil || !bytes.Equal(data, frames) {
		t.Fatalf("framing changed: %v, %x", err, data)
	}
}

func TestFactoryIsAllocationOnlyAndScopesProfilesToTenants(t *testing.T) {
	factory, err := NewFactory(profiles(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	run := runSpec("publish")
	run.TenantID = "another-tenant"
	if _, err := factory(run); err == nil {
		t.Fatal("profile escaped tenant scope")
	}
	run = runSpec("request_reply")
	run.Resources = []string{"bus"}
	if _, err := factory(run); err == nil {
		t.Fatal("generic MQTT driver claimed downstream recovery support")
	}
	run = runSpec("publish")
	run.Resources = []string{"bus"}
	if _, err := factory(run); err == nil {
		t.Fatal("PUBACK was treated as a physical fence")
	}
	run = runSpec("publish")
	d := testDriver(t, run, Options{})
	d.dial = func(context.Context, Profile) (net.Conn, error) {
		t.Error("I/O occurred for cancelled action")
		return nil, io.EOF
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if effect := d.Execute(ctx, action()); effect.Status != db.EffectFailed {
		t.Fatalf("cancel before I/O: %+v", effect)
	}
}

func TestReplyTopicsMustBeFreshAndExact(t *testing.T) {
	for _, reply := range []string{"fixed/reply", "reply/+/{request_id}", "reply/#", "reply/{tenant}", "reply/\x00/{request_id}"} {
		config, _ := json.Marshal(Binding{Connection: "lab", Operations: []Operation{{Name: "execute", Kind: "request_reply", Topic: "request", ReplyTopic: reply}}})
		run := runSpec("request_reply")
		run.Binding.Config = config
		factory, _ := NewFactory(profiles(), Options{})
		if _, err := factory(run); err == nil {
			t.Fatalf("accepted reply topic %q", reply)
		}
	}
}
