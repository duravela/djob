// Package mqttdriver supplies an optional first-party MQTT 5 job driver. Business
// handlers and the durable execution engine do not depend on MQTT payloads.
package mqttdriver

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	djob "github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/worker"
)

const Name = "mqtt"
const Version = "1"
const maxPayload = 1 << 20
const maxPacket = maxPayload + 64<<10

// Profile stays process-local. Bindings reference its name; credentials and TLS
// keys never enter replicated job definitions, action payloads or history.
type Profile struct {
	Broker    string      `json:"broker"`
	Tenants   []string    `json:"tenants"`
	Username  string      `json:"username,omitempty"`
	Password  string      `json:"password,omitempty"`
	CAFile    string      `json:"ca_file,omitempty"`
	CertFile  string      `json:"cert_file,omitempty"`
	KeyFile   string      `json:"key_file,omitempty"`
	TLSConfig *tls.Config `json:"-"`
}

type Operation struct {
	Name               string `json:"name"`
	Kind               string `json:"kind"` // publish or request_reply
	Topic              string `json:"topic"`
	ReplyTopic         string `json:"reply_topic,omitempty"`
	RequireCorrelation bool   `json:"require_correlation,omitempty"`
}
type Binding struct {
	Connection string      `json:"connection"`
	Operations []Operation `json:"operations"`
}

type Reply struct {
	Topic   string
	Payload []byte
}
type Options struct {
	OperationTimeout time.Duration // Defaults to 10s, accepts 1ms..1h.
	// Resource-backed request/reply operations require a device-specific check
	// that this response proves the downstream exchange terminal and quiescent.
	// A PUBACK or arbitrary application response cannot provide that contract.
	// The function must be bounded, honor ctx and not start external effects.
	VerifyResourceCompletion func(context.Context, djob.JobRun, djob.Action, Reply) error
}

func decode(data []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil || d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("invalid MQTT configuration")
	}
	return nil
}

// LoadProfiles validates local credentials and TLS material before runtime
// startup. It performs no broker I/O and never echoes configuration values.
func LoadProfiles(path string) (map[string]Profile, error) {
	resolved, err := worker.ResolveCredentialPath(path)
	if err != nil {
		return nil, fmt.Errorf("MQTT profile file must be private or a managed read-only credential")
	}
	info, err := os.Stat(resolved)
	if err != nil || info.Size() > 64<<10 {
		return nil, fmt.Errorf("MQTT profile file must be a private regular file of at most 64 KiB")
	}
	path = resolved
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read MQTT profile file")
	}
	var file struct {
		Connections map[string]Profile `json:"connections"`
	}
	if decode(data, &file) != nil || len(file.Connections) == 0 {
		return nil, fmt.Errorf("invalid MQTT profile file")
	}
	for name, profile := range file.Connections {
		resolve := func(p string) string {
			if filepath.IsAbs(p) {
				return p
			}
			return filepath.Join(filepath.Dir(path), p)
		}
		if profile.CAFile != "" || profile.CertFile != "" || profile.KeyFile != "" {
			profile.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
			if profile.CAFile != "" {
				pem, err := os.ReadFile(resolve(profile.CAFile))
				pool := x509.NewCertPool()
				if err != nil || !pool.AppendCertsFromPEM(pem) {
					return nil, fmt.Errorf("invalid MQTT CA certificate")
				}
				profile.TLSConfig.RootCAs = pool
			}
			if profile.CertFile != "" || profile.KeyFile != "" {
				keyPath, err := worker.ResolveCredentialPath(resolve(profile.KeyFile))
				if profile.CertFile == "" || profile.KeyFile == "" || err != nil {
					return nil, fmt.Errorf("MQTT client TLS requires a certificate and private regular key file")
				}
				cert, err := tls.LoadX509KeyPair(resolve(profile.CertFile), keyPath)
				if err != nil {
					return nil, fmt.Errorf("invalid MQTT client TLS certificate or key")
				}
				profile.TLSConfig.Certificates = []tls.Certificate{cert}
			}
		}
		file.Connections[name] = profile
	}
	if _, err := NewFactory(file.Connections, Options{}); err != nil {
		return nil, err
	}
	return file.Connections, nil
}

func validateProfile(profile Profile) error {
	u, err := url.Parse(profile.Broker)
	if err != nil || (u.Scheme != "tcp" && u.Scheme != "tls") || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return fmt.Errorf("MQTT broker requires tcp://host:port or tls://host:port without credentials or parameters")
	}
	port, err := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil || port == 0 || len(profile.Tenants) == 0 {
		return fmt.Errorf("MQTT profiles require an explicit port and allowed tenants")
	}
	for _, tenant := range profile.Tenants {
		if strings.TrimSpace(tenant) == "" {
			return fmt.Errorf("MQTT allowed tenants cannot be empty")
		}
	}
	if profile.Password != "" && profile.Username == "" {
		return fmt.Errorf("MQTT password requires a username")
	}
	if (profile.CAFile != "" || profile.CertFile != "" || profile.KeyFile != "") && profile.TLSConfig == nil {
		return fmt.Errorf("MQTT TLS files must be loaded before constructing the driver factory")
	}
	if u.Scheme == "tcp" {
		if profile.TLSConfig != nil {
			return fmt.Errorf("MQTT TLS material requires a tls:// broker")
		}
		if profile.Username != "" {
			ip := net.ParseIP(u.Hostname())
			if ip == nil || !ip.IsLoopback() {
				return fmt.Errorf("MQTT credentials require TLS or a literal loopback broker")
			}
		}
	}
	if profile.TLSConfig != nil && profile.TLSConfig.InsecureSkipVerify {
		return fmt.Errorf("MQTT TLS must verify broker identity")
	}
	return nil
}

func validTopic(topic string) bool {
	return topic != "" && len(topic) <= 65535 && utf8.ValidString(topic) && !strings.ContainsAny(topic, "+#\x00\r\n") && !strings.ContainsAny(strings.ReplaceAll(topic, "{request_id}", ""), "{}")
}

// NewFactory validates and freezes connection profiles without opening sockets.
func NewFactory(profiles map[string]Profile, options Options) (djob.DriverFactory, error) {
	if options.OperationTimeout == 0 {
		options.OperationTimeout = 10 * time.Second
	}
	if options.OperationTimeout < time.Millisecond || options.OperationTimeout > time.Hour || len(profiles) == 0 {
		return nil, fmt.Errorf("MQTT requires profiles and an operation timeout in 1ms..1h")
	}
	frozen := make(map[string]Profile, len(profiles))
	for name, profile := range profiles {
		if strings.TrimSpace(name) == "" || validateProfile(profile) != nil {
			return nil, fmt.Errorf("invalid MQTT connection profile")
		}
		profile.Tenants = slices.Clone(profile.Tenants)
		if profile.TLSConfig != nil {
			profile.TLSConfig = profile.TLSConfig.Clone()
		}
		frozen[name] = profile
	}
	return func(run djob.JobRun) (djob.Driver, error) {
		if run.Binding == nil || len(run.Binding.Config) > 64<<10 {
			return nil, fmt.Errorf("MQTT requires a bounded operation binding")
		}
		var binding Binding
		if decode(run.Binding.Config, &binding) != nil || len(binding.Operations) < 1 || len(binding.Operations) > 64 {
			return nil, fmt.Errorf("invalid MQTT operation binding")
		}
		profile, exists := frozen[binding.Connection]
		if !exists || (!slices.Contains(profile.Tenants, run.TenantID) && !slices.Contains(profile.Tenants, "*")) {
			return nil, fmt.Errorf("MQTT connection is not available to this tenant")
		}
		operations := make(map[string]Operation)
		for _, op := range binding.Operations {
			if op.Name == "" || operations[op.Name].Name != "" || !validTopic(op.Topic) {
				return nil, fmt.Errorf("invalid or duplicate MQTT operation")
			}
			switch op.Kind {
			case "publish":
				if op.ReplyTopic != "" || op.RequireCorrelation || len(run.Resources) != 0 {
					return nil, fmt.Errorf("MQTT publish-only operations require resource-free runs without reply settings")
				}
			case "request_reply":
				if !validTopic(op.ReplyTopic) || !strings.Contains(op.ReplyTopic, "{request_id}") {
					return nil, fmt.Errorf("MQTT reply topic requires a fresh {request_id} and no wildcards")
				}
				if len(run.Resources) != 0 && options.VerifyResourceCompletion == nil {
					return nil, fmt.Errorf("MQTT resource-backed jobs require a device completion verifier")
				}
			default:
				return nil, fmt.Errorf("MQTT operation kind must be publish or request_reply")
			}
			operations[op.Name] = op
		}
		return &driver{profile: profile, operations: operations, options: options, run: run, dial: dial}, nil
	}, nil
}
