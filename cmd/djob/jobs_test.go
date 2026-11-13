package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/modbustcp"
	"github.com/duravela/djob/pkg/jobs"
	modbusbinding "github.com/duravela/djob/pkg/jobs/bindings/modbus"
	"github.com/stretchr/testify/require"
)

func TestGenericCLIRejectsInvalidExecutionSettingsBeforeStartup(t *testing.T) {
	for _, flags := range [][]string{{"--jobs-http"}, {"--jobs-mqtt"}, {"--jobs", "--jobs-mqtt"}, {"--mqtt-profiles", "unused"}, {"--jobs-operation-timeout", "0"}, {"--jobs-operation-timeout", "2h"}, {"--jobs-concurrency", "0"}, {"--jobs-concurrency", "1025"}, {"--jobs-concurrency", "2", "--jobs-per-tenant", "3"}} {
		args := append([]string{"--data-dir", t.TempDir()}, flags...)
		require.Error(t, run(context.Background(), args, io.Discard))
	}
}

func TestInvalidMQTTProfilesFailBeforeCreatingMemberStorage(t *testing.T) {
	dir := t.TempDir()
	profile := filepath.Join(dir, "mqtt.json")
	require.NoError(t, os.WriteFile(profile, []byte(`{"connections":{"lab":{"broker":"tcp://user:secret-marker@host:1883","tenants":["alpha"]}}}`), 0600))
	dataDir := filepath.Join(dir, "not-created")
	err := run(context.Background(), []string{"--data-dir", dataDir, "--jobs", "--jobs-mqtt", "--mqtt-profiles", profile}, io.Discard)
	require.Error(t, err)
	require.False(t, strings.Contains(err.Error(), "secret-marker"))
	_, statErr := os.Stat(dataDir)
	require.True(t, os.IsNotExist(statErr))
}

func TestModbusJobBindingOwnsProtocolDetailsAndRejectsPayloadOverrides(t *testing.T) {
	b := db.JobBinding{Config: []byte(`{"channel_id":"gateway","operations":[{"name":"execute","operation":{"version":1,"unit_id":7,"function":"read_holding_registers","address":20,"quantity":2}}]}`)}
	name, payload, err := modbusbinding.Encode(b, jobs.Action{Operation: "execute"})
	require.NoError(t, err)
	require.Equal(t, modbustcp.ExecutorName, name)
	op, err := modbustcp.DecodeOperation(payload)
	require.NoError(t, err)
	require.EqualValues(t, 7, op.UnitID)
	require.EqualValues(t, 20, op.Address)
	_, _, err = modbusbinding.Encode(b, jobs.Action{Operation: "execute", Input: []byte(`{"unit_id":99}`)})
	require.Error(t, err)
	_, _, err = modbusbinding.Encode(b, jobs.Action{Operation: "unmapped"})
	require.Error(t, err)
}
