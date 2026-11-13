package main

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/modbusrtutcp"
	"github.com/duravela/djob/pkg/executor/modbustcp"
	"github.com/duravela/djob/pkg/jobs"
	"github.com/stretchr/testify/require"
)

func TestCLISelectsOnlyExplicitlyEnabledModbusFraming(t *testing.T) {
	for _, tcp := range []bool{false, true} {
		for _, rtu := range []bool{false, true} {
			factory := modbusFactory(tcp, rtu)
			for _, protocol := range []string{modbustcp.Protocol, modbusrtutcp.Protocol, "unknown"} {
				endpoint := "tcp://127.0.0.1:502?turnaround=5ms"
				if protocol == modbusrtutcp.Protocol {
					endpoint += "&baud=9600"
				}
				transport, err := factory(db.CommunicationChannel{ID: "gateway", GatewayID: "gateway", Protocol: protocol, Endpoint: endpoint})
				if protocol == modbustcp.Protocol && tcp || protocol == modbusrtutcp.Protocol && rtu {
					require.NoError(t, err)
					require.NoError(t, transport.Close(context.Background()), "factory creation must not dial")
				} else {
					require.Error(t, err)
					require.Nil(t, transport)
				}
			}
			registry, err := jobRegistry(false, tcp, rtu, time.Second, nil)(nil)
			require.NoError(t, err)
			require.Equal(t, tcp, registry.Drivers[jobs.Version{Name: modbustcp.Protocol, Version: "1"}] != nil)
			require.Equal(t, rtu, registry.Drivers[jobs.Version{Name: modbusrtutcp.Protocol, Version: "1"}] != nil)
		}
	}
}

func TestCLIDemoCannotEnableRawRTUGatewayTraffic(t *testing.T) {
	for _, args := range [][]string{{"--demo", "--modbus-rtu-tcp"}, {"--demo", "--modbus-rtu-tcp", "--modbus-tcp"}} {
		require.Error(t, run(context.Background(), append([]string{"--data-dir", t.TempDir()}, args...), io.Discard))
	}
}
