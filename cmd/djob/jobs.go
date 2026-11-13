package main

import (
	"time"

	"github.com/duravela/djob/pkg/executor/modbusrtutcp"
	"github.com/duravela/djob/pkg/executor/modbustcp"
	mqttdriver "github.com/duravela/djob/pkg/integrations/mqtt"
	"github.com/duravela/djob/pkg/jobs"
	modbusbinding "github.com/duravela/djob/pkg/jobs/bindings/modbus"
	"github.com/duravela/djob/pkg/server"
)

func jobRegistry(httpEnabled, tcpEnabled, rtuEnabled bool, recovery time.Duration, mqttFactory jobs.DriverFactory) func(server.JobBackend) (jobs.Registry, error) {
	return func(backend server.JobBackend) (jobs.Registry, error) {
		registry := jobs.Builtins()
		if httpEnabled {
			registry.Drivers[jobs.Version{Name: "http", Version: "1"}] = jobs.HTTPFactory(nil)
		}
		if mqttFactory != nil {
			registry.Drivers[jobs.Version{Name: mqttdriver.Name, Version: mqttdriver.Version}] = mqttFactory
		}
		if tcpEnabled {
			registry.Drivers[jobs.Version{Name: "modbus-tcp", Version: "1"}] = jobs.GatewayFactory(backend, modbustcp.Factory, modbusbinding.Encode, recovery)
		}
		if rtuEnabled {
			registry.Drivers[jobs.Version{Name: "modbus-rtu-tcp", Version: "1"}] = jobs.GatewayFactory(backend, modbusrtutcp.Factory, modbusbinding.Encode, recovery)
		}
		return registry, nil
	}
}
