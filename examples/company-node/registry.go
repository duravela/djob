// Package companynode explicitly assembles the company's enabled job handlers
// and integrations. The djob engine discovers no company code implicitly.
package companynode

import (
	"example.com/acme/djob-node/integrations/meter"
	"example.com/acme/djob-node/jobs/measurement"
	"github.com/duravela/djob/pkg/server"
	djob "github.com/duravela/djob/sdk/go"
)

// Registry is supplied to server.Config.JobRegistry by the company executable.
// Keep versions registered while durable runs still reference them.
func Registry(backend server.JobBackend) (djob.Registry, error) {
	return djob.Registry{
		Handlers: map[djob.Version]djob.Handler{
			{Name: measurement.Name, Version: measurement.Version}: measurement.Handle,
		},
		Drivers: map[djob.Version]djob.DriverFactory{
			{Name: meter.Name, Version: meter.Version}: meter.NewFactory(backend),
		},
	}, nil
}
