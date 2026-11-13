// Package companyworker explicitly assembles the company's versioned extensions.
package companyworker

import (
	"time"

	"example.com/acme/djob-worker/integrations/meter"
	"example.com/acme/djob-worker/jobs/measurement"
	"github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/handlers"
)

func Registry(recovery time.Duration) djob.Registry {
	r := handlers.Builtins()
	r.Handlers[djob.Version{Name: measurement.Name, Version: measurement.Version}] = measurement.Handle
	r.Handlers[djob.Version{Name: measurement.TwiceName, Version: measurement.Version}] = measurement.Twice
	r.Drivers[djob.Version{Name: meter.Name, Version: meter.Version}] = meter.NewFactory(recovery)
	return r
}
