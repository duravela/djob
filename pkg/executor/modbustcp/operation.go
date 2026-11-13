// Package modbustcp performs one Modbus TCP operation per claimed run.
package modbustcp

import "github.com/duravela/djob/pkg/executor/modbus"

const (
	Protocol     = "modbus-tcp"
	ExecutorName = modbus.ExecutorName
)

// Aliases preserve the adapter's existing Go API and JSON payload contract.
type Operation = modbus.Operation
type Result = modbus.Result
type Exception = modbus.Exception

func DecodeOperation(payload []byte) (Operation, error) { return modbus.DecodeOperation(payload) }
