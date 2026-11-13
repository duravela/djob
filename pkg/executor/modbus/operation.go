// Package modbus validates and encodes the common Modbus application protocol.
// Transport framing, connections, authority and quiet periods belong to adapters.
package modbus

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/duravela/djob/pkg/db"
)

const ExecutorName = "modbus"

// Operation uses zero-based wire addresses, not the 40001 register notation.
// Reads require Quantity. Writes require Coils or Registers, with quantity
// inferred from their length. Version, UnitID, Function and Address are required.
type Operation struct {
	Version   int      `json:"version"`
	UnitID    uint16   `json:"unit_id"`
	Function  string   `json:"function"`
	Address   uint16   `json:"address"`
	Quantity  uint16   `json:"quantity,omitempty"`
	Coils     []bool   `json:"coils,omitempty"`
	Registers []uint16 `json:"registers,omitempty"`
}

// Result records raw wire values. A write result acknowledges the operation;
// its echoed values are not an independent read-back of the device.
type Result struct {
	Version   int        `json:"version"`
	UnitID    uint16     `json:"unit_id"`
	Function  string     `json:"function"`
	Address   uint16     `json:"address"`
	Quantity  uint16     `json:"quantity"`
	Coils     []bool     `json:"coils,omitempty"`
	Registers []uint16   `json:"registers,omitempty"`
	Exception *Exception `json:"exception,omitempty"`
}

type Exception struct {
	Code uint8  `json:"code"`
	Name string `json:"name"`
}

func (e *Exception) Error() string { return fmt.Sprintf("Modbus exception 0x%02x: %s", e.Code, e.Name) }

type functionSpec struct {
	code  byte
	limit int
	read  bool
	bits  bool
}

func function(name string) (functionSpec, bool) {
	switch name {
	case "read_coils":
		return functionSpec{1, 2000, true, true}, true
	case "read_discrete_inputs":
		return functionSpec{2, 2000, true, true}, true
	case "read_holding_registers":
		return functionSpec{3, 125, true, false}, true
	case "read_input_registers":
		return functionSpec{4, 125, true, false}, true
	case "write_single_coil":
		return functionSpec{5, 1, false, true}, true
	case "write_single_register":
		return functionSpec{6, 1, false, false}, true
	case "write_multiple_coils":
		return functionSpec{15, 1968, false, true}, true
	case "write_multiple_registers":
		return functionSpec{16, 123, false, false}, true
	default:
		return functionSpec{}, false
	}
}

// DecodeOperation rejects ambiguous, unsupported or out-of-range operations
// before any network access. Exact field names and one JSON object are required.
func DecodeOperation(payload []byte) (Operation, error) {
	var op Operation
	if len(payload) > 64*1024 {
		return op, fmt.Errorf("Modbus payload exceeds 64 KiB")
	}
	d := json.NewDecoder(bytes.NewReader(payload))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return op, fmt.Errorf("Modbus operation must be a JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return op, err
		}
		name, ok := key.(string)
		if !ok {
			return op, fmt.Errorf("invalid Modbus operation field")
		}
		if _, exists := fields[name]; exists {
			return op, fmt.Errorf("duplicate Modbus operation field %q", name)
		}
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil {
			return op, err
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return op, fmt.Errorf("Modbus field %q cannot be null", name)
		}
		fields[name] = raw
		var target any
		switch name {
		case "version":
			target = &op.Version
		case "unit_id":
			target = &op.UnitID
		case "function":
			target = &op.Function
		case "address":
			target = &op.Address
		case "quantity":
			target = &op.Quantity
		case "coils", "registers":
			// Decode elements separately: encoding/json otherwise accepts null
			// as a zero bool or integer, which can silently change a write.
			var values []json.RawMessage
			if err := json.Unmarshal(raw, &values); err != nil {
				return op, fmt.Errorf("%s: %w", name, err)
			}
			for _, value := range values {
				if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
					return op, fmt.Errorf("%s cannot contain null", name)
				}
				if name == "coils" {
					var v bool
					if err := json.Unmarshal(value, &v); err != nil {
						return op, err
					}
					op.Coils = append(op.Coils, v)
				} else {
					var v uint16
					if err := json.Unmarshal(value, &v); err != nil {
						return op, err
					}
					op.Registers = append(op.Registers, v)
				}
			}
			continue
		default:
			return op, fmt.Errorf("unknown Modbus operation field %q", name)
		}
		if err := json.Unmarshal(raw, target); err != nil {
			return op, fmt.Errorf("%s: %w", name, err)
		}
	}
	if _, err := d.Token(); err != nil {
		return op, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return op, fmt.Errorf("Modbus payload must contain exactly one object")
	}
	for _, required := range []string{"version", "unit_id", "function", "address"} {
		if _, ok := fields[required]; !ok {
			return op, fmt.Errorf("Modbus field %q is required", required)
		}
	}
	if op.Version != 1 || op.UnitID < 1 || op.UnitID > 247 {
		return op, fmt.Errorf("Modbus version must be 1 and unit_id must be 1..247; broadcasts are unsupported")
	}
	spec, ok := function(op.Function)
	if !ok {
		return op, fmt.Errorf("unsupported Modbus function %q", op.Function)
	}
	_, quantity := fields["quantity"]
	_, coils := fields["coils"]
	_, registers := fields["registers"]
	count := int(op.Quantity)
	if spec.read {
		if !quantity || coils || registers {
			return op, fmt.Errorf("reads require quantity and cannot include write values")
		}
	} else {
		if quantity || (spec.bits && (!coils || registers)) || (!spec.bits && (!registers || coils)) {
			return op, fmt.Errorf("writes require only the matching values array; omit quantity")
		}
		count = len(op.Registers)
		if spec.bits {
			count = len(op.Coils)
		}
		if count > spec.limit {
			return op, fmt.Errorf("Modbus write exceeds function limit %d", spec.limit)
		}
	}
	if count < 1 || count > spec.limit || int(op.Address)+count > 65536 {
		return op, fmt.Errorf("invalid Modbus address range or quantity (function limit %d)", spec.limit)
	}
	return op, nil
}

func (op Operation) quantity() uint16 {
	spec, _ := function(op.Function)
	if spec.read {
		return op.Quantity
	}
	if spec.bits {
		return uint16(len(op.Coils))
	}
	return uint16(len(op.Registers))
}

// RequestPDU encodes an operation returned by DecodeOperation. The operation
// must remain unmodified after validation. It contains no transport header.
func RequestPDU(op Operation) []byte {
	spec, _ := function(op.Function)
	pdu := make([]byte, 5)
	pdu[0] = spec.code
	binary.BigEndian.PutUint16(pdu[1:3], op.Address)
	switch spec.code {
	case 5:
		if op.Coils[0] {
			pdu[3] = 0xff
		}
	case 6:
		binary.BigEndian.PutUint16(pdu[3:5], op.Registers[0])
	default:
		binary.BigEndian.PutUint16(pdu[3:5], op.quantity())
		if !spec.read {
			if spec.bits {
				data := make([]byte, (int(op.quantity())+7)/8)
				for i, v := range op.Coils {
					if v {
						data[i/8] |= 1 << (i % 8)
					}
				}
				pdu = append(pdu, byte(len(data)))
				pdu = append(pdu, data...)
			} else {
				pdu = append(pdu, byte(2*len(op.Registers)))
				for _, v := range op.Registers {
					pdu = binary.BigEndian.AppendUint16(pdu, v)
				}
			}
		}
	}
	return pdu
}

// ParseResponse separates confirmed rejection from uncertain effect. Only a
// normal reply or an illegal-function/address/value rejection establishes a
// terminal downstream exchange. Other exceptions conservatively block reuse.
func ParseResponse(op Operation, pdu []byte) (Result, db.AttemptOutcome, bool, error) {
	result := Result{Version: 1, UnitID: op.UnitID, Function: op.Function, Address: op.Address, Quantity: op.quantity()}
	spec, _ := function(op.Function)
	invalid := func() (Result, db.AttemptOutcome, bool, error) {
		return result, db.AttemptUnknown, false, fmt.Errorf("malformed or mismatched Modbus response")
	}
	if len(pdu) == 2 && pdu[0] == spec.code|0x80 {
		e := &Exception{Code: pdu[1], Name: exceptionName(pdu[1])}
		result.Exception = e
		if e.Code >= 1 && e.Code <= 3 {
			return result, db.AttemptFailed, true, e
		}
		return result, db.AttemptUnknown, false, e
	}
	if len(pdu) < 2 || pdu[0] != spec.code {
		return invalid()
	}
	if !spec.read {
		if !bytes.Equal(pdu, RequestPDU(op)[:5]) {
			return invalid()
		}
		result.Coils, result.Registers = op.Coils, op.Registers
	} else if spec.bits {
		count := (int(op.Quantity) + 7) / 8
		if len(pdu) != 2+count || int(pdu[1]) != count {
			return invalid()
		}
		if remainder := op.Quantity % 8; remainder != 0 && pdu[len(pdu)-1]>>remainder != 0 {
			return invalid()
		}
		result.Coils = make([]bool, op.Quantity)
		for i := range result.Coils {
			result.Coils[i] = pdu[2+i/8]&(1<<(i%8)) != 0
		}
	} else {
		count := 2 * int(op.Quantity)
		if len(pdu) != 2+count || int(pdu[1]) != count {
			return invalid()
		}
		result.Registers = make([]uint16, op.Quantity)
		for i := range result.Registers {
			result.Registers[i] = binary.BigEndian.Uint16(pdu[2+2*i : 4+2*i])
		}
	}
	return result, db.AttemptSucceeded, true, nil
}

func exceptionName(code byte) string {
	switch code {
	case 1:
		return "illegal_function"
	case 2:
		return "illegal_data_address"
	case 3:
		return "illegal_data_value"
	case 4:
		return "server_device_failure"
	case 5:
		return "acknowledge"
	case 6:
		return "server_device_busy"
	case 8:
		return "memory_parity_error"
	case 10:
		return "gateway_path_unavailable"
	case 11:
		return "gateway_target_no_response"
	default:
		return "unknown_exception"
	}
}
