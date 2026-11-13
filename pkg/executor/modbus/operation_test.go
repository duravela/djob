package modbus

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func unhex(t *testing.T, value string) []byte {
	t.Helper()
	data, err := hex.DecodeString(value)
	require.NoError(t, err)
	return data
}

func payload(fields string) []byte {
	return []byte(`{"version":1,"unit_id":7,` + fields + `}`)
}

func TestStandardFunctionWireVectors(t *testing.T) {
	for _, tc := range []struct {
		fields, request, response string
		coils                     []bool
		registers                 []uint16
	}{
		{`"function":"read_coils","address":19,"quantity":10`, "010013000a", "0102cd01", []bool{true, false, true, true, false, false, true, true, true, false}, nil},
		{`"function":"read_discrete_inputs","address":0,"quantity":8`, "0200000008", "020180", []bool{false, false, false, false, false, false, false, true}, nil},
		{`"function":"read_holding_registers","address":0,"quantity":2`, "0300000002", "030412340000", nil, []uint16{0x1234, 0}},
		{`"function":"read_input_registers","address":65535,"quantity":1`, "04ffff0001", "0402ffff", nil, []uint16{65535}},
		{`"function":"write_single_coil","address":7,"coils":[false]`, "0500070000", "0500070000", []bool{false}, nil},
		{`"function":"write_single_coil","address":7,"coils":[true]`, "050007ff00", "050007ff00", []bool{true}, nil},
		{`"function":"write_single_register","address":65535,"registers":[0]`, "06ffff0000", "06ffff0000", nil, []uint16{0}},
		{`"function":"write_multiple_coils","address":19,"coils":[true,false,true,true,false,false,true,true,true,false]`, "0f0013000a02cd01", "0f0013000a", []bool{true, false, true, true, false, false, true, true, true, false}, nil},
		{`"function":"write_multiple_registers","address":1,"registers":[4660,65535]`, "1000010002041234ffff", "1000010002", nil, []uint16{0x1234, 65535}},
	} {
		t.Run(tc.request, func(t *testing.T) {
			op, err := DecodeOperation(payload(tc.fields))
			require.NoError(t, err)
			require.Equal(t, unhex(t, tc.request), RequestPDU(op))
			result, status, terminal, err := ParseResponse(op, unhex(t, tc.response))
			require.NoError(t, err)
			require.Equal(t, db.AttemptSucceeded, status)
			require.True(t, terminal)
			require.Equal(t, tc.coils, result.Coils)
			require.Equal(t, tc.registers, result.Registers)
			require.Equal(t, op.quantity(), result.Quantity)
			encoded, err := json.Marshal(op)
			require.NoError(t, err)
			decoded, err := DecodeOperation(encoded)
			require.NoError(t, err, "decoding must preserve a valid operation for persistence")
			require.Equal(t, op, decoded)
		})
	}
}

func TestOperationRejectsAmbiguousOrUnsafeWrites(t *testing.T) {
	valid := string(payload(`"function":"write_single_register","address":0,"registers":[0]`))
	for _, bad := range []string{
		"", "null", "[]", valid + "{}", valid + "garbage", valid[:len(valid)-1],
		strings.Replace(valid, `"version":1`, `"version":2`, 1),
		strings.Replace(valid, `"version":1,`, "", 1),
		strings.Replace(valid, `"unit_id":7`, `"unit_id":0`, 1),
		strings.Replace(valid, `"unit_id":7`, `"unit_id":248`, 1),
		strings.Replace(valid, `"unit_id":7`, `"unit_id":-1`, 1),
		strings.Replace(valid, `"address":0`, `"address":65536`, 1),
		strings.Replace(valid, `"address":0`, `"address":0,"address":1`, 1),
		strings.Replace(valid, `"address":0`, `"address":0,"Address":1`, 1),
		strings.Replace(valid, `"address":0`, `"address":null`, 1),
		strings.Replace(valid, `"address":0,`, "", 1),
		strings.Replace(valid, `"registers":[0]`, `"registers":[null]`, 1),
		strings.Replace(valid, `"registers":[0]`, `"registers":null`, 1),
		strings.Replace(valid, `"registers":[0]`, `"registers":[]`, 1),
		strings.Replace(valid, `"registers":[0]`, `"registers":[0,1]`, 1),
		strings.Replace(valid, `"registers":[0]`, `"registers":[65536]`, 1),
		strings.Replace(valid, `"registers":[0]`, `"registers":[-1]`, 1),
		strings.Replace(valid, `"registers":[0]`, `"registers":[1.5]`, 1),
		strings.Replace(valid, `"registers":[0]`, `"coils":[false]`, 1),
		strings.Replace(valid, `"registers":[0]`, `"registers":[0],"quantity":1`, 1),
		string(payload(`"function":"read_coils","address":0,"quantity":1,"coils":[]`)),
		string(payload(`"function":"read_coils","address":0,"quantity":0`)),
		string(payload(`"function":"read_coils","address":65535,"quantity":2`)),
		string(payload(`"function":"read_coils","address":0,"quantity":2001`)),
		string(payload(`"function":"read_holding_registers","address":0,"quantity":126`)),
		string(payload(`"function":"write_single_coil","address":0,"coils":[null]`)),
		string(payload(`"function":"write_single_coil","address":0,"coils":[1]`)),
		string(payload(`"function":"raw","address":0,"quantity":1`)),
		string(payload(`"function":"read_coils","address":0,"quantity":1,"retry":true`)),
		strings.Repeat(" ", 64*1024+1),
	} {
		_, err := DecodeOperation([]byte(bad))
		require.Error(t, err, "payload: %.160s", bad)
	}
}

func TestFunctionLimitsAndAddressBoundary(t *testing.T) {
	for _, name := range []string{"read_coils", "read_discrete_inputs", "read_holding_registers", "read_input_registers",
		"write_multiple_coils", "write_multiple_registers"} {
		t.Run(name, func(t *testing.T) {
			spec, _ := function(name)
			for _, count := range []int{spec.limit, spec.limit + 1} {
				fields := fmt.Sprintf(`"function":%q,"address":0,`, name)
				if spec.read {
					fields += fmt.Sprintf(`"quantity":%d`, count)
				} else {
					field, value := "registers", "0"
					if spec.bits {
						field, value = "coils", "false"
					}
					fields += fmt.Sprintf(`%q:[%s]`, field, strings.TrimSuffix(strings.Repeat(value+",", count), ","))
				}
				op, err := DecodeOperation(payload(fields))
				if count > spec.limit {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.LessOrEqual(t, len(RequestPDU(op)), 253)
				}
			}
		})
	}
}

func TestExceptionsSeparateRejectionFromUncertainDownstreamWork(t *testing.T) {
	op, err := DecodeOperation(payload(`"function":"write_single_register","address":0,"registers":[42]`))
	require.NoError(t, err)
	for _, code := range []byte{0, 1, 2, 3, 4, 5, 6, 8, 10, 11, 255} {
		result, status, terminal, err := ParseResponse(op, []byte{0x86, code})
		require.Error(t, err)
		require.Equal(t, code, result.Exception.Code)
		var exception *Exception
		require.ErrorAs(t, err, &exception)
		if code >= 1 && code <= 3 {
			require.Equal(t, db.AttemptFailed, status)
			require.True(t, terminal)
		} else {
			require.Equal(t, db.AttemptUnknown, status)
			require.False(t, terminal)
		}
	}
}

func TestInvalidResponseNeverConfirmsAnOperation(t *testing.T) {
	for _, tc := range []struct{ fields, response string }{
		{`"function":"read_coils","address":0,"quantity":1`, "010180"},
		{`"function":"read_coils","address":0,"quantity":1`, "01020000"},
		{`"function":"read_coils","address":0,"quantity":1`, "0101"},
		{`"function":"read_coils","address":0,"quantity":1`, "020100"},
		{`"function":"read_coils","address":0,"quantity":1`, "810100"},
		{`"function":"read_holding_registers","address":0,"quantity":1`, "030100"},
		{`"function":"read_holding_registers","address":0,"quantity":1`, "0302000000"},
		{`"function":"write_single_coil","address":0,"coils":[true]`, "0500000000"},
		{`"function":"write_single_register","address":0,"registers":[42]`, "060001002a"},
		{`"function":"write_multiple_registers","address":0,"registers":[42]`, "1000000002"},
		{`"function":"write_multiple_registers","address":0,"registers":[42]`, "100000000100"},
		{`"function":"read_coils","address":0,"quantity":1`, ""},
	} {
		op, err := DecodeOperation(payload(tc.fields))
		require.NoError(t, err)
		_, status, terminal, err := ParseResponse(op, unhex(t, tc.response))
		require.Error(t, err)
		require.Equal(t, db.AttemptUnknown, status)
		require.False(t, terminal)
	}
}
