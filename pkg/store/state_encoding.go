package store

import (
	"bytes"
	"encoding/gob"
	"reflect"

	"github.com/duravela/djob/pkg/db"
)

// Each Raft entry is an independent Gob stream. Encoding the full command union
// repeats schemas for every nil operation, which dominate small claims and
// completions and remain resident in the Raft catch-up log. Gob matches structs
// by exported field name, so these envelopes can omit zero fields without
// changing the existing decoder, command version, or semantic fingerprint.
func encodeStateProposal(command db.Command) ([]byte, error) {
	var output bytes.Buffer
	err := gob.NewEncoder(&output).Encode(sparseCommandEnvelope(reflect.ValueOf(command)).Interface())
	return output.Bytes(), err
}

func commandEnvelopeType(t reflect.Type) bool {
	return t == reflect.TypeFor[db.Command]() || t == reflect.TypeFor[db.StateCommand]() || t == reflect.TypeFor[db.JobCommand]()
}

// Only project the three closed command envelopes. Payload structs, maps,
// admission credentials, and future nonzero fields keep their original types
// and values. StructOf caches types by shape; validated commands have a bounded
// set of shapes determined by the compiled schema, never by caller strings.
func sparseCommandEnvelope(value reflect.Value) reflect.Value {
	var fields []reflect.StructField
	var values []reflect.Value
	for i := 0; i < value.NumField(); i++ {
		field := value.Type().Field(i)
		if field.PkgPath != "" || field.Anonymous {
			return value // Preserve the original encoder for unsupported layouts.
		}
		v := value.Field(i)
		if v.IsZero() {
			continue
		}
		if v.Kind() == reflect.Pointer && commandEnvelopeType(v.Type().Elem()) {
			child := sparseCommandEnvelope(v.Elem())
			v = reflect.New(child.Type())
			v.Elem().Set(child)
		}
		fields = append(fields, reflect.StructField{Name: field.Name, Type: v.Type(), Tag: field.Tag})
		values = append(values, v)
	}
	if len(fields) == 0 {
		return value
	}
	projected := reflect.New(reflect.StructOf(fields)).Elem()
	for i, v := range values {
		projected.Field(i).Set(v)
	}
	return projected
}
