package db

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompactReceiptEncodingPreservesExistingReaders(t *testing.T) {
	base := compactJobReceipt{RequestID: "session/request-12345", TenantID: "alpha", RunID: "run-12345", Fingerprint: [32]byte{1, 128, 255}}
	for name, mutate := range map[string]func(*compactJobReceipt){
		"run":    func(c *compactJobReceipt) { c.HasRun, c.Step, c.Status = true, 7, JobSucceeded },
		"event":  func(c *compactJobReceipt) { c.EventID = "callback" },
		"action": func(c *compactJobReceipt) {},
		"activation": func(c *compactJobReceipt) {
			c.HasRun, c.Step, c.Status = true, 1, JobReady
			c.TriggerID, c.Source, c.ActivationID, c.TriggerRevision = "calendar", "meters", "occurrence", 3
		},
		"configuration": func(c *compactJobReceipt) {
			c.RunID = ""
			c.Configuration = &compactJobConfigurationReceipt{Kind: JobTriggerRecord, ID: "calendar", Revision: 3, Created: 2}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := base
			mutate(&c)
			full, err := encode(&c)
			require.NoError(t, err)
			narrow, err := encode(storedCompactReceipt(&c))
			require.NoError(t, err)
			var oldReader compactJobReceipt
			require.NoError(t, gob.NewDecoder(bytes.NewReader(narrow)).Decode(&oldReader))
			require.Equal(t, c, oldReader)
			require.Equal(t, c.result(), oldReader.result())
			require.Less(t, len(narrow), len(full))
			t.Logf("%s receipt: %d -> %d bytes", name, len(full), len(narrow))
		})
	}
}

func TestCompactReceiptEncodingPreservesEveryField(t *testing.T) {
	// This also exercises newly added fields: a projection must preserve their
	// value or fall back to the complete receipt, even for unusual combinations.
	typ := reflect.TypeOf(compactJobReceipt{})
	for i := 0; i < typ.NumField(); i++ {
		t.Run(typ.Field(i).Name, func(t *testing.T) {
			c := compactJobReceipt{RequestID: "request", TenantID: "alpha", RunID: "run", HasRun: true}
			field := reflect.ValueOf(&c).Elem().Field(i)
			switch field.Kind() {
			case reflect.String:
				field.SetString("nonzero")
			case reflect.Uint64:
				field.SetUint(9)
			case reflect.Bool:
				field.SetBool(true)
			case reflect.Array:
				field.Index(0).SetUint(255)
			case reflect.Pointer:
				field.Set(reflect.New(field.Type().Elem()))
			default:
				t.Fatalf("add fixture for %s", field.Type())
			}
			data, err := encode(storedCompactReceipt(&c))
			require.NoError(t, err)
			var decoded compactJobReceipt
			require.NoError(t, gob.NewDecoder(bytes.NewReader(data)).Decode(&decoded))
			require.Equal(t, c, decoded)
		})
	}
}

func FuzzCompactReceiptEncoding(f *testing.F) {
	for _, flags := range []byte{0, 1, 2, 4, 8, 15, 255} {
		f.Add(flags, "request", uint64(7))
	}
	f.Fuzz(func(t *testing.T, flags byte, text string, step uint64) {
		c := compactJobReceipt{RequestID: text, TenantID: "tenant", RunID: text, Step: step, Fingerprint: [32]byte{flags, 255}, HasRun: flags&1 != 0}
		if flags&2 != 0 {
			c.Status, c.EventID = JobSucceeded, text
		}
		if flags&4 != 0 {
			c.TriggerID, c.Source, c.ActivationID, c.TriggerRevision = text, text, text, step
		}
		if flags&8 != 0 {
			c.Configuration = &compactJobConfigurationReceipt{Kind: JobTriggerRecord, ID: text, Revision: step, Created: int(flags)}
		}
		data, err := encode(storedCompactReceipt(&c))
		require.NoError(t, err)
		var decoded compactJobReceipt
		require.NoError(t, gob.NewDecoder(bytes.NewReader(data)).Decode(&decoded))
		require.Equal(t, c, decoded)
	})
}
