package db

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMaintenanceReceiptEncodingPreservesExistingReaders(t *testing.T) {
	at := time.Date(2026, 11, 9, 0, 0, 0, 123, time.UTC)
	for _, job := range []*JobResult{
		{Deletion: &JobDeletion{TenantID: "alpha", RunID: "done", Step: 7, Status: JobSucceeded, DefinitionID: "work", DefinitionRevision: 3, TriggerID: "calendar", TriggerRevision: 1, Resources: []string{"bus"}, CreatedAt: at, FinishedAt: at, DeletedAt: at, LastCleanupAt: at, Actor: "operator", Reason: "history retention", PolicyRevision: 2, RecordsDeleted: 5, CleanupComplete: true}},
		{ReceiptCompaction: &JobReceiptCompaction{TenantID: "alpha", RunID: "done", Step: 7, Actor: "operator", Reason: "history retention", StartedAt: at, LastCleanupAt: at, ReceiptsCompacted: 9, PayloadBytesRemoved: 1 << 20, Complete: true}},
		{ConfigurationReceiptCompaction: &JobConfigurationReceiptCompaction{TenantID: "alpha", ID: "sweep", Step: 7, Horizon: 100, Actor: "operator", Reason: "history retention", StartedAt: at, LastCleanupAt: at, ReceiptsCompacted: 9, PayloadBytesRemoved: 1 << 20, Complete: true}},
	} {
		source := StateResult{RequestID: "request", Fingerprint: [32]byte{1, 2, 3}, Job: job}
		original, err := encode(source)
		require.NoError(t, err)
		projected, err := encode(storedStateReceipt(source))
		require.NoError(t, err)
		// This is the existing reader, with no special decoder or format activation.
		var decoded StateResult
		require.NoError(t, gob.NewDecoder(bytes.NewReader(projected)).Decode(&decoded))
		require.Equal(t, source, decoded)
		require.Less(t, len(projected), len(original)/2)
		t.Logf("maintenance receipt: %d -> %d bytes", len(original), len(projected))
	}
}

func TestSparseStateReceiptEncodingPreservesExistingReaders(t *testing.T) {
	for _, rejection := range []*CommandRejection{nil, {Code: "resource_busy", Message: "resource is in use"}} {
		source := StateResult{RequestID: "request", Fingerprint: [32]byte{1, 2, 3}, Rejection: rejection}
		original, err := encode(source)
		require.NoError(t, err)
		projected, err := encode(storedStateReceipt(source))
		require.NoError(t, err)
		var decoded StateResult
		require.NoError(t, gob.NewDecoder(bytes.NewReader(projected)).Decode(&decoded))
		require.Equal(t, source, decoded)
		require.Less(t, len(projected), len(original)/2)
		t.Logf("sparse receipt: %d -> %d bytes", len(original), len(projected))
	}
	// Any other current or future result field must prevent narrowing.
	typ := reflect.TypeOf(StateResult{})
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if name == "RequestID" || name == "Fingerprint" || name == "Rejection" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			r := StateResult{RequestID: "request"}
			field := reflect.ValueOf(&r).Elem().Field(i)
			switch field.Kind() {
			case reflect.Pointer:
				field.Set(reflect.New(field.Type().Elem()))
			case reflect.Slice:
				field.Set(reflect.MakeSlice(field.Type(), 1, 1))
			case reflect.Bool:
				field.SetBool(true)
			default:
				t.Fatalf("add nonzero fixture for %s", field.Type())
			}
			require.IsType(t, StateResult{}, storedStateReceipt(r))
		})
	}
}

func TestMaintenanceReceiptEncodingRetainsAdditionalFields(t *testing.T) {
	base := StateResult{RequestID: "request", Fingerprint: [32]byte{1}, Job: &JobResult{Deletion: &JobDeletion{TenantID: "alpha", RunID: "done"}}}
	// Every other current or future JobResult field must prevent narrowing.
	typ := reflect.TypeOf(*base.Job)
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).Name == "Deletion" {
			continue
		}
		t.Run(typ.Field(i).Name, func(t *testing.T) {
			r := base
			job := *base.Job
			r.Job = &job
			field := reflect.ValueOf(r.Job).Elem().Field(i)
			if field.Kind() == reflect.Pointer {
				field.Set(reflect.New(field.Type().Elem()))
			} else {
				field.SetInt(1)
			}
			require.IsType(t, StateResult{}, storedStateReceipt(r))
		})
	}
	for _, r := range []StateResult{
		{RequestID: "request", Compacted: true, Job: base.Job},
		{RequestID: "request", Rejection: &CommandRejection{Code: "invalid"}, Job: base.Job},
		{RequestID: "request", Channel: &CommunicationChannel{ID: "gateway"}, Job: base.Job},
		{RequestID: "request", Run: &Run{ID: "legacy"}, Job: base.Job},
		{RequestID: "request", Runs: []Run{{ID: "legacy"}}, Job: base.Job},
	} {
		require.IsType(t, StateResult{}, storedStateReceipt(r))
	}
}
