package db

import "reflect"

// Storage projections preserve compactJobReceipt semantics. Gob's field-name
// matching lets existing readers decode every projection.
// Omitting unused schema branches matters for records whose identities remain
// after payload compaction. Unknown nonzero fields always retain the full type.
type runReceipt struct {
	RequestID       string
	Fingerprint     [32]byte
	TenantID, RunID string
	HasRun          bool
	Step            uint64
	Status          JobStatus
	EventID         string
}

type configurationReceipt struct {
	RequestID     string
	Fingerprint   [32]byte
	TenantID      string
	Configuration *compactJobConfigurationReceipt
}

type activationAck struct {
	RequestID                       string
	Fingerprint                     [32]byte
	TenantID, RunID                 string
	HasRun                          bool
	Step                            uint64
	Status                          JobStatus
	EventID                         string
	TriggerID, Source, ActivationID string
	TriggerRevision                 uint64
}

func storedCompactReceipt(c *compactJobReceipt) any {
	rest := *c
	rest.RequestID, rest.Fingerprint, rest.TenantID = "", [32]byte{}, ""
	if c.Configuration != nil {
		rest.Configuration = nil
		if reflect.ValueOf(rest).IsZero() {
			return configurationReceipt{RequestID: c.RequestID, Fingerprint: c.Fingerprint,
				TenantID: c.TenantID, Configuration: c.Configuration}
		}
		return c
	}
	rest.RunID, rest.HasRun, rest.Step, rest.Status, rest.EventID = "", false, 0, "", ""
	if reflect.ValueOf(rest).IsZero() {
		return runReceipt{RequestID: c.RequestID, Fingerprint: c.Fingerprint,
			TenantID: c.TenantID, RunID: c.RunID, HasRun: c.HasRun, Step: c.Step,
			Status: c.Status, EventID: c.EventID}
	}
	rest.TriggerID, rest.Source, rest.ActivationID, rest.TriggerRevision = "", "", "", 0
	if reflect.ValueOf(rest).IsZero() {
		return activationAck{RequestID: c.RequestID, Fingerprint: c.Fingerprint,
			TenantID: c.TenantID, RunID: c.RunID, HasRun: c.HasRun, Step: c.Step,
			Status: c.Status, EventID: c.EventID, TriggerID: c.TriggerID,
			Source: c.Source, ActivationID: c.ActivationID, TriggerRevision: c.TriggerRevision}
	}
	return c
}
