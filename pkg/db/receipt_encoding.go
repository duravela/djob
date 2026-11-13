package db

import "reflect"

// Gob matches struct fields by name. Narrow maintenance envelopes retain every
// acknowledgement field while omitting unrelated, nil branches of JobResult's
// type schema. Existing StateResult decoders can read these envelopes unchanged.
// This matters after history cleanup: its own audit receipts remain permanent.
type maintenanceReceipt[T any] struct {
	RequestID   string
	Fingerprint [32]byte
	Job         *T
}
type deletionReceiptResult struct{ Deletion *JobDeletion }
type compactionReceiptResult struct{ ReceiptCompaction *JobReceiptCompaction }
type configurationCompactionReceiptResult struct {
	ConfigurationReceiptCompaction *JobConfigurationReceiptCompaction
}

type identityReceipt struct {
	RequestID   string
	Fingerprint [32]byte
}
type rejectionReceipt struct {
	RequestID   string
	Fingerprint [32]byte
	Rejection   *CommandRejection
}

func storedStateReceipt(r StateResult) any {
	if r.Job == nil {
		outer := r
		outer.RequestID, outer.Fingerprint, outer.Rejection = "", [32]byte{}, nil
		if reflect.ValueOf(outer).IsZero() {
			if r.Rejection != nil {
				return rejectionReceipt{r.RequestID, r.Fingerprint, r.Rejection}
			}
			return identityReceipt{r.RequestID, r.Fingerprint}
		}
		return r
	}
	outer := r
	outer.RequestID = ""
	outer.Fingerprint = [32]byte{}
	outer.Job = nil
	// Additional fields, including future fields, always use the full envelope.
	if !reflect.ValueOf(outer).IsZero() {
		return r
	}
	if r.Job.Deletion != nil {
		rest := *r.Job
		rest.Deletion = nil
		if reflect.ValueOf(rest).IsZero() {
			return maintenanceReceipt[deletionReceiptResult]{r.RequestID, r.Fingerprint, &deletionReceiptResult{r.Job.Deletion}}
		}
	}
	if r.Job.ReceiptCompaction != nil {
		rest := *r.Job
		rest.ReceiptCompaction = nil
		if reflect.ValueOf(rest).IsZero() {
			return maintenanceReceipt[compactionReceiptResult]{r.RequestID, r.Fingerprint, &compactionReceiptResult{r.Job.ReceiptCompaction}}
		}
	}
	if r.Job.ConfigurationReceiptCompaction != nil {
		rest := *r.Job
		rest.ConfigurationReceiptCompaction = nil
		if reflect.ValueOf(rest).IsZero() {
			return maintenanceReceipt[configurationCompactionReceiptResult]{r.RequestID, r.Fingerprint, &configurationCompactionReceiptResult{r.Job.ConfigurationReceiptCompaction}}
		}
	}
	return r
}
