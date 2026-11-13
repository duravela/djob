package store

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func checkStateEncoding(t *testing.T, state db.StateCommand) {
	t.Helper()
	command := db.Command{Version: db.CurrentCommandVersion, Type: db.CmdState, State: &state}
	var legacy bytes.Buffer
	require.NoError(t, gob.NewEncoder(&legacy).Encode(command))
	encoded, err := encodeStateProposal(command)
	require.NoError(t, err)
	var old, projected db.Command
	require.NoError(t, gob.NewDecoder(bytes.NewReader(legacy.Bytes())).Decode(&old))
	// The original decoder must read both encodings without a special path.
	require.NoError(t, gob.NewDecoder(bytes.NewReader(encoded)).Decode(&projected))
	require.Equal(t, old, projected)
	if state.Validate() == nil {
		before, err := state.Fingerprint()
		require.NoError(t, err)
		after, err := projected.State.Fingerprint()
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
	t.Logf("proposal encoding: %d -> %d bytes", legacy.Len(), len(encoded))
}

func TestSparseStateProposalPreservesEveryOperation(t *testing.T) {
	base := db.StateCommand{RequestID: "request", At: time.Now()}
	// Include every compiled field, including metadata and future operations.
	for _, typ := range []reflect.Type{reflect.TypeFor[db.StateCommand](), reflect.TypeFor[db.JobCommand]()} {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.Type.Kind() != reflect.Pointer {
				continue
			}
			t.Run(typ.Name()+"/"+field.Name, func(t *testing.T) {
				state := base
				var v reflect.Value
				if typ == reflect.TypeFor[db.StateCommand]() {
					v = reflect.ValueOf(&state).Elem()
				} else {
					state.Job = &db.JobCommand{}
					v = reflect.ValueOf(state.Job).Elem()
				}
				v.Field(i).Set(reflect.New(field.Type.Elem()))
				checkStateEncoding(t, state)
			})
		}
	}
}

func TestSparseStateProposalPreservesPayloadAndWorkerAdmission(t *testing.T) {
	owner := db.JobOwner{TenantID: "tenant", RunID: "run", AttemptID: "attempt", OwnerID: "worker"}
	for _, command := range []db.StateCommand{
		{Job: &db.JobCommand{Claim: &db.ClaimJob{JobOwner: owner, LeaseUntil: time.Now().Add(time.Minute)}}, WorkerAdmission: &db.WorkerAdmission{WorkerID: "worker", CredentialHash: [32]byte{1}, Enrolled: true, DeploymentID: "release", DeploymentGeneration: 2}},
		{Job: &db.JobCommand{Advance: &db.AdvanceJob{JobOwner: owner, ExpectedStep: 7, Kind: db.JobComplete, Status: db.JobSucceeded, State: []byte{0, 255}, Result: []byte("result"), Quiescent: true}}},
		{Job: &db.JobCommand{PutDefinition: &db.PutJobDefinition{ExpectedRevision: 3, Definition: db.JobDefinition{TenantID: "tenant", ID: "work", Runtime: db.StepRuntime, Handler: "hash", HandlerVersion: "1", Input: []byte{0, 255}, WorkerReleaseWeights: map[string]uint32{"v1": 9000, "v2": 1000}}}}},
	} {
		command.RequestID, command.At = "request", time.Now()
		require.NoError(t, command.Validate())
		checkStateEncoding(t, command)
		encoded, err := encodeStateProposal(db.Command{Version: command.CommandVersion(), Type: db.CmdState, State: &command})
		require.NoError(t, err)
		var legacy bytes.Buffer
		require.NoError(t, gob.NewEncoder(&legacy).Encode(db.Command{Version: command.CommandVersion(), Type: db.CmdState, State: &command}))
		require.Less(t, len(encoded), legacy.Len()/2)
	}
}
