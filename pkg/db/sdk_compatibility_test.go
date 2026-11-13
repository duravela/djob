package db

import (
	"encoding/gob"
	"encoding/json"
	"os"
	"testing"
)

// These fixtures were written by the implementation before the SDK extraction.
// They guard persisted command/record decoding and the public JSON field shape
// independently of the new type definitions and engine aliases.
func TestPreSDKJobContractsRemainReadable(t *testing.T) {
	f, err := os.Open("testdata/pre-sdk-contracts.gob")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var value struct {
		Commands []StateCommand
		Run      JobRun
		Event    JobEvent
		Resource JobResource
	}
	decoder := gob.NewDecoder(f)
	for _, target := range []any{&value.Commands, &value.Run, &value.Event, &value.Resource} {
		if err := decoder.Decode(target); err != nil {
			t.Fatalf("decode pre-SDK data: %v", err)
		}
	}
	got, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/pre-sdk-contracts.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(append(got, '\n')) != string(want) {
		t.Fatal("pre-SDK command or job data changed after decoding")
	}
}
