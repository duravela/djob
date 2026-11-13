package node

// Private fault seam for durability regressions. Production nodes leave it nil.
// WAL hooks run on the serialized Ready loop. Snapshot-file hooks can also run
// on the single background encoder; test hooks must synchronize shared state.
type persistenceFault struct{ before func(string) error }
