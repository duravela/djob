package commit

// Entry carries the original Raft index. Empty data still advances application
// progress, for example for elections and membership changes.
type Entry struct {
	Index uint64
	Data  []byte
}

type Snapshot struct {
	Index uint64
	Data  []byte
}

// Recovery describes the WAL-selected snapshot and the durable commit boundary.
// An application database beyond this boundary belongs to incompatible state.
type Recovery struct {
	Snapshot       *Snapshot
	CommittedIndex uint64
}

type Commit struct {
	Entries     []Entry
	Snapshot    *Snapshot
	Recovery    *Recovery
	ApplyDoneC  chan<- struct{}
	ApplyErrorC chan<- error
}
