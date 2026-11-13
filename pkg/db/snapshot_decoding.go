package db

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"go.etcd.io/bbolt"
)

// RestoreBytesAt restores an already buffered Raft snapshot without copying its
// version-1 Gob message or constructing a second tree of records. The caller
// must keep data immutable until this method returns. No bytes are retained
// after the transaction commits. Other Gob schemas use the existing decoder.
func (q *DB) RestoreBytesAt(data []byte, index uint64) error {
	return q.restoreBytes(data, &index)
}

func (q *DB) restoreBytes(data []byte, expected *uint64) error {
	payload, ok := snapshotGobPayload(data)
	if !ok {
		return q.restore(bytes.NewReader(data), expected)
	}
	r := snapshotGobReader{data: payload}
	var version uint32
	var index, buckets uint64
	last, hasBuckets := -1, false
	for field := r.field(&last, 3); field >= 0; field = r.field(&last, 3) {
		switch field {
		case 0:
			u := r.uint()
			if u > math.MaxUint32 {
				r.fail("snapshot version overflows uint32")
			}
			version = uint32(u)
		case 1:
			index = r.uint()
		case 2:
			buckets, hasBuckets = r.count(), true
		}
		if hasBuckets {
			break
		}
	}
	if r.err != nil {
		return fmt.Errorf("decode snapshot: %w", r.err)
	}
	if err := validateSnapshotIndex(version, index, expected); err != nil {
		return err
	}
	return q.replaceSnapshot(index, func(tx *bbolt.Tx) error {
		for range buckets {
			if err := r.bucket(tx.CreateBucket, 0); err != nil {
				return err
			}
		}
		if hasBuckets && r.field(&last, 3) >= 0 {
			r.fail("unexpected snapshot field after buckets")
		}
		// Like gob.Decoder.Decode, consume the first value only. A writer may
		// have appended another value to the stream or padding to its frame.
		return r.err
	})
}

type snapshotGobReader struct {
	data []byte
	err  error
}

func (r *snapshotGobReader) fail(message string) {
	if r.err == nil {
		r.err = fmt.Errorf("decode snapshot: %s", message)
	}
}

func (r *snapshotGobReader) uint() uint64 {
	if r.err != nil {
		return 0
	}
	value, n := snapshotGobUint(r.data)
	if n == 0 {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	r.data = r.data[n:]
	return value
}

func (r *snapshotGobReader) take(length uint64) []byte {
	if r.err != nil {
		return nil
	}
	if length > uint64(len(r.data)) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	p := r.data[:int(length)]
	r.data = r.data[int(length):]
	return p
}

func (r *snapshotGobReader) bytes() []byte { return r.take(r.uint()) }

func (r *snapshotGobReader) count() uint64 {
	count := r.uint()
	// Each inline struct element needs at least its terminator. Check the
	// count before allocating or walking records from damaged input.
	if count > uint64(len(r.data)) {
		r.fail("element count exceeds remaining message")
	}
	return count
}

func (r *snapshotGobReader) field(last *int, fields int) int {
	delta := r.uint()
	if r.err != nil || delta == 0 {
		return -1
	}
	if delta >= uint64(fields-*last) {
		r.fail("struct field outside snapshot schema")
		return -1
	}
	*last += int(delta)
	return *last
}

func (r *snapshotGobReader) positiveID() uint64 {
	u := r.uint()
	if u == 0 || u&1 != 0 || u>>1 > math.MaxInt32 {
		r.fail("invalid Gob type ID")
	}
	return u >> 1
}

func (r *snapshotGobReader) bucket(create func([]byte) (*bbolt.Bucket, error), depth int) error {
	if depth > 10000 {
		return fmt.Errorf("snapshot bucket nesting exceeds 10000")
	}
	var target *bbolt.Bucket
	var name []byte
	var sequence uint64
	ensure := func() error {
		if target == nil {
			var err error
			target, err = create(name)
			if err != nil {
				return err
			}
			return target.SetSequence(sequence)
		}
		return nil
	}
	last := -1
	for field := r.field(&last, 4); field >= 0; field = r.field(&last, 4) {
		switch field {
		case 0:
			name = r.bytes()
		case 1:
			sequence = r.uint()
		case 2, 3:
			count := r.count()
			if r.err != nil {
				return r.err
			}
			if err := ensure(); err != nil {
				return err
			}
			if field == 2 {
				keys := make(map[string]bool)
				for range count {
					var key, value []byte
					recordLast := -1
					for f := r.field(&recordLast, 2); f >= 0; f = r.field(&recordLast, 2) {
						if f == 0 {
							key = r.bytes()
						} else {
							value = r.bytes()
						}
					}
					if r.err != nil {
						return r.err
					}
					if keys[string(key)] {
						return fmt.Errorf("duplicate snapshot key %q", key)
					}
					keys[string(key)] = true
					if err := target.Put(key, value); err != nil {
						return err
					}
				}
			} else {
				for range count {
					if err := r.bucket(target.CreateBucket, depth+1); err != nil {
						return err
					}
				}
			}
		}
	}
	if r.err != nil {
		return r.err
	}
	return ensure()
}

type snapshotWireField struct {
	name []byte
	id   uint64
}

type snapshotWireType struct {
	id     uint64
	kind   int // Gob wireType fields: SliceT=1, StructT=2.
	elem   uint64
	fields []snapshotWireField
}

// Read only the small public Gob type descriptions, then verify the complete
// graph before interpreting any values. User-assigned IDs and definition order
// may differ between processes. Unsupported layouts go to the native decoder.
func snapshotGobPayload(data []byte) ([]byte, bool) {
	if snapshotEncoding() == nil {
		return nil, false
	}
	r := snapshotGobReader{data: data}
	var types []snapshotWireType
	for len(r.data) > 0 && r.err == nil {
		length := r.uint()
		if length >= snapshotGobMessageLimit {
			return nil, false
		}
		frame := snapshotGobReader{data: r.take(length)}
		id := frame.uint()
		if r.err != nil || frame.err != nil || id == 0 || id>>1 > math.MaxInt32 {
			return nil, false
		}
		if id&1 == 0 {
			return frame.data, len(types) == 5 && snapshotWireSchema(types, id>>1)
		}
		if len(types) == 5 || len(frame.data) > 64<<10 {
			return nil, false
		}
		definition := frame.wireType(id>>1 + 1)
		if frame.err != nil || len(frame.data) != 0 {
			return nil, false
		}
		for _, old := range types {
			if old.id == definition.id {
				return nil, false
			}
		}
		types = append(types, definition)
	}
	return nil, false
}

func (r *snapshotGobReader) commonType() uint64 {
	var id uint64
	last := -1
	for field := r.field(&last, 2); field >= 0; field = r.field(&last, 2) {
		if field == 0 {
			r.bytes() // Type names do not affect Gob's structural compatibility.
		} else {
			id = r.positiveID()
		}
	}
	return id
}

func (r *snapshotGobReader) wireType(id uint64) snapshotWireType {
	t := snapshotWireType{id: id, kind: -1}
	outer := -1
	t.kind = r.field(&outer, 7)
	if t.kind != 1 && t.kind != 2 {
		r.fail("unsupported Gob wire type")
		return t
	}
	last := -1
	var common uint64
	for field := r.field(&last, 2); field >= 0; field = r.field(&last, 2) {
		if field == 0 {
			common = r.commonType()
		} else if t.kind == 1 {
			t.elem = r.positiveID()
		} else {
			count := r.count()
			if count > 4 {
				r.fail("unsupported Gob struct field count")
				return t
			}
			for range count {
				var f snapshotWireField
				fieldLast := -1
				for n := r.field(&fieldLast, 2); n >= 0; n = r.field(&fieldLast, 2) {
					if n == 0 {
						f.name = r.bytes()
					} else {
						f.id = r.positiveID()
					}
				}
				t.fields = append(t.fields, f)
			}
		}
	}
	if common != id || id < 64 || r.field(&outer, 7) >= 0 {
		r.fail("unsupported Gob type description")
	}
	return t
}

func snapshotWireSchema(types []snapshotWireType, rootID uint64) bool {
	find := func(id uint64) snapshotWireType {
		for _, t := range types {
			if t.id == id {
				return t
			}
		}
		return snapshotWireType{kind: -1}
	}
	matches := func(t snapshotWireType, names ...string) bool {
		if t.kind != 2 || len(t.fields) != len(names) {
			return false
		}
		for i, name := range names {
			if string(t.fields[i].name) != name {
				return false
			}
		}
		return true
	}
	root := find(rootID)
	if !matches(root, "Version", "LastIndex", "Buckets") {
		return false
	}
	buckets := find(root.fields[2].id)
	bucket := find(buckets.elem)
	if buckets.kind != 1 || !matches(bucket, "Name", "Sequence", "Records", "Buckets") {
		return false
	}
	records := find(bucket.fields[2].id)
	record := find(records.elem)
	if records.kind != 1 || !matches(record, "Key", "Value") {
		return false
	}
	// These are Gob's documented, fixed built-in IDs (not process-local IDs).
	const uintType, bytesType = 3, 5
	return root.fields[0].id == uintType && root.fields[1].id == uintType &&
		bucket.fields[0].id == bytesType && bucket.fields[1].id == uintType &&
		bucket.fields[3].id == buckets.id && record.fields[0].id == bytesType && record.fields[1].id == bytesType
}
