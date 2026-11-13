package db

import (
	"bufio"
	"bytes"
	"encoding"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math/bits"
	"reflect"
	"sync"

	"go.etcd.io/bbolt"
)

// Gob stages an entire value to prefix it with its length. For the fixed
// version-1 snapshot schema, count against the pinned Bolt view, then stream the
// same bytes without constructing a record tree or another payload-sized buffer.
// Type descriptors and their process-local IDs come from the public encoder.
// Restore continues to use the unmodified Gob decoder.
type snapshotGobTemplate struct {
	types   []byte
	valueID []byte
}

var snapshotEncoding = sync.OnceValue(func() *snapshotGobTemplate {
	if CurrentSnapshotVersion != 1 || !snapshotGobSchemaSupported(
		reflect.TypeFor[Snapshot](), reflect.TypeFor[SnapshotBucket](), reflect.TypeFor[SnapshotRecord](),
	) {
		return nil
	}
	var encoded bytes.Buffer
	if err := gob.NewEncoder(&encoded).Encode(Snapshot{}); err != nil {
		return nil
	}
	return parseSnapshotGobTemplate(encoded.Bytes())
})

func snapshotGobSchemaSupported(root, bucket, record reflect.Type) bool {
	for _, t := range []reflect.Type{root, bucket, record} {
		if t.Kind() != reflect.Struct {
			return false
		}
		for _, custom := range []reflect.Type{
			reflect.TypeFor[gob.GobEncoder](), reflect.TypeFor[encoding.BinaryMarshaler](),
			reflect.TypeFor[encoding.TextMarshaler](),
			reflect.TypeFor[gob.GobDecoder](), reflect.TypeFor[encoding.BinaryUnmarshaler](),
			reflect.TypeFor[encoding.TextUnmarshaler](),
		} {
			if t.Implements(custom) || reflect.PointerTo(t).Implements(custom) {
				return false
			}
		}
	}
	type field struct {
		name   string
		typeOf reflect.Type
	}
	matches := func(t reflect.Type, fields []field) bool {
		if t.NumField() != len(fields) {
			return false
		}
		for i, want := range fields {
			got := t.Field(i)
			if got.Name != want.name || got.Type != want.typeOf || got.Anonymous {
				return false
			}
		}
		return true
	}
	u64, raw := reflect.TypeFor[uint64](), reflect.TypeFor[[]byte]()
	return matches(root, []field{
		{"Version", reflect.TypeFor[uint32]()}, {"LastIndex", u64}, {"Buckets", reflect.SliceOf(bucket)},
	}) && matches(bucket, []field{
		{"Name", raw}, {"Sequence", u64}, {"Records", reflect.SliceOf(record)}, {"Buckets", reflect.SliceOf(bucket)},
	}) && matches(record, []field{{"Key", raw}, {"Value", raw}})
}

// Accept only type-definition frames followed by a single empty struct value.
// Unexpected encoder output disables the optimization instead of guessing IDs.
func parseSnapshotGobTemplate(encoded []byte) *snapshotGobTemplate {
	for offset := 0; offset < len(encoded); {
		start := offset
		length, n := snapshotGobUint(encoded[offset:])
		if n == 0 || length > uint64(len(encoded)-offset-n) {
			return nil
		}
		offset += n
		end := offset + int(length)
		id, idLen := snapshotGobUint(encoded[offset:end])
		if idLen == 0 || id == 0 {
			return nil
		}
		if id&1 == 0 { // Positive signed type ID: the value, not a definition.
			if start == 0 || end != len(encoded) || end-offset-idLen != 1 || encoded[end-1] != 0 {
				return nil
			}
			return &snapshotGobTemplate{types: bytes.Clone(encoded[:start]), valueID: bytes.Clone(encoded[offset : offset+idLen])}
		}
		offset = end
	}
	return nil
}

func snapshotGobUint(encoded []byte) (uint64, int) {
	if len(encoded) == 0 {
		return 0, 0
	}
	if encoded[0] < 128 {
		return uint64(encoded[0]), 1
	}
	n := -int(int8(encoded[0]))
	if n > 8 || len(encoded) < n+1 {
		return 0, 0
	}
	var value uint64
	for _, b := range encoded[1 : n+1] {
		value = value<<8 | uint64(b)
	}
	return value, n + 1
}

// Match encoding/gob's message limit on both 32-bit and 64-bit architectures.
const snapshotGobMessageLimit = uint64((1 << 30) << (^uint(0) >> 62))

type snapshotGobWriter struct {
	output  *bufio.Writer // nil counts the exact payload size without retaining it
	size    uint64
	err     error
	scratch [9]byte
}

type snapshotCheckedWriter struct{ io.Writer }

func (w snapshotCheckedWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

func (w *snapshotGobWriter) raw(p []byte) {
	if w.err != nil {
		return
	}
	if w.output == nil {
		if uint64(len(p)) >= snapshotGobMessageLimit-w.size {
			w.err = fmt.Errorf("gob: encoder: message too big")
			return
		}
	} else if _, err := w.output.Write(p); err != nil {
		w.err = err
		return
	}
	w.size += uint64(len(p))
}

func (w *snapshotGobWriter) uint(value uint64) {
	if value < 128 {
		w.scratch[0] = byte(value)
		w.raw(w.scratch[:1])
		return
	}
	binary.BigEndian.PutUint64(w.scratch[1:], value)
	leading := bits.LeadingZeros64(value) / 8
	w.scratch[leading] = byte(leading - 8)
	w.raw(w.scratch[leading:])
}

func (w *snapshotGobWriter) field(last *int, field int) {
	w.uint(uint64(field - *last))
	*last = field
}

func (w *snapshotGobWriter) bytes(value []byte) {
	w.uint(uint64(len(value)))
	w.raw(value)
}

func writeSnapshot(output io.Writer, tx *bbolt.Tx, index uint64) error {
	template := snapshotEncoding()
	if template == nil {
		return writeSnapshotNative(output, tx, index)
	}
	var count snapshotGobWriter
	count.raw(template.valueID)
	if err := writeSnapshotGobBody(&count, tx, index); err != nil {
		return err
	}
	var header snapshotGobWriter
	header.uint(count.size)
	total := uint64(len(template.types)) + header.size + count.size
	if buffer, ok := output.(*bytes.Buffer); ok {
		if total > uint64(int(^uint(0)>>1)-buffer.Len()) {
			return fmt.Errorf("snapshot exceeds buffer size limit")
		}
		buffer.Grow(int(total))
	}
	w := snapshotGobWriter{output: bufio.NewWriterSize(snapshotCheckedWriter{output}, 64<<10)}
	w.raw(template.types)
	w.uint(count.size)
	w.raw(template.valueID)
	if err := writeSnapshotGobBody(&w, tx, index); err != nil {
		return err
	}
	if w.size != total {
		return fmt.Errorf("snapshot length mismatch: counted=%d written=%d", total, w.size)
	}
	return w.output.Flush()
}

func writeSnapshotGobBody(w *snapshotGobWriter, tx *bbolt.Tx, index uint64) error {
	if w.err != nil {
		return w.err
	}
	last := -1
	w.field(&last, 0)
	w.uint(uint64(CurrentSnapshotVersion))
	if index != 0 {
		w.field(&last, 1)
		w.uint(index)
	}
	var buckets uint64
	if err := tx.ForEach(func([]byte, *bbolt.Bucket) error { buckets++; return nil }); err != nil {
		return err
	}
	if buckets != 0 {
		w.field(&last, 2)
		w.uint(buckets)
		if err := tx.ForEach(func(name []byte, bucket *bbolt.Bucket) error {
			writeSnapshotGobBucket(w, name, bucket)
			return w.err
		}); err != nil {
			return err
		}
	}
	w.uint(0) // Struct terminator.
	return w.err
}

func writeSnapshotGobBucket(w *snapshotGobWriter, name []byte, bucket *bbolt.Bucket) {
	last := -1
	if len(name) != 0 {
		w.field(&last, 0)
		w.bytes(name)
	}
	if sequence := bucket.Sequence(); sequence != 0 {
		w.field(&last, 1)
		w.uint(sequence)
	}
	var records, children uint64
	cursor := bucket.Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		if value == nil {
			children++
		} else {
			records++
		}
	}
	if records != 0 {
		w.field(&last, 2)
		w.uint(records)
		for key, value := cursor.First(); key != nil && w.err == nil; key, value = cursor.Next() {
			if value == nil {
				continue
			}
			recordLast := -1
			w.field(&recordLast, 0)
			w.bytes(key) // Bolt keys are nonempty.
			if len(value) != 0 {
				w.field(&recordLast, 1)
				w.bytes(value)
			}
			w.uint(0)
		}
	}
	if children != 0 {
		w.field(&last, 3)
		w.uint(children)
		for key, value := cursor.First(); key != nil && w.err == nil; key, value = cursor.Next() {
			if value == nil {
				writeSnapshotGobBucket(w, key, bucket.Bucket(key))
			}
		}
	}
	w.uint(0)
}
