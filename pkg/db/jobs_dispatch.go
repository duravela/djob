package db

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"go.etcd.io/bbolt"
)

func prefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 255 {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// ListDispatchJobs is a trusted worker discovery query, never a public tenant
// list. The global ready index is already maintained by v8 writers. Skipping a
// locally full tenant seeks past its entire namespace rather than walking every
// queued run. At most limit rows and len(skipTenants) skipped namespaces are read.
func (q *DB) ListDispatchJobs(after []byte, limit int, skipTenants []string) (JobRecordPage, error) {
	if err := queryLimit(limit); err != nil {
		return JobRecordPage{}, err
	}
	if len(skipTenants) > 1024 {
		return JobRecordPage{}, fmt.Errorf("too many dispatch tenant exclusions")
	}
	skipped := make(map[string]bool, len(skipTenants))
	for _, id := range skipTenants {
		if !validIdentity(id) {
			return JobRecordPage{}, fmt.Errorf("invalid dispatch tenant exclusion")
		}
		skipped[id] = true
	}
	page := JobRecordPage{}
	err := q.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(jobReadyBucket).Cursor()
		key, value := pageStart(cursor, after)
		for key != nil && len(page.Entries) < limit {
			if len(key) < 4 {
				return fmt.Errorf("invalid ready job tenant key")
			}
			n := uint64(binary.BigEndian.Uint32(key[:4]))
			if n == 0 || n > 256 || n+4 > uint64(len(key)) {
				return fmt.Errorf("invalid ready job tenant prefix")
			}
			tenant := string(key[4 : 4+n])
			if skipped[tenant] {
				end := prefixEnd(key[:4+n])
				if end == nil {
					key = nil
					break
				}
				key, value = cursor.Seek(end)
				continue
			}
			run, err := readRecord[JobRun](tx, jobRunsBucket, string(value))
			if err != nil {
				return err
			}
			if run.TenantID != tenant || run.Status != JobReady {
				return fmt.Errorf("inconsistent ready job index")
			}
			page.Entries = append(page.Entries, JobRecordEntry{Record: JobRecord{Run: run}, Cursor: bytes.Clone(key)})
			page.NextCursor = bytes.Clone(key)
			key, value = cursor.Next()
		}
		if key == nil {
			page.NextCursor = nil
		}
		return nil
	})
	return page, err
}
