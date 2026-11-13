package db

import (
	"crypto/sha256"
	"encoding/binary"
	"go.etcd.io/bbolt"
	"sort"
)

// Weights use basis points. No fallback redistribution occurs when a release
// closes admission: the operator must first update the policy and its triggers.
func checkWorkerRouting(tx *bbolt.Tx, tenant string, d JobDefinition) error {
	if len(d.WorkerReleaseWeights) == 0 {
		return checkNewWorkerReleaseRun(tx, tenant, d.WorkerRelease)
	}
	if d.WorkerRelease != "" || len(d.WorkerReleaseWeights) > 8 {
		return reject("invalid", "choose one release or at most eight weighted releases")
	}
	releases := make([]string, 0, len(d.WorkerReleaseWeights))
	for release := range d.WorkerReleaseWeights {
		releases = append(releases, release)
	}
	sort.Strings(releases)
	total := uint64(0)
	for _, release := range releases {
		weight := d.WorkerReleaseWeights[release]
		if !validWorkerID(release) || weight == 0 || weight > 10000 {
			return reject("invalid", "release weights must be positive basis points")
		}
		total += uint64(weight)
		if err := checkNewWorkerReleaseRun(tx, tenant, release); err != nil {
			return err
		}
	}
	if total != 10000 {
		return reject("invalid", "release weights must sum to 10000 basis points")
	}
	return nil
}

func routedRelease(tenant, id string, d JobDefinition) string {
	if len(d.WorkerReleaseWeights) == 0 {
		return d.WorkerRelease
	}
	sum := sha256.Sum256([]byte(jobKey(tenant, id, d.ID)))
	bucket := binary.BigEndian.Uint64(sum[:8]) % 10000
	releases := make([]string, 0, len(d.WorkerReleaseWeights))
	for release := range d.WorkerReleaseWeights {
		releases = append(releases, release)
	}
	sort.Strings(releases)
	for _, release := range releases {
		weight := uint64(d.WorkerReleaseWeights[release])
		if bucket < weight {
			return release
		}
		bucket -= weight
	}
	return "" // Invalid policies cannot enter the catalog.
}
