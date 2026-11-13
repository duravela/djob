package node

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
)

type memberIdentity struct {
	Version        int      `json:"version"`
	MemberID       uint64   `json:"member_id,string"`
	ClusterID      uint64   `json:"cluster_id,string"`
	Peers          []string `json:"peers"`
	InitialMembers []uint64 `json:"initial_members,omitempty"`
}

func readMemberIdentity(directory string) (identity memberIdentity, err error) {
	path := filepath.Join(directory, ".djob-cluster.json")
	info, err := os.Lstat(path)
	if err != nil {
		return identity, err
	}
	if !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return identity, fmt.Errorf("invalid member identity file")
	}
	f, err := os.Open(path)
	if err != nil {
		return identity, err
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&identity); err != nil {
		return identity, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return identity, fmt.Errorf("trailing member identity content")
	}
	if (identity.Version != 1 && identity.Version != 2) || identity.MemberID == 0 || identity.MemberID > uint64(len(identity.Peers)) || identity.ClusterID == 0 {
		return identity, fmt.Errorf("invalid member identity")
	}
	return identity, nil
}

func writeIdentity(directory string, identity memberIdentity) error {
	data, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(directory, ".identity-pending-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := publishMaintenanceDirectory(f.Name(), filepath.Join(directory, ".djob-cluster.json")); err != nil {
		return err
	}
	return syncMaintenanceDirectory(directory)
}

func (rc *RaftNode) validateMemberIdentity(directory string) error {
	if _, err := os.Lstat(filepath.Join(directory, ".djob-restore-pending")); err == nil {
		return fmt.Errorf("prepared member restore has not been activated")
	} else if !os.IsNotExist(err) {
		return err
	}
	if _, err := os.Lstat(filepath.Join(directory, ".djob-transferred")); err == nil {
		return fmt.Errorf("member state was transferred; the retired source must never restart")
	} else if !os.IsNotExist(err) {
		return err
	}
	for _, name := range []string{backupManifestName, ".djob-manifest.sha256", ".djob-restored"} {
		if _, err := os.Lstat(filepath.Join(directory, name)); err == nil {
			return fmt.Errorf("restore the backup artifact before starting a member")
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	path := filepath.Join(directory, ".djob-cluster.json")
	_, err := os.Lstat(path)
	if os.IsNotExist(err) {
		identity := memberIdentity{Version: 1, MemberID: rc.raftCfg.ID, ClusterID: rc.raftCfg.ClusterID, Peers: rc.raftCfg.Peers}
		if rc.raftCfg.DynamicMembership {
			identity.Version, identity.InitialMembers = 2, rc.raftCfg.InitialMembers
		}
		return writeIdentity(directory, identity)
	}
	if err != nil {
		return err
	}
	identity, err := readMemberIdentity(directory)
	if err != nil {
		return err
	}
	if (identity.Version == 2) != rc.raftCfg.DynamicMembership || !reflect.DeepEqual(identity.InitialMembers, rc.raftCfg.InitialMembers) {
		return fmt.Errorf("dynamic membership mode or bootstrap voters differ from persistent identity")
	}
	if identity.MemberID != rc.raftCfg.ID || !reflect.DeepEqual(identity.Peers, rc.raftCfg.Peers) {
		return fmt.Errorf("member identity or ordered peers differ from the stored cluster; use an explicit recovery procedure")
	}
	if rc.raftCfg.ClusterID != 0x1000 && rc.raftCfg.ClusterID != identity.ClusterID {
		return fmt.Errorf("configured cluster ID differs from persistent identity")
	}
	rc.raftCfg.ClusterID = identity.ClusterID
	return nil
}
