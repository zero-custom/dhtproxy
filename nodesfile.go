package main

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	adht "github.com/anacrolix/dht/v2"
	"github.com/anacrolix/dht/v2/krpc"
)

// nodeSaveInterval is the period between routing-table persists. A constant,
// not a flag: table churn is slow, so there is nothing worth tuning here;
// change this one line if the trade-off ever shifts.
const nodeSaveInterval = 15 * time.Minute

// nodesTable is the Nodes/AddNode surface of *adht.Server used by the
// persistence helpers. *adht.Server satisfies it; tests stub it so no UDP
// or DHT network is needed.
type nodesTable interface {
	Nodes() []krpc.NodeInfo
	AddNode(krpc.NodeInfo) error
}

// deriveV6Path derives the v6 nodes-file path from the v4 one: a single
// --dhtNodesFile flag covers both tables, whose lifetimes always match.
// Empty in means disabled, so empty out.
func deriveV6Path(path string) string {
	if path == "" {
		return ""
	}
	return path + ".v6"
}

// saveNodesAtomic snapshots srv's routing table and persists it to path via
// a temp file in the same directory plus rename. The library's
// WriteNodesToFile truncates in place, so a crash mid-write would destroy
// the live file; pointing the library at the temp file and renaming over
// the target keeps the previous snapshot intact on crash. All errors are
// returned for the caller to log; nothing here is fatal and nothing logs.
func saveNodesAtomic(srv nodesTable, path string) error {
	if path == "" {
		return errors.New("nodesfile: empty path")
	}
	nodes := srv.Nodes()
	tmp, err := os.CreateTemp(filepath.Dir(path), ".nodes-tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := adht.WriteNodesToFile(nodes, tmpName); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

// loadNodes reads a nodes file back into srv's routing table. v6 selects
// the family filter: the v4 file keeps To4()!=nil entries, the v6 file
// keeps true-v6 (To4()==nil) entries, because the library mixes families in
// Nodes() and does no family check in AddNode — the split must happen here
// or the v6 table gets polluted with v4 entries. It returns how many
// entries were accepted.
//
// Missing file is a silent cold start ((0, nil), today's behavior).
// A truncated/corrupt tail still loads the decodable prefix and returns the
// read error alongside the count, so the caller warns and keeps running on
// the prefix instead of dying.
func loadNodes(srv nodesTable, path string, v6 bool) (int, error) {
	ns, readErr := adht.ReadNodesFromFile(path)
	if readErr != nil && os.IsNotExist(readErr) {
		return 0, nil
	}
	loaded := 0
	for _, ni := range ns {
		if isV4 := ni.Addr.IP.To4() != nil; v6 == isV4 {
			continue
		}
		if srv.AddNode(ni) == nil {
			loaded++
		}
	}
	return loaded, readErr
}
