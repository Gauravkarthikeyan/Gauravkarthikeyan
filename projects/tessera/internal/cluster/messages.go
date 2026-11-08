// Package cluster assembles Tessera's processes — storage nodes, request
// routers, the metadata service, global admission control and clients — on
// top of the deterministic simulator, and provides the harness that runs a
// workload under faults and checks the resulting history.
package cluster

import (
	"github.com/Gauravkarthikeyan/tessera/internal/kv"
	"github.com/Gauravkarthikeyan/tessera/internal/raft"
	"github.com/Gauravkarthikeyan/tessera/internal/sim"
)

// Status is the outcome of a request as seen by a router or client.
type Status uint8

const (
	StOK          Status = iota // applied (or read) exactly once; Res is valid
	StThrottled                 // rejected by admission control; not applied
	StNotLeader                 // storage: this replica is not the leader
	StNoGroup                   // storage: this node does not host the group
	StWrongRange                // storage: key is not in the group's range
	StAmbiguous                 // may or may not have been applied
	StUnavailable               // router gave up; see MaybeApplied
)

func (s Status) String() string {
	return [...]string{"ok", "throttled", "not-leader", "no-group", "wrong-range", "ambiguous", "unavailable"}[s]
}

// --- client ↔ router --------------------------------------------------------

// ClientReq is sent by a client to a request router.
type ClientReq struct {
	Table   string
	Op      kv.Op
	Attempt int  // client-side attempt number; (Client, Seq, Attempt) is unique
	Poison  bool // a "query of death" that crashes whichever router parses it
}

// ClientResp answers a ClientReq.
type ClientResp struct {
	Seq          uint64
	Attempt      int
	Status       Status
	Res          kv.Result
	MaybeApplied bool // true if a non-OK outcome might nevertheless have been applied
}

// --- router ↔ storage -------------------------------------------------------

// StoreReq is sent by a router to a storage node.
type StoreReq struct {
	RID     uint64 // router-local request id
	Attempt int    // router-local attempt; (router, RID, Attempt) is unique
	Group   kv.GroupID
	Op      kv.Op
}

// StoreResp answers a StoreReq.
type StoreResp struct {
	RID        uint64
	Attempt    int
	Group      kv.GroupID
	Status     Status
	Res        kv.Result
	LeaderHint sim.NodeID
}

// RaftMsg carries a Raft RPC for one group between storage nodes.
type RaftMsg struct {
	Group kv.GroupID
	M     raft.Message
}

// --- control plane ----------------------------------------------------------

// GroupsReport is periodically sent by every storage node to the metadata
// service with the descriptors of every replica it hosts (anti-entropy).
type GroupsReport struct{ Descs []kv.RangeDesc }

// MapReq asks the metadata service for the partition map.
type MapReq struct{}

// MapResp is the partition map.
type MapResp struct {
	Version uint64
	Ranges  map[string][]kv.RangeDesc
}

// TokenReq asks global admission control for capacity tokens for a table.
type TokenReq struct {
	Table string
	Want  float64
}

// TokenGrant answers a TokenReq.
type TokenGrant struct {
	Table   string
	Granted float64
}
