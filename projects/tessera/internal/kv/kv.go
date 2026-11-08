// Package kv is the replicated state machine that runs inside every Raft
// group: one key range ("partition") of one table.
//
// It provides
//
//   - item-level operations with per-item versions: Get, Put, Delete and a
//     conditional put (CAS on version, or put-if-absent with version 0) —
//     the same shape as conditional writes in most managed NoSQL stores;
//   - exactly-once semantics for retried requests through a per-client
//     session table (client id → last sequence number and its response);
//   - range ownership checks applied *in log order*, so a request that was
//     proposed before a split but committed after it is rejected rather than
//     silently written into the wrong partition;
//   - splits as a log entry ("split trigger"): every replica applies the
//     split at the same log position against the same state, so every
//     replica derives a byte-identical initial state for the child range
//     without any snapshot transfer.
package kv

import (
	"fmt"
	"maps"
	"sort"
)

// GroupID identifies a Raft group / partition. Children of a split are named
// "<parent>.<generation>", which makes IDs globally unique without any
// coordination.
type GroupID string

// RangeDesc describes the key range [Start, End) of a table owned by a group.
// End == "" means +infinity. Gen increases every time the range changes.
type RangeDesc struct {
	Table    string
	ID       GroupID
	Start    string
	End      string
	Gen      uint64
	Replicas []int
}

// Contains reports whether key falls in the range.
func (d RangeDesc) Contains(key string) bool {
	return key >= d.Start && (d.End == "" || key < d.End)
}

func (d RangeDesc) String() string {
	end := d.End
	if end == "" {
		end = "∞"
	}
	return fmt.Sprintf("%s/%s[%q,%q)@g%d", d.Table, d.ID, d.Start, end, d.Gen)
}

// OpKind enumerates client operations.
type OpKind uint8

const (
	OpGet OpKind = iota + 1
	OpPut
	OpDelete
	OpCAS // conditional put: succeeds iff current version == Expect (0 = absent)
)

func (k OpKind) String() string {
	return [...]string{"?", "get", "put", "del", "cas"}[k]
}

// Op is a client request as it travels through the log.
type Op struct {
	Kind   OpKind
	Key    string
	Value  string
	Expect uint64
	Client uint64
	Seq    uint64
}

// Code is a result code.
type Code uint8

const (
	OK Code = iota
	CondFailed
	WrongRange
)

func (c Code) String() string { return [...]string{"ok", "cond-failed", "wrong-range"}[c] }

// Result is the outcome of applying an Op.
type Result struct {
	Code    Code
	Found   bool   // Get: item existed
	Value   string // Get: value
	Version uint64 // Get: current version; Put/CAS: new version
}

// SplitCmd splits the range at Key. The child takes [Key, End).
type SplitCmd struct {
	Key   string
	Child GroupID
	Gen   uint64 // must equal the range's current Gen, else the split is stale
}

// Item is a stored value.
type Item struct {
	Value   string
	Version uint64
}

type session struct {
	Seq uint64
	Res Result
}

// Bugs lets the simulator inject well-known classes of defects to prove the
// checker catches them. All false in production.
type Bugs struct {
	NoDedup           bool // ignore the session table → retries double-apply
	NoRangeCheck      bool // skip in-log range check → writes lost across splits
	SplitDropSessions bool // children do not inherit sessions → retries double-apply after split
}

// Snapshot is the full state of a range. It is the initial state of a child
// range created by a split.
type Snapshot struct {
	Desc     RangeDesc
	Items    map[string]Item
	Sessions map[uint64]session
}

// StateMachine is one partition's data.
type StateMachine struct {
	desc     RangeDesc
	items    map[string]Item
	sessions map[uint64]session
	bugs     Bugs
}

// New restores a state machine from a snapshot (copied, never aliased).
func New(s Snapshot, bugs Bugs) *StateMachine {
	return &StateMachine{
		desc:     s.Desc,
		items:    maps.Clone(s.Items),
		sessions: maps.Clone(s.Sessions),
		bugs:     bugs,
	}
}

// EmptySnapshot returns the initial snapshot for a new range.
func EmptySnapshot(d RangeDesc) Snapshot {
	return Snapshot{Desc: d, Items: map[string]Item{}, Sessions: map[uint64]session{}}
}

// Desc returns the current range descriptor.
func (s *StateMachine) Desc() RangeDesc { return s.desc }

// Len returns the number of stored items.
func (s *StateMachine) Len() int { return len(s.items) }

// Read serves a Get directly from local state, bypassing the log. Only used
// by the injected "local reads" bug.
func (s *StateMachine) Read(key string) Result {
	it, ok := s.items[key]
	return Result{Code: OK, Found: ok, Value: it.Value, Version: it.Version}
}

// Apply executes an Op in log order.
func (s *StateMachine) Apply(op Op) Result {
	if !s.bugs.NoDedup {
		if ss, ok := s.sessions[op.Client]; ok && op.Seq <= ss.Seq {
			// A retry of something we already executed (or an older request
			// overtaken by a newer one). Never re-execute.
			return ss.Res
		}
	}
	if !s.bugs.NoRangeCheck && !s.desc.Contains(op.Key) {
		// Not recorded in the session: the client will retry elsewhere.
		return Result{Code: WrongRange}
	}
	var res Result
	cur, exists := s.items[op.Key]
	switch op.Kind {
	case OpGet:
		res = Result{Code: OK, Found: exists, Value: cur.Value, Version: cur.Version}
	case OpPut:
		res = s.put(op.Key, op.Value, cur, exists)
	case OpDelete:
		delete(s.items, op.Key)
		res = Result{Code: OK}
	case OpCAS:
		if (op.Expect == 0 && !exists) || (exists && cur.Version == op.Expect) {
			res = s.put(op.Key, op.Value, cur, exists)
		} else {
			res = Result{Code: CondFailed}
		}
	default:
		panic("kv: unknown op")
	}
	s.sessions[op.Client] = session{Seq: op.Seq, Res: res}
	return res
}

func (s *StateMachine) put(key, val string, cur Item, exists bool) Result {
	v := uint64(1)
	if exists {
		v = cur.Version + 1
	}
	s.items[key] = Item{Value: val, Version: v}
	return Result{Code: OK, Version: v}
}

// ApplySplit executes a split trigger. It returns the child's initial
// snapshot, or ok=false if the split is stale or invalid (in which case
// nothing changes). It is deterministic: every replica returns an identical
// child snapshot.
func (s *StateMachine) ApplySplit(c SplitCmd) (Snapshot, bool) {
	if c.Gen != s.desc.Gen || !s.desc.Contains(c.Key) || c.Key == s.desc.Start {
		return Snapshot{}, false
	}
	child := RangeDesc{
		Table:    s.desc.Table,
		ID:       c.Child,
		Start:    c.Key,
		End:      s.desc.End,
		Gen:      1,
		Replicas: append([]int(nil), s.desc.Replicas...),
	}
	snap := Snapshot{Desc: child, Items: map[string]Item{}, Sessions: map[uint64]session{}}
	for k, it := range s.items {
		if k >= c.Key {
			snap.Items[k] = it
			delete(s.items, k)
		}
	}
	if !s.bugs.SplitDropSessions {
		// Both sides need the full session table: a request applied in the
		// parent before the split may be retried against the child after it.
		snap.Sessions = maps.Clone(s.sessions)
	}
	s.desc.End = c.Key
	s.desc.Gen++
	return snap, true
}

// Keys returns the stored keys in order (for tests and debugging).
func (s *StateMachine) Keys() []string {
	ks := make([]string, 0, len(s.items))
	for k := range s.items {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
