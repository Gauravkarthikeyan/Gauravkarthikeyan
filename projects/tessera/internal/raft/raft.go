// Package raft implements the Raft consensus protocol as a pure,
// deterministic state machine.
//
// The core never touches a clock, a socket or a goroutine. The host drives it
// with three inputs and drains two outputs:
//
//	inputs:  Tick()          — one logical clock tick
//	         Step(Message)   — a message from a peer
//	         Propose(data)   — a new command (leader only)
//	outputs: Messages()      — messages to send to peers
//	         Committed()     — newly committed entries to apply, in order
//
// This "Ready"-style design (popularised by etcd/raft) is what makes the
// protocol testable under deterministic simulation: the simulator owns time
// and the network, Raft only owns the algorithm.
//
// Durable state (term, vote, log) lives in a *HardState that the host owns.
// Raft mutates it in place *before* emitting any message that depends on it,
// so the host's "disk" is always at least as new as anything a peer has seen.
//
// Implemented: leader election with randomized timeouts, log replication with
// fast conflict back-off (term-skipping hints), the "commit only current-term
// entries" rule (§5.4.2, Figure 8), and CheckQuorum so an isolated leader
// steps down instead of accepting writes it can never commit.
//
// Not implemented (deliberately, see README): log compaction/snapshots,
// membership changes, pre-vote.
package raft

import (
	"fmt"
	"math/rand"

	"github.com/Gauravkarthikeyan/tessera/internal/sim"
)

// NodeID identifies a peer. 0 means "none".
type NodeID = sim.NodeID

const None NodeID = 0

// Entry is one slot in the replicated log. A nil Data is a leader no-op.
type Entry struct {
	Term  uint64
	Index uint64
	Data  any
}

// HardState is the durable portion of Raft's state. Log[0] is a sentinel
// entry {Term:0, Index:0} so that real entries start at index 1.
type HardState struct {
	Term     uint64
	VotedFor NodeID
	Log      []Entry
}

// NewHardState returns an empty durable state.
func NewHardState() *HardState { return &HardState{Log: []Entry{{}}} }

// MsgType enumerates Raft RPCs.
type MsgType uint8

const (
	MsgVote MsgType = iota + 1
	MsgVoteResp
	MsgApp
	MsgAppResp
)

func (t MsgType) String() string {
	switch t {
	case MsgVote:
		return "Vote"
	case MsgVoteResp:
		return "VoteResp"
	case MsgApp:
		return "App"
	case MsgAppResp:
		return "AppResp"
	}
	return "?"
}

// Message is a Raft RPC. Fields are interpreted according to Type.
type Message struct {
	Type     MsgType
	From, To NodeID
	Term     uint64

	// MsgVote
	LastIndex, LastTerm uint64
	// MsgVoteResp
	Granted bool

	// MsgApp
	PrevIndex, PrevTerm uint64
	Entries             []Entry
	Commit              uint64

	// MsgAppResp
	Success    bool
	MatchIndex uint64 // on success: highest index known to match
	HintIndex  uint64 // on failure: where the leader should retry from
}

// Role is the Raft role of a peer.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string { return [...]string{"follower", "candidate", "leader"}[r] }

// Config configures a Raft peer.
type Config struct {
	ID    NodeID
	Peers []NodeID // all voters, including ID
	// ElectionTicks is the minimum election timeout in ticks; the actual
	// timeout is randomized in [ElectionTicks, 2*ElectionTicks).
	ElectionTicks  int
	HeartbeatTicks int
	MaxBatch       int // max entries per MsgApp
	Rand           *rand.Rand

	// UnsafeCommitPriorTerm disables the §5.4.2 restriction and lets a
	// leader commit entries from earlier terms by counting replicas. This is
	// the bug illustrated by Figure 8 of the Raft paper; it exists only so
	// the simulator can demonstrate that it finds it.
	UnsafeCommitPriorTerm bool
}

// Raft is a single peer in a single consensus group.
type Raft struct {
	cfg Config
	hs  *HardState

	role    Role
	leader  NodeID
	commit  uint64
	applied uint64

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int

	votes      map[NodeID]bool
	next       map[NodeID]uint64
	match      map[NodeID]uint64
	active     map[NodeID]bool // CheckQuorum: heard from since last check
	quorumTick int

	msgs []Message

	// Observability.
	Elections uint64
}

// New creates a Raft peer over durable state hs (which may be non-empty after
// a restart). Volatile state — role, commit index, applied index — always
// starts from zero, exactly as after a real process restart.
func New(cfg Config, hs *HardState) *Raft {
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 10
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 1
	}
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = 64
	}
	if len(hs.Log) == 0 {
		hs.Log = []Entry{{}}
	}
	r := &Raft{cfg: cfg, hs: hs}
	r.resetElectionTimer()
	return r
}

// --- accessors -------------------------------------------------------------

func (r *Raft) ID() NodeID        { return r.cfg.ID }
func (r *Raft) Role() Role        { return r.role }
func (r *Raft) Leader() NodeID    { return r.leader }
func (r *Raft) Term() uint64      { return r.hs.Term }
func (r *Raft) Commit() uint64    { return r.commit }
func (r *Raft) IsLeader() bool    { return r.role == Leader }
func (r *Raft) LastIndex() uint64 { return r.hs.Log[len(r.hs.Log)-1].Index }
func (r *Raft) lastTerm() uint64  { return r.hs.Log[len(r.hs.Log)-1].Term }
func (r *Raft) termAt(i uint64) uint64 {
	if i > r.LastIndex() {
		return 0
	}
	return r.hs.Log[i].Term
}

// TermAt returns the term of the entry at index i, or 0 if absent.
func (r *Raft) TermAt(i uint64) uint64 { return r.termAt(i) }

func (r *Raft) quorum() int { return len(r.cfg.Peers)/2 + 1 }

func (r *Raft) String() string {
	return fmt.Sprintf("n%d[%s t=%d c=%d a=%d last=%d]", r.cfg.ID, r.role, r.hs.Term, r.commit, r.applied, r.LastIndex())
}

// --- outputs ----------------------------------------------------------------

// Messages drains the outbox.
func (r *Raft) Messages() []Message {
	m := r.msgs
	r.msgs = nil
	return m
}

// Committed returns entries in (applied, commit] and marks them applied.
func (r *Raft) Committed() []Entry {
	if r.applied >= r.commit {
		return nil
	}
	out := make([]Entry, r.commit-r.applied)
	copy(out, r.hs.Log[r.applied+1:r.commit+1])
	r.applied = r.commit
	return out
}

func (r *Raft) send(m Message) {
	m.From = r.cfg.ID
	m.Term = r.hs.Term
	r.msgs = append(r.msgs, m)
}

// --- inputs -----------------------------------------------------------------

// Propose appends data to the log if this peer is the leader. It returns the
// index and term the entry was assigned; the entry is committed iff a later
// Committed() returns an entry with the same index *and* term.
func (r *Raft) Propose(data any) (index, term uint64, ok bool) {
	if r.role != Leader {
		return 0, 0, false
	}
	idx := r.appendEntry(data)
	r.match[r.cfg.ID] = idx
	r.broadcastAppend()
	r.maybeCommit() // single-node groups commit immediately
	return idx, r.hs.Term, true
}

// Tick advances the logical clock by one tick.
func (r *Raft) Tick() {
	switch r.role {
	case Leader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.cfg.HeartbeatTicks {
			r.heartbeatElapsed = 0
			r.broadcastAppend()
		}
		// CheckQuorum: a leader that cannot reach a majority for a full
		// election timeout steps down. Otherwise a partitioned leader keeps
		// accepting proposals that can never commit.
		r.quorumTick++
		if r.quorumTick >= r.cfg.ElectionTicks {
			r.quorumTick = 0
			n := 1
			for _, p := range r.cfg.Peers {
				if p != r.cfg.ID && r.active[p] {
					n++
				}
			}
			r.active = map[NodeID]bool{}
			if n < r.quorum() {
				r.becomeFollower(r.hs.Term, None)
			}
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.campaign()
		}
	}
}

// Step processes a message from a peer.
func (r *Raft) Step(m Message) {
	if m.Term > r.hs.Term {
		lead := None
		if m.Type == MsgApp {
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	}
	if r.role == Leader && m.Term == r.hs.Term {
		r.active[m.From] = true
	}
	switch m.Type {
	case MsgVote:
		r.handleVote(m)
	case MsgVoteResp:
		r.handleVoteResp(m)
	case MsgApp:
		r.handleAppend(m)
	case MsgAppResp:
		r.handleAppendResp(m)
	}
}

// --- role transitions -------------------------------------------------------

func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	r.electionTimeout = r.cfg.ElectionTicks + r.cfg.Rand.Intn(r.cfg.ElectionTicks)
}

func (r *Raft) becomeFollower(term uint64, lead NodeID) {
	if term > r.hs.Term {
		r.hs.Term = term
		r.hs.VotedFor = None // persisted before any reply is sent
	}
	r.role = Follower
	r.leader = lead
	r.resetElectionTimer()
}

func (r *Raft) campaign() {
	r.hs.Term++
	r.hs.VotedFor = r.cfg.ID
	r.role = Candidate
	r.leader = None
	r.votes = map[NodeID]bool{r.cfg.ID: true}
	r.Elections++
	r.resetElectionTimer()
	if len(r.votes) >= r.quorum() {
		r.becomeLeader()
		return
	}
	for _, p := range r.cfg.Peers {
		if p != r.cfg.ID {
			r.send(Message{Type: MsgVote, To: p, LastIndex: r.LastIndex(), LastTerm: r.lastTerm()})
		}
	}
}

func (r *Raft) becomeLeader() {
	r.role = Leader
	r.leader = r.cfg.ID
	r.heartbeatElapsed = 0
	r.quorumTick = 0
	r.active = map[NodeID]bool{}
	r.next = map[NodeID]uint64{}
	r.match = map[NodeID]uint64{}
	for _, p := range r.cfg.Peers {
		r.next[p] = r.LastIndex() + 1
		r.match[p] = 0
	}
	// A no-op in the new term lets the leader commit everything before it
	// (§5.4.2 / §8): entries from older terms are only ever committed
	// indirectly, by committing a current-term entry after them.
	r.match[r.cfg.ID] = r.appendEntry(nil)
	r.broadcastAppend()
	r.maybeCommit()
}

// --- elections --------------------------------------------------------------

func (r *Raft) handleVote(m Message) {
	if m.Term < r.hs.Term {
		r.send(Message{Type: MsgVoteResp, To: m.From, Granted: false})
		return
	}
	upToDate := m.LastTerm > r.lastTerm() ||
		(m.LastTerm == r.lastTerm() && m.LastIndex >= r.LastIndex())
	canVote := r.hs.VotedFor == None || r.hs.VotedFor == m.From
	grant := canVote && upToDate && r.role != Leader
	if grant {
		r.hs.VotedFor = m.From // durable before the reply leaves
		r.resetElectionTimer()
	}
	r.send(Message{Type: MsgVoteResp, To: m.From, Granted: grant})
}

func (r *Raft) handleVoteResp(m Message) {
	if r.role != Candidate || m.Term != r.hs.Term || !m.Granted {
		return
	}
	r.votes[m.From] = true
	if len(r.votes) >= r.quorum() {
		r.becomeLeader()
	}
}

// --- replication ------------------------------------------------------------

func (r *Raft) appendEntry(data any) uint64 {
	idx := r.LastIndex() + 1
	r.hs.Log = append(r.hs.Log, Entry{Term: r.hs.Term, Index: idx, Data: data})
	return idx
}

func (r *Raft) broadcastAppend() {
	for _, p := range r.cfg.Peers {
		if p != r.cfg.ID {
			r.sendAppend(p)
		}
	}
}

func (r *Raft) sendAppend(to NodeID) {
	next := r.next[to]
	if next < 1 {
		next = 1
	}
	prev := next - 1
	end := r.LastIndex()
	if end-prev > uint64(r.cfg.MaxBatch) {
		end = prev + uint64(r.cfg.MaxBatch)
	}
	var ents []Entry
	if end > prev {
		// Copy: the log's backing array is reused after truncation, and the
		// message may sit in the simulated network for a while.
		ents = make([]Entry, end-prev)
		copy(ents, r.hs.Log[prev+1:end+1])
	}
	r.send(Message{
		Type:      MsgApp,
		To:        to,
		PrevIndex: prev,
		PrevTerm:  r.termAt(prev),
		Entries:   ents,
		Commit:    r.commit,
	})
}

func (r *Raft) handleAppend(m Message) {
	if m.Term < r.hs.Term {
		r.send(Message{Type: MsgAppResp, To: m.From, Success: false, HintIndex: r.LastIndex() + 1})
		return
	}
	// A valid leader for this term exists.
	if r.role != Follower || r.leader != m.From {
		r.becomeFollower(m.Term, m.From)
	}
	r.resetElectionTimer()

	if m.PrevIndex > r.LastIndex() {
		r.send(Message{Type: MsgAppResp, To: m.From, Success: false, HintIndex: r.LastIndex() + 1})
		return
	}
	if t := r.termAt(m.PrevIndex); t != m.PrevTerm {
		// Fast back-off: skip the whole conflicting term in one round trip.
		hint := m.PrevIndex
		for hint > r.commit+1 && r.termAt(hint-1) == t {
			hint--
		}
		r.send(Message{Type: MsgAppResp, To: m.From, Success: false, HintIndex: hint})
		return
	}
	for i, e := range m.Entries {
		if e.Index <= r.LastIndex() {
			if r.termAt(e.Index) == e.Term {
				continue
			}
			if e.Index <= r.commit {
				panic(fmt.Sprintf("raft: %v asked to truncate committed index %d", r, e.Index))
			}
			r.hs.Log = r.hs.Log[:e.Index]
		}
		r.hs.Log = append(r.hs.Log, m.Entries[i:]...)
		break
	}
	// Only entries up to the end of *this* message are known to match the
	// leader; anything after them may be stale and must not be committed.
	match := m.PrevIndex + uint64(len(m.Entries))
	if c := min(m.Commit, match); c > r.commit {
		r.commit = c
	}
	r.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: match})
}

func (r *Raft) handleAppendResp(m Message) {
	if r.role != Leader || m.Term != r.hs.Term {
		return
	}
	if m.Success {
		if m.MatchIndex > r.match[m.From] {
			r.match[m.From] = m.MatchIndex
		}
		if r.next[m.From] < r.match[m.From]+1 {
			r.next[m.From] = r.match[m.From] + 1
		}
		r.maybeCommit()
		if r.next[m.From] <= r.LastIndex() {
			r.sendAppend(m.From)
		}
		return
	}
	// Rejection: move next back (never below what we know matches).
	n := m.HintIndex
	if n > r.next[m.From] {
		n = r.next[m.From] - 1
	}
	if n <= r.match[m.From] {
		n = r.match[m.From] + 1
	}
	if n < 1 {
		n = 1
	}
	r.next[m.From] = n
	r.sendAppend(m.From)
}

func (r *Raft) maybeCommit() {
	for n := r.LastIndex(); n > r.commit; n-- {
		if r.termAt(n) != r.hs.Term && !r.cfg.UnsafeCommitPriorTerm {
			// §5.4.2: never commit a prior-term entry by counting replicas.
			break
		}
		cnt := 0
		for _, p := range r.cfg.Peers {
			if r.match[p] >= n {
				cnt++
			}
		}
		if cnt >= r.quorum() {
			r.commit = n
			r.broadcastAppend() // tell followers promptly
			return
		}
	}
}
