package cluster

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/Gauravkarthikeyan/tessera/internal/admission"
	"github.com/Gauravkarthikeyan/tessera/internal/kv"
	"github.com/Gauravkarthikeyan/tessera/internal/raft"
	"github.com/Gauravkarthikeyan/tessera/internal/sim"
)

// groupDisk is the durable record of one replica: the snapshot it was born
// with (empty for bootstrap ranges, the split image for children) and the
// Raft hard state. The state machine itself is volatile and is rebuilt on
// restart by replaying the log over the initial snapshot.
type groupDisk struct {
	Init kv.Snapshot
	HS   *raft.HardState
}

type pendingReq struct {
	router  sim.NodeID
	rid     uint64
	attempt int
	term    uint64
}

type replica struct {
	id        kv.GroupID
	sm        *kv.StateMachine
	r         *raft.Raft
	pending   map[uint64]pendingReq // log index → waiting request (leader only)
	wasLead   bool
	bucket    *admission.TokenBucket
	heat      *admission.HeatTracker
	lastSplit time.Duration
	splitting bool
}

type rejectKey struct {
	router  sim.NodeID
	rid     uint64
	attempt int
}

type rejection struct {
	resp StoreResp
	at   time.Duration
}

// StorageNode hosts replicas of many partitions (multi-Raft).
type StorageNode struct {
	w      *sim.World
	id     sim.NodeID
	c      *Cluster
	groups map[kv.GroupID]*groupDisk // durable
	reps   map[kv.GroupID]*replica
	order  []kv.GroupID // deterministic iteration order
	// rejected remembers definite rejections so that a duplicated copy of
	// the same request (the network duplicates packets) gets the same answer
	// instead of being proposed later — which would turn "definitely not
	// applied" into a lie.
	rejected map[rejectKey]rejection
}

func (c *Cluster) newStorage(w *sim.World, id sim.NodeID, disk *sim.Disk) sim.Node {
	groups, _ := disk.Get("groups").(map[kv.GroupID]*groupDisk)
	if groups == nil {
		groups = map[kv.GroupID]*groupDisk{}
		disk.Put("groups", groups)
		for _, d := range c.bootstrap {
			for _, r := range d.Replicas {
				if sim.NodeID(r) == id {
					groups[d.ID] = &groupDisk{Init: kv.EmptySnapshot(d), HS: raft.NewHardState()}
				}
			}
		}
	}
	s := &StorageNode{w: w, id: id, c: c, groups: groups, reps: map[kv.GroupID]*replica{}, rejected: map[rejectKey]rejection{}}
	ids := make([]kv.GroupID, 0, len(groups))
	for gid := range groups {
		ids = append(ids, gid)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, gid := range ids {
		s.startReplica(gid)
	}
	w.After(id, tickInterval, s.tick)
	w.After(id, reportInterval, s.report)
	return s
}

func (s *StorageNode) startReplica(gid kv.GroupID) *replica {
	gd := s.groups[gid]
	peers := make([]raft.NodeID, len(gd.Init.Desc.Replicas))
	for i, p := range gd.Init.Desc.Replicas {
		peers[i] = raft.NodeID(p)
	}
	rep := &replica{
		id:      gid,
		sm:      kv.New(gd.Init, s.c.cfg.Bugs.Bugs),
		pending: map[uint64]pendingReq{},
		bucket:  admission.NewTokenBucket(s.c.cfg.PartitionRate, s.c.cfg.PartitionRate*s.c.cfg.BurstSeconds),
		heat:    admission.NewHeatTracker(heatWindow),
	}
	rep.r = raft.New(raft.Config{
		ID:                    s.id,
		Peers:                 peers,
		ElectionTicks:         electionTicks,
		HeartbeatTicks:        heartbeatTicks,
		Rand:                  rand.New(rand.NewSource(s.w.Rand().Int63())),
		UnsafeCommitPriorTerm: s.c.cfg.Bugs.UnsafeCommit,
	}, gd.HS)
	rep.lastSplit = s.w.Now()
	s.reps[gid] = rep
	s.order = append(s.order, gid)
	sort.Slice(s.order, func(i, j int) bool { return s.order[i] < s.order[j] })
	return rep
}

// Receive implements sim.Node.
func (s *StorageNode) Receive(from sim.NodeID, msg any) {
	switch m := msg.(type) {
	case RaftMsg:
		rep := s.reps[m.Group]
		if rep == nil {
			return // not created yet (lagging split) or never hosted
		}
		rep.r.Step(m.M)
		s.flush(rep)
	case StoreReq:
		s.handleStore(from, m)
	}
}

func (s *StorageNode) tick() {
	for _, gid := range s.order {
		rep := s.reps[gid]
		rep.r.Tick()
		s.flush(rep)
		s.maybeSplit(rep)
	}
	now := s.w.Now()
	for k, v := range s.rejected {
		if now-v.at > rejectCacheTTL {
			delete(s.rejected, k)
		}
	}
	s.w.After(s.id, tickInterval, s.tick)
}

func (s *StorageNode) report() {
	descs := make([]kv.RangeDesc, 0, len(s.order))
	for _, gid := range s.order {
		descs = append(descs, s.reps[gid].sm.Desc())
	}
	s.w.Send(s.id, metadataNode, GroupsReport{Descs: descs})
	s.w.After(s.id, reportInterval, s.report)
}

// flush sends Raft's outbox, applies newly committed entries and answers the
// requests that were waiting on them.
func (s *StorageNode) flush(rep *replica) {
	for _, m := range rep.r.Messages() {
		s.w.Send(s.id, m.To, RaftMsg{Group: rep.id, M: m})
	}
	for _, e := range rep.r.Committed() {
		s.apply(rep, e)
	}
	// Lost leadership: we can no longer tell whether in-flight proposals will
	// commit (a new leader may or may not have them). Say so explicitly.
	if rep.wasLead && !rep.r.IsLeader() {
		s.failPending(rep, StAmbiguous)
	}
	rep.wasLead = rep.r.IsLeader()
}

func (s *StorageNode) failPending(rep *replica, st Status) {
	idx := make([]uint64, 0, len(rep.pending))
	for i := range rep.pending {
		idx = append(idx, i)
	}
	sort.Slice(idx, func(a, b int) bool { return idx[a] < idx[b] })
	for _, i := range idx {
		p := rep.pending[i]
		s.w.Send(s.id, p.router, StoreResp{RID: p.rid, Attempt: p.attempt, Group: rep.id, Status: st, LeaderHint: rep.r.Leader()})
	}
	rep.pending = map[uint64]pendingReq{}
	rep.splitting = false
}

func (s *StorageNode) apply(rep *replica, e raft.Entry) {
	var res kv.Result
	switch cmd := e.Data.(type) {
	case nil: // leader no-op
	case kv.Op:
		res = rep.sm.Apply(cmd)
	case kv.SplitCmd:
		snap, ok := rep.sm.ApplySplit(cmd)
		rep.splitting = false
		if ok {
			rep.heat = admission.NewHeatTracker(heatWindow)
			rep.lastSplit = s.w.Now()
			if _, exists := s.groups[snap.Desc.ID]; !exists {
				// Durable before the child does anything. On replay after a
				// restart the child already exists and this is skipped, but
				// the parent still re-trims its own data above.
				s.groups[snap.Desc.ID] = &groupDisk{Init: snap, HS: raft.NewHardState()}
				s.startReplica(snap.Desc.ID)
				if !s.c.splitSeen[snap.Desc.ID] {
					s.c.splitSeen[snap.Desc.ID] = true // count once, not once per replica
					s.c.stats.Splits++
				}
				s.c.tracef("SPLIT n%d %s → %s + %s", s.id, cmd.Child, rep.sm.Desc(), snap.Desc)
			}
		}
	}
	p, ok := rep.pending[e.Index]
	if !ok {
		return
	}
	delete(rep.pending, e.Index)
	if p.term != e.Term {
		s.w.Send(s.id, p.router, StoreResp{RID: p.rid, Attempt: p.attempt, Group: rep.id, Status: StAmbiguous})
		return
	}
	st := StOK
	if res.Code == kv.WrongRange {
		st = StWrongRange
	}
	s.w.Send(s.id, p.router, StoreResp{RID: p.rid, Attempt: p.attempt, Group: rep.id, Status: st, Res: res})
}

func (s *StorageNode) reject(router sim.NodeID, m StoreReq, st Status, hint sim.NodeID) {
	resp := StoreResp{RID: m.RID, Attempt: m.Attempt, Group: m.Group, Status: st, LeaderHint: hint}
	s.rejected[rejectKey{router, m.RID, m.Attempt}] = rejection{resp: resp, at: s.w.Now()}
	s.w.Send(s.id, router, resp)
}

func (s *StorageNode) handleStore(from sim.NodeID, m StoreReq) {
	if prev, ok := s.rejected[rejectKey{from, m.RID, m.Attempt}]; ok {
		s.w.Send(s.id, from, prev.resp) // duplicate of a rejected request
		return
	}
	rep := s.reps[m.Group]
	if rep == nil {
		s.reject(from, m, StNoGroup, 0)
		return
	}
	if !rep.r.IsLeader() {
		s.reject(from, m, StNotLeader, rep.r.Leader())
		return
	}
	if !rep.sm.Desc().Contains(m.Op.Key) {
		s.reject(from, m, StWrongRange, 0)
		return
	}
	now := s.w.Now()
	if !rep.bucket.TryTake(now, 1) {
		rep.heat.RecordThrottle(now, m.Op.Key)
		s.c.stats.PartitionThrottles++
		s.reject(from, m, StThrottled, 0)
		return
	}
	rep.heat.Record(now, m.Op.Key, 1)
	if s.c.cfg.Bugs.LocalReads && m.Op.Kind == kv.OpGet {
		s.w.Send(s.id, from, StoreResp{RID: m.RID, Attempt: m.Attempt, Group: m.Group, Status: StOK, Res: rep.sm.Read(m.Op.Key)})
		return
	}
	idx, term, ok := rep.r.Propose(m.Op)
	if !ok {
		s.reject(from, m, StNotLeader, rep.r.Leader())
		return
	}
	rep.pending[idx] = pendingReq{router: from, rid: m.RID, attempt: m.Attempt, term: term}
	s.flush(rep)
}

// maybeSplit implements split-for-heat on the partition leader: if the
// partition has been running near its ceiling, propose a split at the
// load-weighted median key.
func (s *StorageNode) maybeSplit(rep *replica) {
	cfg := s.c.cfg
	if cfg.SplitThreshold <= 0 || !rep.r.IsLeader() || rep.splitting {
		return
	}
	now := s.w.Now()
	if now-rep.lastSplit < splitCooldown {
		return
	}
	if rate := rep.heat.Rate(now); rate < cfg.SplitThreshold*cfg.PartitionRate {
		return
	}
	desc := rep.sm.Desc()
	if s.c.partitionsOf(desc.Table) >= cfg.MaxPartitions {
		return
	}
	key, ok := rep.heat.SplitKey(now, desc.Start)
	if !ok || !desc.Contains(key) {
		s.c.stats.SplitsRefused++
		rep.lastSplit = now // single hot key: don't re-evaluate every tick
		return
	}
	cmd := kv.SplitCmd{Key: key, Child: kv.GroupID(fmt.Sprintf("%s.%d", desc.ID, desc.Gen)), Gen: desc.Gen}
	if _, _, ok := rep.r.Propose(cmd); ok {
		rep.splitting = true
		rep.lastSplit = now
		s.flush(rep)
	}
}
