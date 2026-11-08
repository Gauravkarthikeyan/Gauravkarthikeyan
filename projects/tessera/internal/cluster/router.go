package cluster

import (
	"sort"
	"time"

	"github.com/Gauravkarthikeyan/tessera/internal/kv"
	"github.com/Gauravkarthikeyan/tessera/internal/sim"
)

type clientKey struct {
	client uint64
	seq    uint64
	att    int
}

type rreq struct {
	rid      uint64
	client   sim.NodeID
	ck       clientKey
	req      ClientReq
	attempt  int
	maybe    bool // some attempt may have been applied
	parked   bool
	started  time.Duration
	desc     kv.RangeDesc
	replicaI int
	target   sim.NodeID
}

// Router is a stateless-ish request router: it caches the partition map and
// leader hints, enforces table-level admission with tokens leased from GAC,
// and retries across replicas. Losing a router loses nothing durable.
type Router struct {
	w   *sim.World
	id  sim.NodeID
	c   *Cluster
	rid uint64

	ranges     map[string][]kv.RangeDesc
	mapVersion uint64
	mapAsked   bool
	leaders    map[kv.GroupID]sim.NodeID

	tokens     map[string]float64
	tokenAsked map[string]bool
	tokenWait  map[string][]*rreq
	pending    map[uint64]*rreq
	byClient   map[clientKey]*rreq
	done       map[clientKey]ClientResp // completed, for duplicate ClientReqs
	doneOrder  []clientKey
	mapWaiters []*rreq
}

func (c *Cluster) newRouter(w *sim.World, id sim.NodeID, _ *sim.Disk) sim.Node {
	r := &Router{
		w: w, id: id, c: c,
		ranges:     map[string][]kv.RangeDesc{},
		leaders:    map[kv.GroupID]sim.NodeID{},
		tokens:     map[string]float64{},
		tokenAsked: map[string]bool{},
		tokenWait:  map[string][]*rreq{},
		pending:    map[uint64]*rreq{},
		byClient:   map[clientKey]*rreq{},
		done:       map[clientKey]ClientResp{},
	}
	r.askMap()
	w.After(id, mapRefresh, r.refreshLoop)
	return r
}

func (r *Router) refreshLoop() {
	r.mapAsked = false
	r.askMap()
	r.w.After(r.id, mapRefresh, r.refreshLoop)
}

func (r *Router) askMap() {
	if !r.mapAsked {
		r.mapAsked = true
		r.w.Send(r.id, metadataNode, MapReq{})
		// If the reply is lost, ask again.
		r.w.After(r.id, 50*time.Millisecond, func() {
			if r.mapAsked && len(r.mapWaiters) > 0 {
				r.mapAsked = false
				r.askMap()
			}
		})
	}
}

// Receive implements sim.Node.
func (r *Router) Receive(from sim.NodeID, msg any) {
	switch m := msg.(type) {
	case ClientReq:
		r.onClient(from, m)
	case StoreResp:
		r.onStore(m)
	case MapResp:
		r.onMap(m)
	case TokenGrant:
		r.onGrant(m)
	}
}

func (r *Router) onClient(from sim.NodeID, m ClientReq) {
	if m.Poison {
		// A "query of death": parsing it crashes the process. Shuffle
		// sharding bounds how many tenants share the routers it can kill.
		r.c.stats.PoisonCrashes++
		r.w.Crash(r.id)
		return
	}
	ck := clientKey{m.Op.Client, m.Op.Seq, m.Attempt}
	if resp, ok := r.done[ck]; ok {
		r.w.Send(r.id, from, resp) // duplicate of a finished request
		return
	}
	if _, ok := r.byClient[ck]; ok {
		return // duplicate of an in-flight request
	}
	r.rid++
	q := &rreq{rid: r.rid, client: from, ck: ck, req: m, started: r.w.Now()}
	r.byClient[ck] = q
	r.admit(q)
}

// admit charges one token against the table's leased budget.
func (r *Router) admit(q *rreq) {
	t := q.req.Table
	if r.tokens[t] >= 1 {
		r.tokens[t]--
		if r.tokens[t] < tokenLease/2 {
			r.askTokens(t)
		}
		r.route(q)
		return
	}
	r.askTokens(t)
	r.tokenWait[t] = append(r.tokenWait[t], q)
	// If GAC cannot grant within a short window, the table is over its
	// provisioned rate: throttle. Nothing was forwarded, so this is definite.
	r.w.After(r.id, 20*time.Millisecond, func() {
		for i, w := range r.tokenWait[t] {
			if w == q {
				r.tokenWait[t] = append(r.tokenWait[t][:i], r.tokenWait[t][i+1:]...)
				r.c.stats.TableThrottles++
				r.finish(q, ClientResp{Status: StThrottled})
				return
			}
		}
	})
}

func (r *Router) askTokens(t string) {
	if r.tokenAsked[t] {
		return
	}
	r.tokenAsked[t] = true
	r.w.Send(r.id, gacNode, TokenReq{Table: t, Want: tokenLease})
	r.w.After(r.id, 10*time.Millisecond, func() { r.tokenAsked[t] = false })
}

func (r *Router) onGrant(g TokenGrant) {
	r.tokenAsked[g.Table] = false
	r.tokens[g.Table] += g.Granted
	waiting := r.tokenWait[g.Table]
	r.tokenWait[g.Table] = nil
	for _, q := range waiting {
		if r.tokens[g.Table] >= 1 {
			r.tokens[g.Table]--
			r.route(q)
		} else {
			r.tokenWait[g.Table] = append(r.tokenWait[g.Table], q)
		}
	}
	if len(r.tokenWait[g.Table]) > 0 {
		r.askTokens(g.Table)
	}
}

func (r *Router) lookup(table, key string) (kv.RangeDesc, bool) {
	var best kv.RangeDesc
	found := false
	for _, d := range r.ranges[table] {
		// While a split propagates, the map may briefly hold both the old
		// (wide) parent and the child; the child has the larger Start.
		if d.Contains(key) && (!found || d.Start > best.Start) {
			best, found = d, true
		}
	}
	return best, found
}

func (r *Router) route(q *rreq) {
	d, ok := r.lookup(q.req.Table, q.req.Op.Key)
	if !ok {
		r.parkForMap(q)
		return
	}
	if d.ID != q.desc.ID {
		q.desc = d
		q.replicaI = 0
	}
	q.attempt++
	if q.attempt > routerMaxAttempt {
		r.finish(q, ClientResp{Status: StUnavailable, MaybeApplied: q.maybe})
		return
	}
	target, ok := r.leaders[d.ID]
	if !ok || target == 0 {
		target = sim.NodeID(d.Replicas[q.replicaI%len(d.Replicas)])
		q.replicaI++
	}
	op := q.req.Op
	if r.c.cfg.Bugs.RouterRetryNewSeq && q.attempt > 1 {
		// BUG: a "fresh" request per retry defeats server-side dedup.
		op.Seq += uint64(q.attempt) << 40
	}
	r.pending[q.rid] = q
	q.target = target
	r.w.Send(r.id, target, StoreReq{RID: q.rid, Attempt: q.attempt, Group: d.ID, Op: op})
	att := q.attempt
	r.w.After(r.id, routerAttemptTO, func() {
		if r.pending[q.rid] == q && q.attempt == att {
			// No answer: the request may be sitting in a leader's log.
			q.maybe = true
			delete(r.leaders, d.ID)
			r.route(q)
		}
	})
}

func (r *Router) parkForMap(q *rreq) {
	if q.parked {
		return
	}
	q.parked = true
	delete(r.pending, q.rid)
	r.mapWaiters = append(r.mapWaiters, q)
	r.askMap()
	r.w.After(r.id, 200*time.Millisecond, func() {
		if q.parked {
			q.parked = false
			r.finish(q, ClientResp{Status: StUnavailable, MaybeApplied: q.maybe})
		}
	})
}

func (r *Router) onMap(m MapResp) {
	r.mapAsked = false
	if m.Version >= r.mapVersion {
		r.mapVersion = m.Version
		r.ranges = m.Ranges
	}
	waiters := r.mapWaiters
	r.mapWaiters = nil
	for _, q := range waiters {
		if q.parked {
			q.parked = false
			r.route(q)
		}
	}
}

func (r *Router) onStore(m StoreResp) {
	q := r.pending[m.RID]
	if q == nil {
		return
	}
	if m.Status == StOK {
		// Even a late OK from an earlier attempt is a valid answer: the
		// session table guarantees the op was applied exactly once.
		if m.Attempt == q.attempt {
			r.leaders[m.Group] = q.target
		}
		r.finish(q, ClientResp{Status: StOK, Res: m.Res})
		return
	}
	if m.Attempt != q.attempt {
		if m.Status == StAmbiguous {
			q.maybe = true
		}
		return // stale non-OK answer to an attempt we already gave up on
	}
	switch m.Status {
	case StThrottled:
		r.finish(q, ClientResp{Status: StThrottled, MaybeApplied: q.maybe})
	case StNotLeader:
		if m.LeaderHint != 0 {
			r.leaders[m.Group] = m.LeaderHint
			r.route(q)
		} else {
			delete(r.leaders, m.Group)
			r.retryLater(q, 5*time.Millisecond)
		}
	case StNoGroup:
		delete(r.leaders, m.Group)
		r.retryLater(q, 5*time.Millisecond)
	case StWrongRange:
		// Our map is stale (the range split). Refresh and re-route.
		delete(r.leaders, m.Group)
		q.desc = kv.RangeDesc{}
		r.mapAsked = false
		r.parkForMap(q)
	case StAmbiguous:
		q.maybe = true
		r.retryLater(q, 5*time.Millisecond)
	}
}

func (r *Router) retryLater(q *rreq, d time.Duration) {
	att := q.attempt
	r.w.After(r.id, d, func() {
		if r.pending[q.rid] == q && q.attempt == att {
			r.route(q)
		}
	})
}

func (r *Router) finish(q *rreq, resp ClientResp) {
	delete(r.pending, q.rid)
	delete(r.byClient, q.ck)
	resp.Seq, resp.Attempt = q.req.Op.Seq, q.req.Attempt
	r.done[q.ck] = resp
	r.doneOrder = append(r.doneOrder, q.ck)
	if len(r.doneOrder) > 4096 {
		delete(r.done, r.doneOrder[0])
		r.doneOrder = r.doneOrder[1:]
	}
	r.w.Send(r.id, q.client, resp)
}

// --- Metadata service -------------------------------------------------------

// Metadata is the authoritative partition map. It is fed by anti-entropy
// reports from storage nodes and merges them by range generation, so lost,
// duplicated or reordered reports are harmless.
type Metadata struct {
	w       *sim.World
	id      sim.NodeID
	version uint64
	byID    map[kv.GroupID]kv.RangeDesc
}

func (c *Cluster) newMetadata(w *sim.World, id sim.NodeID, _ *sim.Disk) sim.Node {
	m := &Metadata{w: w, id: id, byID: map[kv.GroupID]kv.RangeDesc{}}
	for _, d := range c.bootstrap {
		m.byID[d.ID] = d
	}
	m.version = 1
	c.meta = m
	return m
}

// Receive implements sim.Node.
func (m *Metadata) Receive(from sim.NodeID, msg any) {
	switch x := msg.(type) {
	case GroupsReport:
		for _, d := range x.Descs {
			cur, ok := m.byID[d.ID]
			if !ok || d.Gen > cur.Gen {
				m.byID[d.ID] = d
				m.version++
			}
		}
	case MapReq:
		m.w.Send(m.id, from, MapResp{Version: m.version, Ranges: m.snapshot()})
	}
}

func (m *Metadata) snapshot() map[string][]kv.RangeDesc {
	out := map[string][]kv.RangeDesc{}
	for _, d := range m.byID {
		out[d.Table] = append(out[d.Table], d)
	}
	for t := range out {
		sort.Slice(out[t], func(i, j int) bool { return out[t][i].Start < out[t][j].Start })
	}
	return out
}

func (m *Metadata) count(table string) int {
	n := 0
	for _, d := range m.byID {
		if d.Table == table {
			n++
		}
	}
	return n
}
