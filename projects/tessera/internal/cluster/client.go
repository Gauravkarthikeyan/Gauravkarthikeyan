package cluster

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/Gauravkarthikeyan/tessera/internal/admission"
	"github.com/Gauravkarthikeyan/tessera/internal/checker"
	"github.com/Gauravkarthikeyan/tessera/internal/kv"
	"github.com/Gauravkarthikeyan/tessera/internal/sim"
)

// GAC is global admission control: one token bucket per table, from which
// routers lease small batches of capacity. This is how a table-level limit is
// enforced across a fleet of routers that never talk to each other.
type GAC struct {
	w       *sim.World
	id      sim.NodeID
	buckets map[string]*admission.TokenBucket
}

func (c *Cluster) newGAC(w *sim.World, id sim.NodeID, _ *sim.Disk) sim.Node {
	g := &GAC{w: w, id: id, buckets: map[string]*admission.TokenBucket{}}
	for _, t := range c.cfg.Tables {
		g.buckets[t.Name] = admission.NewTokenBucket(t.ProvisionedRate, t.ProvisionedRate*c.cfg.BurstSeconds)
	}
	return g
}

// Receive implements sim.Node.
func (g *GAC) Receive(from sim.NodeID, msg any) {
	if m, ok := msg.(TokenReq); ok {
		b := g.buckets[m.Table]
		granted := 0.0
		if b != nil {
			granted = b.TakeUpTo(g.w.Now(), m.Want)
		}
		g.w.Send(g.id, from, TokenGrant{Table: m.Table, Granted: granted})
	}
}

// --- Client -----------------------------------------------------------------

type inflight struct {
	op      checker.Op
	kvop    kv.Op
	attempt int
	maybe   bool
	backoff time.Duration
}

// Client is a closed-loop workload generator for one tenant. It issues one
// operation at a time, retries with the same sequence number (so the server
// can de-duplicate), and records every operation for the checker.
type Client struct {
	w       *sim.World
	id      sim.NodeID
	c       *Cluster
	cid     uint64
	table   TableConfig
	routers []sim.NodeID
	rng     *rand.Rand
	zipf    *rand.Zipf
	seq     uint64
	cur     *inflight
	pref    int
	seen    map[string]uint64 // last version observed per key, for CAS
	poison  bool
}

func (c *Cluster) newClient(t TableConfig, cid uint64, routers []sim.NodeID, poison bool) Factory {
	return func(w *sim.World, id sim.NodeID, _ *sim.Disk) sim.Node {
		cl := &Client{
			w: w, id: id, c: c, cid: cid, table: t, routers: routers,
			rng:    rand.New(rand.NewSource(w.Rand().Int63())),
			seen:   map[string]uint64{},
			poison: poison,
		}
		if t.Zipf > 1 {
			cl.zipf = rand.NewZipf(cl.rng, t.Zipf, 1, uint64(t.Keys-1))
		}
		cl.pref = cl.rng.Intn(len(routers))
		w.After(id, time.Duration(cl.rng.Intn(20))*time.Millisecond, cl.next)
		return cl
	}
}

// Factory is re-exported for readability.
type Factory = sim.Factory

func (cl *Client) key() string {
	var i int
	if cl.zipf != nil {
		i = int(cl.zipf.Uint64())
	} else {
		i = cl.rng.Intn(cl.table.Keys)
	}
	return fmt.Sprintf("k%04d", i)
}

func (cl *Client) next() {
	if cl.w.Now() >= cl.c.cfg.Duration {
		return
	}
	if cl.poison {
		r := cl.routers[cl.rng.Intn(len(cl.routers))]
		cl.w.Send(cl.id, r, ClientReq{Table: cl.table.Name, Poison: true})
		cl.w.After(cl.id, 300*time.Millisecond, cl.next)
		return
	}
	cl.seq++
	k := cl.key()
	op := kv.Op{Key: k, Client: cl.cid, Seq: cl.seq}
	x := cl.rng.Float64()
	t := cl.table
	switch {
	case x < t.ReadFraction:
		op.Kind = kv.OpGet
	case x < t.ReadFraction+t.CASFraction:
		op.Kind = kv.OpCAS
		op.Expect = cl.seen[k]
		op.Value = fmt.Sprintf("c%d.%d", cl.cid, cl.seq)
	case x < t.ReadFraction+t.CASFraction+t.DeleteFraction:
		op.Kind = kv.OpDelete
	default:
		op.Kind = kv.OpPut
		op.Value = fmt.Sprintf("c%d.%d", cl.cid, cl.seq)
	}
	cl.cur = &inflight{
		kvop: op,
		op: checker.Op{
			ClientID: cl.cid, Key: t.Name + "/" + k, Kind: op.Kind,
			Value: op.Value, Expect: op.Expect, Call: cl.w.Now(),
		},
		backoff: 2 * time.Millisecond,
	}
	cl.send()
}

func (cl *Client) send() {
	f := cl.cur
	f.attempt++
	r := cl.routers[cl.pref%len(cl.routers)]
	cl.w.Send(cl.id, r, ClientReq{Table: cl.table.Name, Op: f.kvop, Attempt: f.attempt})
	att := f.attempt
	cl.w.After(cl.id, clientAttemptTO, func() {
		if cl.cur == f && f.attempt == att {
			// The router may have crashed after forwarding: ambiguous.
			f.maybe = true
			cl.pref++ // fail over to the next router in our shuffle shard
			cl.retry()
		}
	})
}

func (cl *Client) retry() {
	f := cl.cur
	if f.attempt >= clientMaxAttempt {
		cl.complete(false)
		return
	}
	d := f.backoff + time.Duration(cl.rng.Int63n(int64(f.backoff)))
	f.backoff *= 2
	att := f.attempt
	cl.w.After(cl.id, d, func() {
		if cl.cur == f && f.attempt == att {
			cl.send()
		}
	})
}

// Receive implements sim.Node.
func (cl *Client) Receive(_ sim.NodeID, msg any) {
	m, ok := msg.(ClientResp)
	f := cl.cur
	if !ok || f == nil || m.Seq != f.kvop.Seq || m.Attempt != f.attempt {
		return
	}
	st := cl.c.stats.tenant(cl.table.Name)
	switch m.Status {
	case StOK:
		f.op.Out = m.Res
		if f.kvop.Kind == kv.OpGet {
			cl.seen[f.kvop.Key] = m.Res.Version
		} else if m.Res.Code == kv.OK && f.kvop.Kind != kv.OpDelete {
			cl.seen[f.kvop.Key] = m.Res.Version
		} else if f.kvop.Kind == kv.OpDelete {
			cl.seen[f.kvop.Key] = 0
		}
		cl.complete(true)
	case StThrottled:
		st.Throttled++
		if m.MaybeApplied {
			f.maybe = true
		}
		cl.retry()
	default:
		if m.MaybeApplied {
			f.maybe = true
		}
		cl.pref++
		cl.retry()
	}
}

func (cl *Client) complete(ok bool) {
	f := cl.cur
	cl.cur = nil
	st := cl.c.stats.tenant(cl.table.Name)
	now := cl.w.Now()
	switch {
	case ok:
		f.op.Return = now
		st.OK++
		st.latencies = append(st.latencies, now-f.op.Call)
		cl.c.history = append(cl.c.history, f.op)
	case f.maybe:
		// Outcome unknown: it may take effect at any point after its call,
		// or never. The checker must consider both.
		f.op.Return = checker.Indeterminate
		f.op.Unknown = true
		st.Indeterminate++
		cl.c.history = append(cl.c.history, f.op)
		cl.seen[f.kvop.Key] = 0
	default:
		st.Failed++ // definitely never applied: not part of the history
	}
	think := cl.table.ThinkTime
	if think > 0 {
		think = time.Duration(cl.rng.Int63n(int64(think)) + 1)
	}
	cl.w.After(cl.id, think, cl.next)
}
