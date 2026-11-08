// Package sim is a deterministic discrete-event simulator.
//
// Every source of non-determinism in a distributed system (time, scheduling,
// the network, crashes) is routed through a single World, which is driven by
// one seeded PRNG and one event queue. Given the same seed and the same code,
// a run is bit-for-bit reproducible: the same messages are dropped, the same
// node crashes at the same virtual nanosecond, and the same bug surfaces.
//
// The model is intentionally small:
//
//   - Time is virtual. Nothing sleeps; the World jumps from event to event.
//   - A Node is anything that can Receive a message. Timers are closures.
//   - The network delivers, delays, drops, duplicates and reorders messages
//     according to a FaultConfig, and honours partitions between node sets.
//   - Crashing a node bumps its incarnation. Every pending timer and in-flight
//     message addressed to the old incarnation is silently discarded, which is
//     exactly how a real process loses its volatile state.
//   - Durable state is whatever the node stored in its Disk. The World keeps
//     the Disk across crashes and hands it back to the node factory on restart.
package sim

import (
	"container/heap"
	"fmt"
	"math/rand"
	"time"
)

// NodeID identifies a simulated process.
type NodeID int

// Node is a simulated process.
type Node interface {
	// Receive is invoked when a message is delivered to the node.
	Receive(from NodeID, msg any)
}

// Factory (re)creates a node from its durable disk. It is called once at
// start-up and once for every restart after a crash.
type Factory func(w *World, id NodeID, disk *Disk) Node

// Disk is the durable state of a node. Anything stored here survives a crash;
// anything not stored here does not. Writes are modelled as synchronously
// fsync'ed.
type Disk struct {
	Values map[string]any
}

// Get returns the value stored under key, or nil.
func (d *Disk) Get(key string) any { return d.Values[key] }

// Put durably stores v under key.
func (d *Disk) Put(key string, v any) { d.Values[key] = v }

// FaultConfig controls network misbehaviour.
type FaultConfig struct {
	MinLatency time.Duration
	MaxLatency time.Duration
	DropRate   float64 // probability a message is lost
	DupRate    float64 // probability a message is delivered twice
}

// DefaultFaults is a mildly hostile datacenter network.
var DefaultFaults = FaultConfig{
	MinLatency: 200 * time.Microsecond,
	MaxLatency: 2 * time.Millisecond,
	DropRate:   0.01,
	DupRate:    0.005,
}

type event struct {
	at  time.Duration
	seq uint64
	fn  func()
}

type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq // FIFO tie-break keeps runs deterministic
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(*event)) }
func (q *eventQueue) Pop() any     { old := *q; n := len(old); e := old[n-1]; *q = old[:n-1]; return e }
func (q eventQueue) peek() *event  { return q[0] }

type proc struct {
	node        Node
	factory     Factory
	disk        *Disk
	up          bool
	incarnation uint64
}

// Stats counts what the network did during a run.
type Stats struct {
	Sent, Delivered, Dropped, Duplicated, Partitioned uint64
	Crashes, Restarts                                 uint64
	Events                                            uint64
}

// World owns all simulated time, randomness, nodes and the network.
type World struct {
	now    time.Duration
	seq    uint64
	queue  eventQueue
	rng    *rand.Rand
	procs  map[NodeID]*proc
	order  []NodeID // deterministic iteration order
	faults FaultConfig
	// cut[a][b] == true means messages from a to b are dropped.
	cut   map[NodeID]map[NodeID]bool
	Stats Stats
	// Trace, if non-nil, receives a line for notable events.
	Trace func(format string, args ...any)
}

// NewWorld creates a world seeded with seed.
func NewWorld(seed int64, faults FaultConfig) *World {
	return &World{
		rng:    rand.New(rand.NewSource(seed)),
		procs:  map[NodeID]*proc{},
		faults: faults,
		cut:    map[NodeID]map[NodeID]bool{},
	}
}

// Now returns the current virtual time.
func (w *World) Now() time.Duration { return w.now }

// Rand returns the world's PRNG. All randomness in a simulation must come
// from here (or from a PRNG seeded from here) to keep runs reproducible.
func (w *World) Rand() *rand.Rand { return w.rng }

// SetFaults replaces the network fault configuration.
func (w *World) SetFaults(f FaultConfig) { w.faults = f }

// Faults returns the current network fault configuration.
func (w *World) Faults() FaultConfig { return w.faults }

func (w *World) tracef(format string, args ...any) {
	if w.Trace != nil {
		w.Trace("%10.3fms "+format, append([]any{float64(w.now) / 1e6}, args...)...)
	}
}

// Spawn registers a node and starts it.
func (w *World) Spawn(id NodeID, f Factory) Node {
	if _, ok := w.procs[id]; ok {
		panic(fmt.Sprintf("sim: node %d spawned twice", id))
	}
	p := &proc{factory: f, disk: &Disk{Values: map[string]any{}}, up: true, incarnation: 1}
	w.procs[id] = p
	w.order = append(w.order, id)
	p.node = f(w, id, p.disk)
	return p.node
}

// Node returns the live node for id, or nil if it is down.
func (w *World) Node(id NodeID) Node {
	p := w.procs[id]
	if p == nil || !p.up {
		return nil
	}
	return p.node
}

// Up reports whether node id is running.
func (w *World) Up(id NodeID) bool { p := w.procs[id]; return p != nil && p.up }

// Nodes returns all node ids in spawn order.
func (w *World) Nodes() []NodeID { return append([]NodeID(nil), w.order...) }

// Crash stops a node. Its volatile state is lost; its Disk is kept.
func (w *World) Crash(id NodeID) {
	p := w.procs[id]
	if p == nil || !p.up {
		return
	}
	p.up = false
	p.incarnation++
	p.node = nil
	w.Stats.Crashes++
	w.tracef("CRASH n%d", id)
}

// Restart brings a crashed node back from its Disk.
func (w *World) Restart(id NodeID) {
	p := w.procs[id]
	if p == nil || p.up {
		return
	}
	p.up = true
	p.incarnation++
	w.Stats.Restarts++
	w.tracef("RESTART n%d", id)
	p.node = p.factory(w, id, p.disk)
}

// Partition cuts all links between group a and group b in both directions.
func (w *World) Partition(a, b []NodeID) {
	for _, x := range a {
		for _, y := range b {
			w.setCut(x, y, true)
			w.setCut(y, x, true)
		}
	}
	w.tracef("PARTITION %v | %v", a, b)
}

// Heal removes every partition.
func (w *World) Heal() {
	w.cut = map[NodeID]map[NodeID]bool{}
	w.tracef("HEAL")
}

func (w *World) setCut(a, b NodeID, v bool) {
	m := w.cut[a]
	if m == nil {
		m = map[NodeID]bool{}
		w.cut[a] = m
	}
	m[b] = v
}

func (w *World) schedule(at time.Duration, fn func()) {
	w.seq++
	heap.Push(&w.queue, &event{at: at, seq: w.seq, fn: fn})
}

// At runs fn at absolute virtual time t, unconditionally (used by the test
// harness for fault injection; not bound to any node's lifetime).
func (w *World) At(t time.Duration, fn func()) {
	if t < w.now {
		t = w.now
	}
	w.schedule(t, fn)
}

// After runs fn on behalf of node id after d, unless the node crashes first.
func (w *World) After(id NodeID, d time.Duration, fn func()) {
	p := w.procs[id]
	if p == nil {
		panic(fmt.Sprintf("sim: After for unknown node %d", id))
	}
	inc := p.incarnation
	w.schedule(w.now+d, func() {
		if p.up && p.incarnation == inc {
			fn()
		}
	})
}

// Send transmits msg from one node to another through the faulty network.
func (w *World) Send(from, to NodeID, msg any) {
	w.Stats.Sent++
	src, dst := w.procs[from], w.procs[to]
	if src == nil || dst == nil || !src.up {
		w.Stats.Dropped++
		return
	}
	if w.cut[from][to] {
		w.Stats.Partitioned++
		return
	}
	if w.rng.Float64() < w.faults.DropRate {
		w.Stats.Dropped++
		return
	}
	copies := 1
	if w.rng.Float64() < w.faults.DupRate {
		copies = 2
		w.Stats.Duplicated++
	}
	for i := 0; i < copies; i++ {
		lat := w.faults.MinLatency
		if span := w.faults.MaxLatency - w.faults.MinLatency; span > 0 {
			lat += time.Duration(w.rng.Int63n(int64(span)))
		}
		inc := dst.incarnation
		w.schedule(w.now+lat, func() {
			// Messages addressed to a previous incarnation are lost with it.
			if !dst.up || dst.incarnation != inc {
				w.Stats.Dropped++
				return
			}
			w.Stats.Delivered++
			dst.node.Receive(from, msg)
		})
	}
}

// Step executes the next event. It returns false when the queue is empty.
func (w *World) Step() bool {
	if len(w.queue) == 0 {
		return false
	}
	e := heap.Pop(&w.queue).(*event)
	w.now = e.at
	w.Stats.Events++
	e.fn()
	return true
}

// RunUntil executes events until virtual time t (inclusive) or until the
// queue drains.
func (w *World) RunUntil(t time.Duration) {
	for len(w.queue) > 0 && w.queue.peek().at <= t {
		w.Step()
	}
	if w.now < t {
		w.now = t
	}
}

// RunFor advances virtual time by d.
func (w *World) RunFor(d time.Duration) { w.RunUntil(w.now + d) }
