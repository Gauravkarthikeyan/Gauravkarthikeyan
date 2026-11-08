package raft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/Gauravkarthikeyan/tessera/internal/sim"
)

// peer hosts one Raft instance on a simulated node and records everything it
// applies, so the test can check Raft's safety properties after the run.
type peer struct {
	w       *sim.World
	id      sim.NodeID
	r       *Raft
	applied *[]Entry // durable across crashes only for inspection
}

type world struct {
	w       *sim.World
	ids     []sim.NodeID
	applied map[sim.NodeID]*[]Entry
	leaders map[uint64]sim.NodeID // term → leader (election safety)
	t       *testing.T
	unsafe  bool
}

func (p *peer) Receive(_ sim.NodeID, msg any) {
	p.r.Step(msg.(Message))
	p.flush()
}

func (p *peer) flush() {
	for _, m := range p.r.Messages() {
		p.w.Send(p.id, m.To, m)
	}
	for _, e := range p.r.Committed() {
		*p.applied = append(*p.applied, e)
	}
}

func newWorld(t *testing.T, seed int64, n int, unsafe bool) *world {
	ww := &world{
		w:       sim.NewWorld(seed, sim.DefaultFaults),
		applied: map[sim.NodeID]*[]Entry{},
		leaders: map[uint64]sim.NodeID{},
		t:       t,
		unsafe:  unsafe,
	}
	for i := 1; i <= n; i++ {
		ww.ids = append(ww.ids, sim.NodeID(i))
	}
	for _, id := range ww.ids {
		ww.w.Spawn(id, ww.factory)
	}
	return ww
}

func (ww *world) factory(w *sim.World, id sim.NodeID, disk *sim.Disk) sim.Node {
	hs, _ := disk.Get("hs").(*HardState)
	if hs == nil {
		hs = NewHardState()
		disk.Put("hs", hs)
	}
	// Volatile: a restarted node re-applies the log from the beginning.
	app := &[]Entry{}
	ww.applied[id] = app
	p := &peer{w: w, id: id, applied: app}
	p.r = New(Config{
		ID: id, Peers: ww.ids, ElectionTicks: 10, HeartbeatTicks: 2,
		Rand:                  rand.New(rand.NewSource(w.Rand().Int63())),
		UnsafeCommitPriorTerm: ww.unsafe,
	}, hs)
	var tick func()
	tick = func() {
		p.r.Tick()
		p.flush()
		ww.observe(p)
		w.After(id, 10*time.Millisecond, tick)
	}
	w.After(id, 10*time.Millisecond, tick)
	return p
}

func (ww *world) observe(p *peer) {
	if p.r.IsLeader() {
		if prev, ok := ww.leaders[p.r.Term()]; ok && prev != p.id {
			ww.t.Fatalf("election safety violated: n%d and n%d both leader in term %d", prev, p.id, p.r.Term())
		}
		ww.leaders[p.r.Term()] = p.id
	}
}

func (ww *world) propose(v int) {
	for _, id := range ww.ids {
		if n, ok := ww.w.Node(id).(*peer); ok && n.r.IsLeader() {
			n.r.Propose(v)
			n.flush()
			return
		}
	}
}

// checkStateMachineSafety: every node applies the same entry at each index.
func (ww *world) checkStateMachineSafety() int {
	ref := map[uint64]Entry{}
	maxLen := 0
	for id, app := range ww.applied {
		for i, e := range *app {
			if e.Index != uint64(i+1) {
				ww.t.Fatalf("n%d applied out of order: position %d has index %d", id, i, e.Index)
			}
			if r, ok := ref[e.Index]; ok {
				if r.Term != e.Term || fmt.Sprint(r.Data) != fmt.Sprint(e.Data) {
					ww.t.Fatalf("state machine safety violated at index %d: %v vs %v", e.Index, r, e)
				}
			} else {
				ref[e.Index] = e
			}
		}
		maxLen = max(maxLen, len(*app))
	}
	return maxLen
}

func runChaos(t *testing.T, seed int64, unsafe bool) (applied int) {
	ww := newWorld(t, seed, 5, unsafe)
	rng := rand.New(rand.NewSource(seed))
	v := 0
	for step := 0; step < 400; step++ {
		ww.w.RunFor(25 * time.Millisecond)
		for i := 0; i < 3; i++ {
			v++
			ww.propose(v)
		}
		switch x := rng.Intn(100); {
		case x < 4:
			ww.w.Crash(ww.ids[rng.Intn(len(ww.ids))])
		case x < 10:
			for _, id := range ww.ids {
				ww.w.Restart(id)
			}
		case x < 13:
			perm := rng.Perm(len(ww.ids))
			var a, b []sim.NodeID
			for i, j := range perm {
				if i < 2 {
					a = append(a, ww.ids[j])
				} else {
					b = append(b, ww.ids[j])
				}
			}
			ww.w.Partition(a, b)
		case x < 18:
			ww.w.Heal()
		}
		ww.checkStateMachineSafety()
	}
	ww.w.Heal()
	for _, id := range ww.ids {
		ww.w.Restart(id)
	}
	ww.w.RunFor(3 * time.Second)
	ww.propose(-1)
	ww.w.RunFor(1 * time.Second)
	return ww.checkStateMachineSafety()
}

func TestRaftSafetyUnderChaos(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			if n := runChaos(t, seed, false); n == 0 {
				t.Fatalf("no progress")
			}
		})
	}
}

func TestSingleNodeCommitsImmediately(t *testing.T) {
	hs := NewHardState()
	r := New(Config{ID: 1, Peers: []NodeID{1}, Rand: rand.New(rand.NewSource(1))}, hs)
	for i := 0; i < 30 && !r.IsLeader(); i++ {
		r.Tick()
	}
	if !r.IsLeader() {
		t.Fatal("single node did not elect itself")
	}
	r.Propose("x")
	got := r.Committed()
	if len(got) != 2 || got[1].Data != "x" {
		t.Fatalf("got %v", got)
	}
}
