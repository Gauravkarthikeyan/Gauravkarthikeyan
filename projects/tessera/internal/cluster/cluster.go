package cluster

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/Gauravkarthikeyan/tessera/internal/checker"
	"github.com/Gauravkarthikeyan/tessera/internal/kv"
	"github.com/Gauravkarthikeyan/tessera/internal/shuffle"
	"github.com/Gauravkarthikeyan/tessera/internal/sim"
)

// TenantStats aggregates one tenant's client-observed outcomes.
type TenantStats struct {
	OK, Throttled, Failed, Indeterminate int
	latencies                            []time.Duration
	P50, P99, Max                        time.Duration
}

// Stats aggregates a run.
type Stats struct {
	Tenants            map[string]*TenantStats
	Splits             int
	SplitsRefused      int
	PartitionThrottles int
	TableThrottles     int
	PoisonCrashes      int
	Faults             []string
}

func (s *Stats) tenant(name string) *TenantStats {
	t := s.Tenants[name]
	if t == nil {
		t = &TenantStats{}
		s.Tenants[name] = t
	}
	return t
}

// Cluster is one simulated deployment.
type Cluster struct {
	cfg       Config
	W         *sim.World
	bootstrap []kv.RangeDesc
	meta      *Metadata
	history   []checker.Op
	stats     Stats
	storage   []sim.NodeID
	routers   []sim.NodeID
	Shards    map[string][]int
	splitSeen map[kv.GroupID]bool
}

func (c *Cluster) tracef(format string, args ...any) {
	if c.cfg.Trace != nil {
		c.cfg.Trace("%10.3fms "+format, append([]any{float64(c.W.Now()) / 1e6}, args...)...)
	}
}

func (c *Cluster) partitionsOf(table string) int { return c.meta.count(table) }

// New builds (but does not run) a cluster.
func New(cfg Config) *Cluster {
	c := &Cluster{cfg: cfg, stats: Stats{Tenants: map[string]*TenantStats{}}, Shards: map[string][]int{}, splitSeen: map[kv.GroupID]bool{}}
	c.W = sim.NewWorld(cfg.Seed, sim.DefaultFaults)
	c.W.Trace = cfg.Trace

	for i := 0; i < cfg.StorageNodes; i++ {
		c.storage = append(c.storage, sim.NodeID(storageBase+i))
	}
	for i := 0; i < cfg.Routers; i++ {
		c.routers = append(c.routers, sim.NodeID(routerBase+i))
	}

	// Bootstrap ranges: split each table's key space evenly and place
	// replicas round-robin across storage nodes.
	place := 0
	for _, t := range cfg.Tables {
		for p := 0; p < t.InitialPartitions; p++ {
			d := kv.RangeDesc{Table: t.Name, ID: kv.GroupID(fmt.Sprintf("%s-%d", t.Name, p)), Gen: 1}
			if p > 0 {
				d.Start = fmt.Sprintf("k%04d", p*t.Keys/t.InitialPartitions)
			}
			if p < t.InitialPartitions-1 {
				d.End = fmt.Sprintf("k%04d", (p+1)*t.Keys/t.InitialPartitions)
			}
			for r := 0; r < cfg.Replication; r++ {
				d.Replicas = append(d.Replicas, int(c.storage[(place+r)%len(c.storage)]))
			}
			place++
			c.bootstrap = append(c.bootstrap, d)
		}
	}

	c.W.Spawn(metadataNode, c.newMetadata)
	c.W.Spawn(gacNode, c.newGAC)
	for _, id := range c.storage {
		c.W.Spawn(id, c.newStorage)
	}
	for _, id := range c.routers {
		c.W.Spawn(id, c.newRouter)
	}

	tables := append([]TableConfig(nil), cfg.Tables...)
	if cfg.PoisonTenant {
		tables = append(tables, TableConfig{Name: "poison", Clients: 1})
	}
	cid := uint64(1)
	nextClient := sim.NodeID(clientBase)
	for _, t := range tables {
		shard := shuffle.Assign(t.Name, len(c.routers), cfg.ShardSize, uint64(cfg.Seed))
		c.Shards[t.Name] = shard
		rs := make([]sim.NodeID, len(shard))
		for i, s := range shard {
			rs[i] = c.routers[s]
		}
		for i := 0; i < t.Clients; i++ {
			c.W.Spawn(nextClient, c.newClient(t, cid, rs, t.Name == "poison"))
			nextClient++
			cid++
		}
	}
	return c
}

// Report is the result of a run.
type Report struct {
	Seed    int64
	Stats   Stats
	Check   checker.Result
	Net     sim.Stats
	Ranges  map[string][]kv.RangeDesc
	Elapsed time.Duration // wall-clock
}

// Run executes the configured workload under faults, then checks the
// history for linearizability.
func (c *Cluster) Run() Report {
	start := time.Now()
	if c.cfg.Faults {
		c.scheduleFaults()
	}
	if c.cfg.PoisonTenant {
		c.scheduleRouterRecovery()
	}
	c.W.RunUntil(c.cfg.Duration)
	// Quiesce: heal everything, restart everyone, let in-flight work finish.
	c.W.Heal()
	c.W.SetFaults(sim.DefaultFaults)
	for _, id := range c.W.Nodes() {
		c.W.Restart(id)
	}
	c.W.RunFor(3 * time.Second)

	for _, t := range c.stats.Tenants {
		sort.Slice(t.latencies, func(i, j int) bool { return t.latencies[i] < t.latencies[j] })
		if n := len(t.latencies); n > 0 {
			t.P50 = t.latencies[n/2]
			t.P99 = t.latencies[n*99/100]
			t.Max = t.latencies[n-1]
		}
	}
	rep := Report{Seed: c.cfg.Seed, Stats: c.stats, Net: c.W.Stats, Ranges: c.meta.snapshot()}
	rep.Check = checker.Check(c.history, 0)
	rep.Elapsed = time.Since(start)
	return rep
}

// History returns the recorded operations.
func (c *Cluster) History() []checker.Op { return c.history }

func (c *Cluster) fault(format string, args ...any) {
	s := fmt.Sprintf("%8.1fms  ", float64(c.W.Now())/1e6) + fmt.Sprintf(format, args...)
	c.stats.Faults = append(c.stats.Faults, s)
	c.tracef("FAULT %s", fmt.Sprintf(format, args...))
}

// scheduleFaults injects a random fault roughly every FaultInterval seconds.
// The schedule draws from its own PRNG (seeded from the run seed), so it is
// reproducible too.
func (c *Cluster) scheduleFaults() {
	rng := rand.New(rand.NewSource(c.cfg.Seed * 7919))
	mean := c.cfg.FaultInterval
	if mean <= 0 {
		mean = 1
	}
	t := time.Duration(0)
	for {
		t += time.Duration((0.3 + rng.ExpFloat64()*mean) * float64(time.Second))
		if t >= c.cfg.Duration {
			return
		}
		at := t
		kind := rng.Intn(100)
		victim := c.storage[rng.Intn(len(c.storage))]
		dur := time.Duration((0.2 + rng.Float64()*1.5) * float64(time.Second))
		switch {
		case kind < 35:
			c.W.At(at, func() { c.fault("crash storage n%d for %v", victim, dur.Round(time.Millisecond)); c.W.Crash(victim) })
			c.W.At(at+dur, func() { c.W.Restart(victim) })
		case kind < 65:
			c.W.At(at, func() {
				rest := []sim.NodeID{}
				for _, id := range c.W.Nodes() {
					if id != victim {
						rest = append(rest, id)
					}
				}
				c.fault("isolate storage n%d for %v", victim, dur.Round(time.Millisecond))
				c.W.Partition([]sim.NodeID{victim}, rest)
			})
			c.W.At(at+dur, func() { c.W.Heal() })
		case kind < 80:
			// Split-brain-ish: two storage nodes on one side, the rest (and
			// every router and client) on the other.
			a := c.storage[rng.Intn(len(c.storage))]
			c.W.At(at, func() {
				side := []sim.NodeID{victim, a}
				rest := []sim.NodeID{}
				for _, id := range c.W.Nodes() {
					if id != victim && id != a {
						rest = append(rest, id)
					}
				}
				// Leave routers able to reach the minority side, so a stale
				// leader there can still be asked questions.
				for _, r := range c.routers {
					for i, x := range rest {
						if x == r {
							rest = append(rest[:i], rest[i+1:]...)
							break
						}
					}
				}
				c.fault("partition storage {n%d,n%d} from the other storage nodes for %v", victim, a, dur.Round(time.Millisecond))
				c.W.Partition(side, rest)
			})
			c.W.At(at+dur, func() { c.W.Heal() })
		case kind < 90:
			c.W.At(at, func() {
				c.fault("packet loss 20%%, latency up to 15ms for %v", dur.Round(time.Millisecond))
				c.W.SetFaults(sim.FaultConfig{MinLatency: time.Millisecond, MaxLatency: 15 * time.Millisecond, DropRate: 0.2, DupRate: 0.05})
			})
			c.W.At(at+dur, func() { c.W.SetFaults(sim.DefaultFaults) })
		default:
			r := c.routers[rng.Intn(len(c.routers))]
			c.W.At(at, func() { c.fault("crash router n%d for %v", r, dur.Round(time.Millisecond)); c.W.Crash(r) })
			c.W.At(at+dur, func() { c.W.Restart(r) })
		}
	}
}

// scheduleRouterRecovery restarts routers killed by poison requests after a
// fixed recovery time, like a process supervisor would.
func (c *Cluster) scheduleRouterRecovery() {
	var loop func()
	down := map[sim.NodeID]time.Duration{}
	loop = func() {
		for _, r := range c.routers {
			if !c.W.Up(r) {
				if _, ok := down[r]; !ok {
					down[r] = c.W.Now()
				} else if c.W.Now()-down[r] >= 500*time.Millisecond {
					c.W.Restart(r)
					delete(down, r)
				}
			}
		}
		if c.W.Now() < c.cfg.Duration {
			c.W.At(c.W.Now()+50*time.Millisecond, loop)
		}
	}
	c.W.At(0, loop)
}

// Summary renders a report for humans.
func (r Report) Summary(verbose bool) string {
	var b strings.Builder
	verdict := "LINEARIZABLE ✓"
	if !r.Check.OK {
		verdict = "NOT LINEARIZABLE ✗"
	}
	fmt.Fprintf(&b, "seed %d: %s  (%d ops over %d keys checked", r.Seed, verdict, r.Check.Ops, r.Check.Keys)
	if r.Check.Inconclusive > 0 {
		fmt.Fprintf(&b, ", %d keys inconclusive", r.Check.Inconclusive)
	}
	fmt.Fprintf(&b, "; wall %v)\n", r.Elapsed.Round(time.Millisecond))
	if !verbose && r.Check.OK {
		return b.String()
	}
	names := make([]string, 0, len(r.Stats.Tenants))
	for n := range r.Stats.Tenants {
		names = append(names, n)
	}
	sort.Strings(names)
	fmt.Fprintf(&b, "\n  %-10s %8s %10s %8s %8s %9s %9s %9s\n", "tenant", "ok", "throttled", "failed", "unknown", "p50", "p99", "max")
	for _, n := range names {
		t := r.Stats.Tenants[n]
		fmt.Fprintf(&b, "  %-10s %8d %10d %8d %8d %9v %9v %9v\n", n, t.OK, t.Throttled, t.Failed, t.Indeterminate,
			t.P50.Round(10*time.Microsecond), t.P99.Round(10*time.Microsecond), t.Max.Round(10*time.Microsecond))
	}
	fmt.Fprintf(&b, "\n  splits: %d (refused: %d)   partition throttles: %d   table throttles: %d   poison crashes: %d\n",
		r.Stats.Splits, r.Stats.SplitsRefused, r.Stats.PartitionThrottles, r.Stats.TableThrottles, r.Stats.PoisonCrashes)
	fmt.Fprintf(&b, "  network: %d sent, %d delivered, %d dropped, %d duplicated, %d cut by partitions\n",
		r.Net.Sent, r.Net.Delivered, r.Net.Dropped, r.Net.Duplicated, r.Net.Partitioned)
	fmt.Fprintf(&b, "  events: %d   crashes: %d   restarts: %d\n", r.Net.Events, r.Net.Crashes, r.Net.Restarts)
	tables := make([]string, 0, len(r.Ranges))
	for t := range r.Ranges {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	b.WriteString("\n  final partition map:\n")
	for _, t := range tables {
		for _, d := range r.Ranges[t] {
			fmt.Fprintf(&b, "    %s  replicas=%v\n", d, d.Replicas)
		}
	}
	if len(r.Stats.Faults) > 0 {
		b.WriteString("\n  faults injected:\n")
		for _, f := range r.Stats.Faults {
			fmt.Fprintf(&b, "    %s\n", f)
		}
	}
	for i, f := range r.Check.Failures {
		if i == 3 {
			fmt.Fprintf(&b, "\n  … and %d more non-linearizable keys\n", len(r.Check.Failures)-3)
			break
		}
		b.WriteString("\n  " + strings.ReplaceAll(f.Explain(6), "\n", "\n  "))
	}
	return b.String()
}
