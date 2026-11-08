package cluster

import (
	"time"

	"github.com/Gauravkarthikeyan/tessera/internal/kv"
)

// Bugs are deliberately injectable defects. Each one is a real class of bug
// that has shipped in real systems; the simulator + checker must catch all of
// them. See README "Proving the checker has teeth".
type Bugs struct {
	kv.Bugs
	// LocalReads: a replica that believes it is leader serves Get from its
	// local state without going through the log (no ReadIndex, no lease).
	// A deposed leader in a minority partition then serves stale reads.
	LocalReads bool
	// UnsafeCommit: Raft commits prior-term entries by counting replicas
	// (the Figure 8 bug).
	UnsafeCommit bool
	// RouterRetryNewSeq: routers retry a timed-out request under a fresh
	// sequence number, defeating server-side de-duplication.
	RouterRetryNewSeq bool
}

// TableConfig describes one tenant's table and workload.
type TableConfig struct {
	Name              string
	ProvisionedRate   float64 // table-level ops/sec enforced by GAC
	InitialPartitions int
	Clients           int
	Keys              int     // key space size
	Zipf              float64 // 0 = uniform; >1 = skewed (e.g. 1.2)
	ReadFraction      float64
	CASFraction       float64
	DeleteFraction    float64
	ThinkTime         time.Duration
}

// Config describes a whole simulated deployment.
type Config struct {
	Seed     int64
	Duration time.Duration

	StorageNodes int
	Replication  int
	Routers      int
	ShardSize    int // routers per tenant (shuffle sharding); 0 = all

	Tables []TableConfig

	// Per-partition hard ceiling (ops/sec) enforced by the partition leader,
	// and the burst credit (seconds of unused capacity that can accrue).
	PartitionRate float64
	BurstSeconds  float64
	// Split-for-heat: split when a partition sustains this fraction of
	// PartitionRate. 0 disables splitting.
	SplitThreshold float64
	MaxPartitions  int // per table

	Faults        bool    // inject crashes, partitions and packet loss
	FaultInterval float64 // mean seconds between faults
	PoisonTenant  bool    // add a tenant that sends router-killing requests

	Bugs  Bugs
	Trace func(format string, args ...any)
}

// DefaultConfig is a small multi-tenant deployment with one hot tenant.
func DefaultConfig(seed int64) Config {
	return Config{
		Seed:           seed,
		Duration:       10 * time.Second,
		StorageNodes:   5,
		Replication:    3,
		Routers:        6,
		ShardSize:      2,
		PartitionRate:  400,
		BurstSeconds:   1,
		SplitThreshold: 0.75,
		MaxPartitions:  8,
		Faults:         true,
		FaultInterval:  1.0,
		Tables: []TableConfig{
			{Name: "orders", ProvisionedRate: 3000, InitialPartitions: 1, Clients: 12, Keys: 64, Zipf: 0, ReadFraction: 0.5, CASFraction: 0.15, DeleteFraction: 0.05, ThinkTime: 2 * time.Millisecond},
			{Name: "carts", ProvisionedRate: 800, InitialPartitions: 2, Clients: 6, Keys: 32, Zipf: 1.1, ReadFraction: 0.6, CASFraction: 0.2, DeleteFraction: 0.05, ThinkTime: 5 * time.Millisecond},
			{Name: "sessions", ProvisionedRate: 300, InitialPartitions: 1, Clients: 6, Keys: 32, Zipf: 0, ReadFraction: 0.3, CASFraction: 0.1, DeleteFraction: 0.1, ThinkTime: 1 * time.Millisecond},
		},
	}
}

// Node id layout.
const (
	storageBase  = 1
	routerBase   = 101
	metadataNode = 201
	gacNode      = 202
	clientBase   = 1001
)

// Timing constants (virtual time).
const (
	tickInterval     = 10 * time.Millisecond
	electionTicks    = 15
	heartbeatTicks   = 3
	reportInterval   = 100 * time.Millisecond
	mapRefresh       = 1 * time.Second
	routerAttemptTO  = 40 * time.Millisecond
	routerMaxAttempt = 8
	clientAttemptTO  = 400 * time.Millisecond
	clientMaxAttempt = 6
	tokenLease       = 25.0
	splitCooldown    = 1500 * time.Millisecond
	heatWindow       = 500 * time.Millisecond
	rejectCacheTTL   = 1 * time.Second
)
