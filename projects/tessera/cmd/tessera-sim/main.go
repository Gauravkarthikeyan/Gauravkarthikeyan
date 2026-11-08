// Command tessera-sim runs Tessera under deterministic simulation.
//
//	tessera-sim                       # one seed, verbose report
//	tessera-sim -seeds 200            # sweep seeds 1..200, report failures
//	tessera-sim -seed 42 -trace       # replay one seed with an event trace
//	tessera-sim -bug local-reads -seeds 50
//	tessera-sim -scenario hot-partition
//	tessera-sim -scenario poison
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Gauravkarthikeyan/tessera/internal/cluster"
)

var bugs = map[string]string{
	"local-reads":         "leaders serve reads from local state (no ReadIndex / lease)",
	"no-dedup":            "state machine ignores the session table",
	"no-range-check":      "state machine skips the in-log range ownership check",
	"split-drop-sessions": "split children do not inherit the session table",
	"router-new-seq":      "routers retry under a fresh sequence number",
	"unsafe-commit":       "Raft commits prior-term entries by counting replicas (Figure 8)",
}

func main() {
	seed := flag.Int64("seed", 1, "first seed")
	seeds := flag.Int("seeds", 1, "number of consecutive seeds to run")
	dur := flag.Duration("duration", 10*time.Second, "virtual duration of the workload")
	faults := flag.Bool("faults", true, "inject crashes, partitions and packet loss")
	bug := flag.String("bug", "", "inject a known bug: "+strings.Join(keys(), ", "))
	scenario := flag.String("scenario", "default", "default | hot-partition | poison | calm")
	trace := flag.Bool("trace", false, "print an event trace (single seed only)")
	verbose := flag.Bool("v", false, "verbose report for every seed")
	stopOnFail := flag.Bool("stop", true, "stop the sweep at the first failure")
	flag.Parse()

	if *bug != "" {
		if _, ok := bugs[*bug]; !ok {
			fmt.Fprintf(os.Stderr, "unknown bug %q; choose one of: %s\n", *bug, strings.Join(keys(), ", "))
			os.Exit(2)
		}
	}

	failed := 0
	start := time.Now()
	for i := 0; i < *seeds; i++ {
		s := *seed + int64(i)
		cfg := build(*scenario, s)
		cfg.Duration = *dur
		cfg.Faults = cfg.Faults && *faults
		applyBug(&cfg, *bug)
		if *trace && *seeds == 1 {
			cfg.Trace = func(f string, a ...any) { fmt.Printf(f+"\n", a...) }
		}
		rep := cluster.New(cfg).Run()
		fmt.Print(rep.Summary(*verbose || *seeds == 1))
		if !rep.Check.OK {
			failed++
			if *seeds > 1 {
				fmt.Printf("  ↳ reproduce with: go run ./cmd/tessera-sim -seed %d -scenario %s%s -v\n", s, *scenario, bugFlag(*bug))
			}
			if *stopOnFail && *seeds > 1 {
				break
			}
		}
	}
	if *seeds > 1 {
		fmt.Printf("\n%d/%d seeds failed  (wall %v)\n", failed, *seeds, time.Since(start).Round(time.Millisecond))
	}
	if failed > 0 {
		os.Exit(1)
	}
}

func build(scenario string, seed int64) cluster.Config {
	cfg := cluster.DefaultConfig(seed)
	switch scenario {
	case "default":
	case "calm":
		cfg.Faults = false
	case "hot-partition":
		// One tenant, one partition, a skewed workload far above the
		// partition ceiling: watch split-for-heat carve it up.
		cfg.Faults = false
		cfg.Tables = []cluster.TableConfig{{
			Name: "hot", ProvisionedRate: 5000, InitialPartitions: 1, Clients: 48, Keys: 256,
			Zipf: 1.05, ReadFraction: 0.5, CASFraction: 0.1, DeleteFraction: 0.02, ThinkTime: time.Millisecond,
		}}
		cfg.PartitionRate = 500
		cfg.MaxPartitions = 16
	case "poison":
		cfg.Faults = false
		cfg.Routers = 8
		cfg.ShardSize = 2
		cfg.PoisonTenant = true
		cfg.Tables = nil
		for i := 0; i < 8; i++ {
			cfg.Tables = append(cfg.Tables, cluster.TableConfig{
				Name: fmt.Sprintf("tenant%02d", i), ProvisionedRate: 2000, InitialPartitions: 1, Clients: 3,
				Keys: 32, ReadFraction: 0.6, CASFraction: 0.1, ThinkTime: 5 * time.Millisecond,
			})
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", scenario)
		os.Exit(2)
	}
	return cfg
}

func applyBug(cfg *cluster.Config, bug string) {
	switch bug {
	case "local-reads":
		cfg.Bugs.LocalReads = true
	case "no-dedup":
		cfg.Bugs.NoDedup = true
	case "no-range-check":
		cfg.Bugs.NoRangeCheck = true
	case "split-drop-sessions":
		cfg.Bugs.SplitDropSessions = true
	case "router-new-seq":
		cfg.Bugs.RouterRetryNewSeq = true
	case "unsafe-commit":
		cfg.Bugs.UnsafeCommit = true
	}
}

func bugFlag(b string) string {
	if b == "" {
		return ""
	}
	return " -bug " + b
}

func keys() []string {
	return []string{"local-reads", "no-dedup", "no-range-check", "split-drop-sessions", "router-new-seq", "unsafe-commit"}
}
