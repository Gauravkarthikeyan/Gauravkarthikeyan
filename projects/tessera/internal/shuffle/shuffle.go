// Package shuffle implements shuffle sharding: every tenant is assigned a
// small, pseudo-random combination of k workers out of n. Two tenants share a
// failure only to the extent their combinations overlap, and the chance that
// a tenant's *entire* shard is shared with a misbehaving tenant is
// 1 / C(n, k) — for n=100, k=5 that is about 1 in 75 million.
//
// The idea: blast radius shrinks combinatorially as n and k grow.
package shuffle

import (
	"hash/fnv"
	"math"
	"math/big"
	"math/rand"
	"sort"
)

// Assign returns tenant's shard: k distinct worker indexes in [0, n), sorted.
// The assignment is a pure function of (tenant, n, k, salt).
func Assign(tenant string, n, k int, salt uint64) []int {
	if k <= 0 || k > n {
		k = n
	}
	h := fnv.New64a()
	h.Write([]byte(tenant))
	rng := rand.New(rand.NewSource(int64(h.Sum64() ^ salt)))
	out := rng.Perm(n)[:k]
	sort.Ints(out)
	return out
}

// Combinations returns C(n, k) exactly.
func Combinations(n, k int) *big.Int { return new(big.Int).Binomial(int64(n), int64(k)) }

// OverlapPMF returns P(|A ∩ B| = j) for two independently, uniformly chosen
// k-subsets A, B of n workers (the hypergeometric distribution), j = 0..k.
func OverlapPMF(n, k int) []float64 {
	total := new(big.Float).SetInt(Combinations(n, k))
	pmf := make([]float64, k+1)
	for j := 0; j <= k; j++ {
		if k-j > n-k {
			continue
		}
		num := new(big.Int).Mul(Combinations(k, j), Combinations(n-k, k-j))
		p, _ := new(big.Float).Quo(new(big.Float).SetInt(num), total).Float64()
		pmf[j] = p
	}
	return pmf
}

// FullOverlapProbability is P(another tenant's shard == this tenant's shard).
func FullOverlapProbability(n, k int) float64 {
	c, _ := new(big.Float).SetInt(Combinations(n, k)).Float64()
	return 1 / c
}

// BlastRadius reports, for a concrete assignment, what happens when every
// worker in poisoned's shard fails: the fraction of other tenants that lose
// *all* of their workers (fully down) and the fraction that lose at least one
// (degraded but, with client-side retry to another shard member, still up).
func BlastRadius(shards map[string][]int, poisoned string) (down, degraded float64) {
	bad := map[int]bool{}
	for _, w := range shards[poisoned] {
		bad[w] = true
	}
	var n, d, g int
	for t, s := range shards {
		if t == poisoned {
			continue
		}
		n++
		hit := 0
		for _, w := range s {
			if bad[w] {
				hit++
			}
		}
		if hit == len(s) {
			d++
		} else if hit > 0 {
			g++
		}
	}
	if n == 0 {
		return 0, 0
	}
	return float64(d) / float64(n), float64(g) / float64(n)
}

// Log10 is a helper for printing very small probabilities.
func Log10(p float64) float64 { return math.Log10(p) }
