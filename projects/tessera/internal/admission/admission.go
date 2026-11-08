// Package admission implements the throughput-control primitives Tessera
// borrows from production multi-tenant databases:
//
//   - TokenBucket: rate limiting with burst credit. Unused capacity accrues
//     up to a cap, so short spikes above the provisioned rate are absorbed
//     ("burst capacity").
//   - HeatTracker: per-partition load sampling that picks a *load-weighted*
//     split point, so a partition that runs hot is split where the traffic
//     is, not where the data is ("split for heat").
//
// Both are pure data structures driven by an explicit clock, so they behave
// identically under the deterministic simulator and in unit tests.
package admission

import (
	"sort"
	"time"
)

// TokenBucket is a classic token bucket with fractional tokens.
type TokenBucket struct {
	Rate     float64 // tokens added per second
	Capacity float64 // max tokens held (burst)
	tokens   float64
	last     time.Duration
	init     bool
}

// NewTokenBucket returns a bucket that starts full.
func NewTokenBucket(rate, capacity float64) *TokenBucket {
	return &TokenBucket{Rate: rate, Capacity: capacity, tokens: capacity}
}

func (b *TokenBucket) refill(now time.Duration) {
	if !b.init {
		b.init = true
		b.last = now
		return
	}
	if now > b.last {
		b.tokens += b.Rate * (now - b.last).Seconds()
		if b.tokens > b.Capacity {
			b.tokens = b.Capacity
		}
		b.last = now
	}
}

// Tokens returns the balance at time now.
func (b *TokenBucket) Tokens(now time.Duration) float64 {
	b.refill(now)
	return b.tokens
}

// TryTake removes n tokens if available.
func (b *TokenBucket) TryTake(now time.Duration, n float64) bool {
	b.refill(now)
	if b.tokens >= n {
		b.tokens -= n
		return true
	}
	return false
}

// TakeUpTo removes and returns min(n, balance) tokens. Used by the global
// admission controller to hand out leases.
func (b *TokenBucket) TakeUpTo(now time.Duration, n float64) float64 {
	b.refill(now)
	if n > b.tokens {
		n = b.tokens
	}
	if n < 0 {
		n = 0
	}
	b.tokens -= n
	return n
}

// HeatTracker samples per-key load over a sliding pair of windows.
type HeatTracker struct {
	Window   time.Duration
	start    time.Duration
	cur      map[string]float64
	prev     map[string]float64
	curTotal float64
	prevRate float64 // ops/sec measured over the previous full window
	throttle int
	prevThr  int
}

// NewHeatTracker creates a tracker with the given measurement window.
func NewHeatTracker(window time.Duration) *HeatTracker {
	return &HeatTracker{Window: window, cur: map[string]float64{}, prev: map[string]float64{}}
}

func (h *HeatTracker) roll(now time.Duration) {
	for now-h.start >= h.Window {
		h.prev = h.cur
		h.prevRate = h.curTotal / h.Window.Seconds()
		h.prevThr = h.throttle
		h.cur = map[string]float64{}
		h.curTotal = 0
		h.throttle = 0
		if now-h.start >= 2*h.Window {
			// Idle for more than a window: everything is cold.
			h.start = now
			h.prev = map[string]float64{}
			h.prevRate = 0
			h.prevThr = 0
		} else {
			h.start += h.Window
		}
	}
}

// Record notes that key consumed weight units of capacity.
func (h *HeatTracker) Record(now time.Duration, key string, weight float64) {
	h.roll(now)
	h.cur[key] += weight
	h.curTotal += weight
}

// RecordThrottle notes a rejected request.
func (h *HeatTracker) RecordThrottle(now time.Duration, key string) {
	h.roll(now)
	h.throttle++
	h.cur[key] += 1 // throttled demand is still heat
}

// Rate returns the consumed rate (units/sec) over the last full window.
func (h *HeatTracker) Rate(now time.Duration) float64 { h.roll(now); return h.prevRate }

// Throttles returns the number of throttled requests in the last full window.
func (h *HeatTracker) Throttles(now time.Duration) int { h.roll(now); return h.prevThr }

// SplitKey returns a key k such that roughly half of the observed load is on
// keys < k and half on keys >= k. It returns ok=false when no split can
// separate load — most importantly when the heat is a single key, which no
// amount of splitting can help (the classic "hot item" limitation).
//
// lo is the partition's start key; the returned key is always > lo.
func (h *HeatTracker) SplitKey(now time.Duration, lo string) (string, bool) {
	h.roll(now)
	load := map[string]float64{}
	for k, v := range h.prev {
		load[k] += v
	}
	for k, v := range h.cur {
		load[k] += v
	}
	keys := make([]string, 0, len(load))
	var total float64
	for k, v := range load {
		keys = append(keys, k)
		total += v
	}
	if len(keys) < 2 {
		return "", false
	}
	sort.Strings(keys)
	// Choose the boundary i (split before keys[i]) that minimises the load
	// imbalance |left - right|.
	best, bestImb := -1, total
	var left float64
	for i := 1; i < len(keys); i++ {
		left += load[keys[i-1]]
		imb := left - (total - left)
		if imb < 0 {
			imb = -imb
		}
		if imb < bestImb && keys[i] > lo {
			best, bestImb = i, imb
		}
	}
	if best < 0 {
		return "", false
	}
	// If one key carries most of the load, a split moves the problem but does
	// not solve it. Require that the hottest side keeps < 85% of the load.
	if bestImb/total > 0.70 {
		return "", false
	}
	return keys[best], true
}
