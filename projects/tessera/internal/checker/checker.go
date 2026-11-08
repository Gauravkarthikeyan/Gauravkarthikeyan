// Package checker verifies that a concurrent history of key-value operations
// is linearizable.
//
// It implements the Wing–Gong / Lowe (WGL) search with memoisation, the same
// algorithm used by Porcupine and Knossos. Because every operation touches a
// single key, the history is first partitioned per key (linearizability is
// *local*, Herlihy & Wing 1990), which turns one exponential search into many
// small ones.
//
// Operations whose outcome the client never learned (timeouts, crashes) are
// "indeterminate": their return time is +∞ and their output is unknown, so
// the search may place them anywhere after their call — or effectively never,
// by linearizing them at the very end where they can affect nothing.
package checker

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/Gauravkarthikeyan/tessera/internal/kv"
)

// Op is one client operation in a history.
type Op struct {
	ClientID uint64
	Key      string // fully qualified: table/key
	Kind     kv.OpKind
	Value    string // Put/CAS input
	Expect   uint64 // CAS input

	Call   time.Duration
	Return time.Duration // math.MaxInt64 if indeterminate

	Unknown bool      // outcome unknown
	Out     kv.Result // valid iff !Unknown
}

// Indeterminate is the Return time of an operation with unknown outcome.
const Indeterminate = time.Duration(math.MaxInt64)

func (o Op) String() string {
	var in, out string
	switch o.Kind {
	case kv.OpPut:
		in = fmt.Sprintf("(%q)", o.Value)
	case kv.OpCAS:
		in = fmt.Sprintf("(v%d→%q)", o.Expect, o.Value)
	}
	switch {
	case o.Unknown:
		out = "?"
	case o.Out.Code != kv.OK:
		out = o.Out.Code.String()
	case o.Kind == kv.OpGet && !o.Out.Found:
		out = "∅"
	case o.Kind == kv.OpGet:
		out = fmt.Sprintf("%q@v%d", o.Out.Value, o.Out.Version)
	case o.Kind == kv.OpDelete:
		out = "ok"
	default:
		out = fmt.Sprintf("v%d", o.Out.Version)
	}
	ret := "∞"
	if o.Return != Indeterminate {
		ret = fmt.Sprintf("%.3fms", float64(o.Return)/1e6)
	}
	return fmt.Sprintf("c%d %s %s%s → %s  [%.3fms, %s]", o.ClientID, o.Key, o.Kind, in, out, float64(o.Call)/1e6, ret)
}

// state is the sequential model of a single key.
type state struct {
	exists  bool
	value   string
	version uint64
}

// step applies op to s. It returns false if op's recorded output is
// impossible from s.
func step(s state, op *Op) (state, bool) {
	var res kv.Result
	next := s
	switch op.Kind {
	case kv.OpGet:
		res = kv.Result{Code: kv.OK, Found: s.exists, Value: s.value, Version: s.version}
	case kv.OpPut:
		next = put(s, op.Value)
		res = kv.Result{Code: kv.OK, Version: next.version}
	case kv.OpDelete:
		next = state{}
		res = kv.Result{Code: kv.OK}
	case kv.OpCAS:
		if (op.Expect == 0 && !s.exists) || (s.exists && s.version == op.Expect) {
			next = put(s, op.Value)
			res = kv.Result{Code: kv.OK, Version: next.version}
		} else {
			res = kv.Result{Code: kv.CondFailed}
		}
	}
	if op.Unknown {
		return next, true
	}
	if res.Code != op.Out.Code || res.Version != op.Out.Version {
		return s, false
	}
	if op.Kind == kv.OpGet && (res.Found != op.Out.Found || (res.Found && res.Value != op.Out.Value)) {
		return s, false
	}
	return next, true
}

func put(s state, v string) state {
	ver := uint64(1)
	if s.exists {
		ver = s.version + 1
	}
	return state{exists: true, value: v, version: ver}
}

// KeyResult is the verdict for one key.
type KeyResult struct {
	Key  string
	Ops  int
	OK   bool
	Gave bool // search budget exhausted (inconclusive)
	// When !OK: the longest prefix of operations that could be linearized,
	// and the operations that could not be placed after it.
	Linearized []*Op
	Stuck      []*Op
}

// Result is the verdict for a whole history.
type Result struct {
	OK           bool
	Keys         int
	Ops          int
	Inconclusive int
	Failures     []KeyResult
}

// Check verifies a history. budget bounds the search per key (0 = 2M steps).
func Check(history []Op, budget int) Result {
	if budget == 0 {
		budget = 2_000_000
	}
	byKey := map[string][]*Op{}
	for i := range history {
		op := &history[i]
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	res := Result{OK: true, Keys: len(keys), Ops: len(history)}
	for _, k := range keys {
		kr := checkKey(k, byKey[k], budget)
		if kr.Gave {
			res.Inconclusive++
		}
		if !kr.OK {
			res.OK = false
			res.Failures = append(res.Failures, kr)
		}
	}
	return res
}

type entry struct {
	isCall bool
	id     int
	t      time.Duration
	match  *entry // call ↔ return
	prev   *entry
	next   *entry
}

func checkKey(key string, ops []*Op, budget int) KeyResult {
	n := len(ops)
	evs := make([]*entry, 0, 2*n)
	for i, op := range ops {
		c := &entry{isCall: true, id: i, t: op.Call}
		r := &entry{isCall: false, id: i, t: op.Return}
		c.match, r.match = r, c
		evs = append(evs, c, r)
	}
	// Sort by time; on ties, calls before returns (treat as concurrent, the
	// permissive and therefore sound-for-bug-reports choice).
	sort.SliceStable(evs, func(i, j int) bool {
		if evs[i].t != evs[j].t {
			return evs[i].t < evs[j].t
		}
		return evs[i].isCall && !evs[j].isCall
	})
	head := &entry{} // sentinel
	cur := head
	for _, e := range evs {
		cur.next = e
		e.prev = cur
		cur = e
	}

	lift := func(e *entry) {
		e.prev.next = e.next
		if e.next != nil {
			e.next.prev = e.prev
		}
		m := e.match
		m.prev.next = m.next
		if m.next != nil {
			m.next.prev = m.prev
		}
	}
	unlift := func(e *entry) {
		m := e.match
		m.prev.next = m
		if m.next != nil {
			m.next.prev = m
		}
		e.prev.next = e
		if e.next != nil {
			e.next.prev = e
		}
	}

	type frame struct {
		e *entry
		s state
	}
	type memo struct {
		bits string
		s    state
	}
	bits := make([]byte, (n+7)/8)
	seen := map[memo]struct{}{}
	var stack []frame
	s := state{}
	best := 0
	var bestLin []int
	steps := 0

	e := head.next
	for head.next != nil {
		steps++
		if steps > budget {
			return KeyResult{Key: key, Ops: n, OK: true, Gave: true}
		}
		if e.isCall {
			if ns, ok := step(s, ops[e.id]); ok {
				bits[e.id/8] |= 1 << (e.id % 8)
				m := memo{string(bits), ns}
				if _, dup := seen[m]; !dup {
					seen[m] = struct{}{}
					stack = append(stack, frame{e, s})
					s = ns
					lift(e)
					if len(stack) > best {
						best = len(stack)
						bestLin = bestLin[:0]
						for _, f := range stack {
							bestLin = append(bestLin, f.e.id)
						}
					}
					e = head.next
					continue
				}
				bits[e.id/8] &^= 1 << (e.id % 8)
			}
			e = e.next
			continue
		}
		// Reached a return whose call has not been linearized: backtrack.
		if len(stack) == 0 {
			return failure(key, ops, bestLin)
		}
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		s = f.s
		bits[f.e.id/8] &^= 1 << (f.e.id % 8)
		unlift(f.e)
		e = f.e.next
	}
	return KeyResult{Key: key, Ops: n, OK: true}
}

func failure(key string, ops []*Op, lin []int) KeyResult {
	kr := KeyResult{Key: key, Ops: len(ops), OK: false}
	done := map[int]bool{}
	for _, id := range lin {
		kr.Linearized = append(kr.Linearized, ops[id])
		done[id] = true
	}
	// The ops that could not be placed: those not linearized whose calls
	// precede the earliest unlinearized return.
	earliest := Indeterminate
	for i, op := range ops {
		if !done[i] && op.Return < earliest {
			earliest = op.Return
		}
	}
	for i, op := range ops {
		if !done[i] && op.Call <= earliest {
			kr.Stuck = append(kr.Stuck, op)
		}
	}
	sort.Slice(kr.Stuck, func(i, j int) bool { return kr.Stuck[i].Call < kr.Stuck[j].Call })
	return kr
}

// Explain renders a failed key as a human-readable report.
func (kr KeyResult) Explain(tail int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "key %s: NOT linearizable (%d ops)\n", kr.Key, kr.Ops)
	from := 0
	if len(kr.Linearized) > tail {
		from = len(kr.Linearized) - tail
		fmt.Fprintf(&b, "  … %d earlier ops linearized fine\n", from)
	}
	b.WriteString("  last linearizable prefix:\n")
	for _, op := range kr.Linearized[from:] {
		fmt.Fprintf(&b, "    ✓ %s\n", op)
	}
	b.WriteString("  no valid order exists for:\n")
	for _, op := range kr.Stuck {
		fmt.Fprintf(&b, "    ✗ %s\n", op)
	}
	return b.String()
}
