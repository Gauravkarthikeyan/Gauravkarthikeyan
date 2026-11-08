package checker

import (
	"testing"
	"time"

	"github.com/Gauravkarthikeyan/tessera/internal/kv"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func mkput(c uint64, v string, call, ret int, ver uint64) Op {
	return Op{ClientID: c, Key: "k", Kind: kv.OpPut, Value: v, Call: ms(call), Return: ms(ret), Out: kv.Result{Version: ver}}
}
func get(c uint64, call, ret int, found bool, v string, ver uint64) Op {
	return Op{ClientID: c, Key: "k", Kind: kv.OpGet, Call: ms(call), Return: ms(ret), Out: kv.Result{Found: found, Value: v, Version: ver}}
}

func TestSequentialOK(t *testing.T) {
	h := []Op{
		mkput(1, "a", 0, 1, 1),
		get(1, 2, 3, true, "a", 1),
		mkput(1, "b", 4, 5, 2),
		get(1, 6, 7, true, "b", 2),
	}
	if r := Check(h, 0); !r.OK {
		t.Fatalf("expected linearizable: %+v", r)
	}
}

func TestConcurrentReadMayObserveEitherSide(t *testing.T) {
	// The put overlaps both reads; reading old then new is fine.
	h := []Op{
		mkput(1, "a", 0, 1, 1),
		mkput(1, "b", 10, 30, 2),
		get(2, 12, 14, true, "a", 1),
		get(3, 15, 20, true, "b", 2),
	}
	if r := Check(h, 0); !r.OK {
		t.Fatalf("expected linearizable")
	}
}

func TestStaleReadDetected(t *testing.T) {
	// Put b completes at 20; a read starting at 25 still sees a.
	h := []Op{
		mkput(1, "a", 0, 1, 1),
		mkput(1, "b", 10, 20, 2),
		get(2, 25, 26, true, "a", 1),
	}
	r := Check(h, 0)
	if r.OK {
		t.Fatalf("stale read not detected")
	}
	if len(r.Failures[0].Stuck) == 0 {
		t.Fatalf("expected explanation")
	}
}

func TestNonMonotonicReadsDetected(t *testing.T) {
	// Two sequential reads during a long put see new then old: impossible.
	h := []Op{
		mkput(1, "a", 0, 1, 1),
		mkput(1, "b", 10, 100, 2),
		get(2, 20, 21, true, "b", 2),
		get(2, 30, 31, true, "a", 1),
	}
	if Check(h, 0).OK {
		t.Fatalf("new-then-old read not detected")
	}
}

func TestIndeterminateWriteMayOrMayNotHappen(t *testing.T) {
	lost := mkput(1, "b", 10, 0, 0)
	lost.Return, lost.Unknown = Indeterminate, true
	// Case 1: later read does not see it.
	h1 := []Op{mkput(1, "a", 0, 1, 1), lost, get(2, 50, 51, true, "a", 1)}
	if !Check(h1, 0).OK {
		t.Fatalf("indeterminate write must be allowed to not happen")
	}
	// Case 2: later read sees it.
	h2 := []Op{mkput(1, "a", 0, 1, 1), lost, get(2, 50, 51, true, "b", 2)}
	if !Check(h2, 0).OK {
		t.Fatalf("indeterminate write must be allowed to happen")
	}
	// Case 3: it cannot happen before it was called.
	h3 := []Op{mkput(1, "a", 0, 1, 1), get(2, 2, 3, true, "b", 2), lost}
	if Check(h3, 0).OK {
		t.Fatalf("effect observed before call must be rejected")
	}
}

func TestDoubleApplyDetectedThroughVersions(t *testing.T) {
	// One logical put returned version 2 although nothing else wrote: the
	// put was applied twice (lost dedup).
	h := []Op{mkput(1, "a", 0, 1, 2)}
	if Check(h, 0).OK {
		t.Fatalf("double-apply not detected")
	}
}

func TestCAS(t *testing.T) {
	cas := func(c uint64, exp uint64, v string, call, ret int, code kv.Code, ver uint64) Op {
		return Op{ClientID: c, Key: "k", Kind: kv.OpCAS, Expect: exp, Value: v, Call: ms(call), Return: ms(ret), Out: kv.Result{Code: code, Version: ver}}
	}
	// Two concurrent put-if-absent: exactly one may win.
	ok := []Op{cas(1, 0, "x", 0, 10, kv.OK, 1), cas(2, 0, "y", 0, 10, kv.CondFailed, 0)}
	if !Check(ok, 0).OK {
		t.Fatalf("one winner should be fine")
	}
	bad := []Op{cas(1, 0, "x", 0, 10, kv.OK, 1), cas(2, 0, "y", 0, 10, kv.OK, 1)}
	if Check(bad, 0).OK {
		t.Fatalf("two winners of put-if-absent not detected")
	}
}
