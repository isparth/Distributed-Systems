package kvsm

import (
	"encoding/json"
	"testing"

	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

func TestKVSM_PutGetDelete(t *testing.T) {
	sm := New()

	// Put
	res := sm.Apply(types.Command{Op: types.OpPut, Key: "k1", Value: "v1"})
	if !res.Ok {
		t.Fatalf("put failed: %s", res.ErrMsg)
	}

	// Get
	v, ok := sm.Get("k1")
	if !ok || v != "v1" {
		t.Fatalf("expected v1, got %q ok=%v", v, ok)
	}

	// Delete existing
	res = sm.Apply(types.Command{Op: types.OpDelete, Key: "k1"})
	if !res.Ok || res.Deleted != 1 {
		t.Fatalf("delete failed: ok=%v deleted=%d", res.Ok, res.Deleted)
	}

	// Get after delete
	_, ok = sm.Get("k1")
	if ok {
		t.Fatal("key should be gone")
	}

	// Delete non-existing
	res = sm.Apply(types.Command{Op: types.OpDelete, Key: "k1"})
	if !res.Ok || res.Deleted != 0 {
		t.Fatalf("delete non-existing: ok=%v deleted=%d", res.Ok, res.Deleted)
	}
}

func TestKVSM_CAS_SuccessAndFail(t *testing.T) {
	sm := New()

	// CAS on missing key (expected "" matches)
	res := sm.Apply(types.Command{Op: types.OpCAS, Key: "k1", Expected: "", Value: "v1"})
	if !res.Ok {
		t.Fatalf("cas on missing key should succeed: %s", res.ErrCode)
	}
	v, _ := sm.Get("k1")
	if v != "v1" {
		t.Fatalf("expected v1, got %q", v)
	}

	// CAS success
	res = sm.Apply(types.Command{Op: types.OpCAS, Key: "k1", Expected: "v1", Value: "v2"})
	if !res.Ok {
		t.Fatalf("cas should succeed")
	}

	// CAS fail
	res = sm.Apply(types.Command{Op: types.OpCAS, Key: "k1", Expected: "wrong", Value: "v3"})
	if res.Ok || res.ErrCode != "cas_failed" {
		t.Fatalf("cas should fail: ok=%v err=%s", res.Ok, res.ErrCode)
	}
	v, _ = sm.Get("k1")
	if v != "v2" {
		t.Fatalf("value should still be v2, got %q", v)
	}
}

func TestKVSM_BatchPutBatchDelete(t *testing.T) {
	sm := New()

	res := sm.Apply(types.Command{
		Op: types.OpBatchPut,
		Entries: []types.Entry{
			{Key: "a", Value: "1"},
			{Key: "b", Value: "2"},
			{Key: "c", Value: "3"},
		},
	})
	if !res.Ok {
		t.Fatalf("batch put failed")
	}

	vals := sm.MGet([]string{"a", "b", "c"})
	if vals["a"] != "1" || vals["b"] != "2" || vals["c"] != "3" {
		t.Fatalf("mget mismatch: %v", vals)
	}

	res = sm.Apply(types.Command{
		Op:   types.OpBatchDelete,
		Keys: []string{"a", "c", "nonexistent"},
	})
	if !res.Ok || res.Deleted != 2 {
		t.Fatalf("batch delete: ok=%v deleted=%d", res.Ok, res.Deleted)
	}

	_, ok := sm.Get("a")
	if ok {
		t.Fatal("a should be deleted")
	}
	v, ok := sm.Get("b")
	if !ok || v != "2" {
		t.Fatal("b should still exist")
	}
}

func TestKVSM_Dedupe_ReplaySameSeqReturnsSameReply(t *testing.T) {
	sm := New()

	cmd := types.Command{ClientID: "c1", Seq: 1, Op: types.OpPut, Key: "k", Value: "v1"}
	res1 := sm.Apply(cmd)
	if !res1.Ok {
		t.Fatal("first apply should succeed")
	}

	// Change the value in the command but keep same seq
	cmd.Value = "v2"
	res2 := sm.Apply(cmd)
	if !res2.Ok {
		t.Fatal("replay should return stored result")
	}

	// Value should still be v1 (dedupe prevented re-apply)
	v, _ := sm.Get("k")
	if v != "v1" {
		t.Fatalf("expected v1 (dedupe), got %q", v)
	}

	// Higher seq should apply
	cmd2 := types.Command{ClientID: "c1", Seq: 2, Op: types.OpPut, Key: "k", Value: "v3"}
	res3 := sm.Apply(cmd2)
	if !res3.Ok {
		t.Fatal("higher seq should apply")
	}
	v, _ = sm.Get("k")
	if v != "v3" {
		t.Fatalf("expected v3, got %q", v)
	}
}

func TestKVSM_SnapshotRestore_RoundTrip(t *testing.T) {
	sm := New()
	sm.Apply(types.Command{ClientID: "c1", Seq: 1, Op: types.OpPut, Key: "x", Value: "1"})
	sm.Apply(types.Command{Op: types.OpPut, Key: "y", Value: "2"})

	data, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Verify it's valid JSON
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatal(err)
	}

	// Restore into a new SM
	sm2 := New()
	if err := sm2.Restore(data); err != nil {
		t.Fatal(err)
	}

	v, ok := sm2.Get("x")
	if !ok || v != "1" {
		t.Fatalf("restored x: %q ok=%v", v, ok)
	}
	v, ok = sm2.Get("y")
	if !ok || v != "2" {
		t.Fatalf("restored y: %q ok=%v", v, ok)
	}

	// Dedupe should also be restored
	seq, ok := sm2.LastSeen("c1")
	if !ok || seq != 1 {
		t.Fatalf("dedupe not restored: seq=%d ok=%v", seq, ok)
	}
}

// --- M5 Tests ---

func TestKVSM_M5_SnapshotIncludesDedupe(t *testing.T) {
	sm := New()

	// Apply commands with client IDs for deduplication
	sm.Apply(types.Command{ClientID: "client1", Seq: 1, Op: types.OpPut, Key: "a", Value: "1"})
	sm.Apply(types.Command{ClientID: "client1", Seq: 2, Op: types.OpPut, Key: "b", Value: "2"})
	sm.Apply(types.Command{ClientID: "client2", Seq: 1, Op: types.OpPut, Key: "c", Value: "3"})

	data, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Verify snapshot contains dedupe records
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatal(err)
	}

	if len(snap.Dedupe) != 2 {
		t.Fatalf("expected 2 dedupe records, got %d", len(snap.Dedupe))
	}

	if snap.Dedupe["client1"].LastSeq != 2 {
		t.Fatalf("expected client1 last seq 2, got %d", snap.Dedupe["client1"].LastSeq)
	}
	if snap.Dedupe["client2"].LastSeq != 1 {
		t.Fatalf("expected client2 last seq 1, got %d", snap.Dedupe["client2"].LastSeq)
	}
}

func TestKVSM_M5_RestoreResetsStateCorrectly(t *testing.T) {
	// Create first state machine with some data
	sm1 := New()
	sm1.Apply(types.Command{ClientID: "c1", Seq: 1, Op: types.OpPut, Key: "old", Value: "data"})
	sm1.Apply(types.Command{ClientID: "c1", Seq: 2, Op: types.OpPut, Key: "more", Value: "stuff"})

	// Create second state machine with different data
	sm2 := New()
	sm2.Apply(types.Command{ClientID: "c2", Seq: 5, Op: types.OpPut, Key: "new", Value: "values"})

	// Snapshot sm2
	data, err := sm2.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Restore sm2's snapshot into sm1
	if err := sm1.Restore(data); err != nil {
		t.Fatal(err)
	}

	// Verify old data is gone
	_, ok := sm1.Get("old")
	if ok {
		t.Fatal("old key should be gone after restore")
	}
	_, ok = sm1.Get("more")
	if ok {
		t.Fatal("more key should be gone after restore")
	}

	// Verify new data is present
	v, ok := sm1.Get("new")
	if !ok || v != "values" {
		t.Fatalf("expected new=values, got %q ok=%v", v, ok)
	}

	// Verify dedupe was reset
	_, ok = sm1.LastSeen("c1")
	if ok {
		t.Fatal("c1 dedupe should be gone after restore")
	}

	seq, ok := sm1.LastSeen("c2")
	if !ok || seq != 5 {
		t.Fatalf("c2 dedupe should be restored: seq=%d ok=%v", seq, ok)
	}
}
