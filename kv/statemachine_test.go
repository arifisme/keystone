package kv

import (
	"bytes"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/arifisme/keystone/proto"
	"github.com/arifisme/keystone/raft"
	"github.com/arifisme/keystone/storage"
)

func openSM(t *testing.T, dir string) *StateMachine {
	t.Helper()
	db, err := storage.Open(dir, storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	sm, err := NewStateMachine(db)
	if err != nil {
		t.Fatal(err)
	}
	return sm
}

func entry(index uint64, cmd *pb.Command) raft.Entry {
	data, err := proto.Marshal(cmd)
	if err != nil {
		panic(err)
	}
	return raft.Entry{Index: index, Term: 1, Type: raft.EntryNormal, Data: data}
}

func result(t *testing.T, b []byte) *pb.Result {
	t.Helper()
	var r pb.Result
	if err := proto.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return &r
}

func put(client, seq uint64, key, value string) *pb.Command {
	return &pb.Command{ClientId: client, Seq: seq, Op: &pb.Command_Put{Put: &pb.PutOp{Key: []byte(key), Value: []byte(value)}}}
}

func cas(client, seq uint64, key string, expected *string, value string) *pb.Command {
	op := &pb.CasOp{Key: []byte(key), Value: []byte(value)}
	if expected != nil {
		op.Expected = []byte(*expected)
	}
	return &pb.Command{ClientId: client, Seq: seq, Op: &pb.Command_Cas{Cas: op}}
}

func TestRegisterAssignsLogIndexAsClientID(t *testing.T) {
	sm := openSM(t, t.TempDir())
	r := result(t, sm.Apply(entry(7, &pb.Command{Op: &pb.Command_Register{Register: &pb.RegisterOp{}}})))
	if r.ClientId != 7 {
		t.Fatalf("client id = %d", r.ClientId)
	}
	if sm.LastApplied() != 7 {
		t.Fatalf("applied = %d", sm.LastApplied())
	}
}

func TestDuplicateCommandReturnsCachedResultWithoutReapplying(t *testing.T) {
	sm := openSM(t, t.TempDir())
	sm.Apply(entry(1, &pb.Command{Op: &pb.Command_Register{Register: &pb.RegisterOp{}}}))
	absent := (*string)(nil)
	first := result(t, sm.Apply(entry(2, cas(1, 1, "k", absent, "v1"))))
	if !first.Success {
		t.Fatal("first cas should succeed on absent key")
	}
	again := result(t, sm.Apply(entry(3, cas(1, 1, "k", absent, "v1"))))
	if !again.Success {
		t.Fatal("retried cas must return the cached success, not re-evaluate")
	}
	v, found, _ := sm.Get([]byte("k"))
	if !found || string(v) != "v1" {
		t.Fatalf("k = %q %v", v, found)
	}
	if sm.LastApplied() != 3 {
		t.Fatalf("applied index must advance past duplicates, got %d", sm.LastApplied())
	}
	older := result(t, sm.Apply(entry(4, put(1, 0, "k", "stale"))))
	if !older.Success {
		t.Fatal("older sequence returns the cached latest result")
	}
	if v, _, _ := sm.Get([]byte("k")); string(v) != "v1" {
		t.Fatalf("older sequence was applied: k = %q", v)
	}
}

func TestCasSemantics(t *testing.T) {
	sm := openSM(t, t.TempDir())
	one, two := "1", "2"
	cases := []struct {
		name     string
		expected *string
		value    string
		success  bool
		after    string
	}{
		{"absent expected on absent key", nil, "1", true, "1"},
		{"absent expected on present key", nil, "x", false, "1"},
		{"mismatch", &two, "x", false, "1"},
		{"match", &one, "2", true, "2"},
	}
	for i, c := range cases {
		r := result(t, sm.Apply(entry(uint64(i+1), cas(0, 0, "k", c.expected, c.value))))
		if r.Success != c.success {
			t.Fatalf("%s: success = %v", c.name, r.Success)
		}
		v, _, _ := sm.Get([]byte("k"))
		if string(v) != c.after {
			t.Fatalf("%s: k = %q, want %q", c.name, v, c.after)
		}
	}
}

func TestAppliedIndexAndSessionsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	db, _ := storage.Open(dir, storage.Options{})
	sm, _ := NewStateMachine(db)
	sm.Apply(entry(1, &pb.Command{Op: &pb.Command_Register{Register: &pb.RegisterOp{}}}))
	sm.Apply(entry(2, put(1, 5, "a", "b")))
	db.Close()

	sm = openSM(t, dir)
	if sm.LastApplied() != 2 {
		t.Fatalf("applied = %d", sm.LastApplied())
	}
	r := result(t, sm.Apply(entry(3, put(1, 5, "a", "other"))))
	if !r.Success {
		t.Fatal("cached result lost")
	}
	if v, _, _ := sm.Get([]byte("a")); string(v) != "b" {
		t.Fatalf("duplicate applied after reopen: a = %q", v)
	}
}

func TestSnapshotCarriesDataAndSessions(t *testing.T) {
	src := openSM(t, t.TempDir())
	src.Apply(entry(1, &pb.Command{Op: &pb.Command_Register{Register: &pb.RegisterOp{}}}))
	for i := uint64(0); i < 50; i++ {
		src.Apply(entry(2+i, put(1, i+1, fmt.Sprintf("key%02d", i), fmt.Sprintf("val%d", i))))
	}
	rc, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	dst := openSM(t, t.TempDir())
	dst.Apply(entry(1, put(0, 0, "junk", "x")))
	if err := dst.Restore(rc); err != nil {
		t.Fatal(err)
	}
	if dst.LastApplied() != 51 {
		t.Fatalf("applied after restore = %d", dst.LastApplied())
	}
	kvs, _ := dst.Scan(nil, nil, 0)
	if len(kvs) != 50 || string(kvs[0].Key) != "key00" || string(kvs[49].Value) != "val49" {
		t.Fatalf("restored %d keys, first %q", len(kvs), kvs[0].Key)
	}
	r := result(t, dst.Apply(entry(52, put(1, 50, "key49", "dup"))))
	if !r.Success {
		t.Fatal("session missing from snapshot")
	}
	if v, _, _ := dst.Get([]byte("key49")); string(v) != "val49" {
		t.Fatalf("duplicate applied after restore: %q", v)
	}
}

func TestScanStaysWithinUserKeys(t *testing.T) {
	sm := openSM(t, t.TempDir())
	sm.Apply(entry(1, &pb.Command{Op: &pb.Command_Register{Register: &pb.RegisterOp{}}}))
	sm.Apply(entry(2, put(1, 1, "b", "2")))
	sm.Apply(entry(3, put(1, 2, "a", "1")))
	sm.Apply(entry(4, put(1, 3, "c", "3")))
	kvs, err := sm.Scan(nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 3 || string(kvs[0].Key) != "a" || string(kvs[2].Key) != "c" {
		t.Fatalf("scan = %v", kvs)
	}
	kvs, _ = sm.Scan([]byte("b"), nil, 1)
	if len(kvs) != 1 || string(kvs[0].Key) != "b" {
		t.Fatalf("bounded scan = %v", kvs)
	}
	kvs, _ = sm.Scan([]byte("a"), []byte("c"), 0)
	if len(kvs) != 2 || !bytes.Equal(kvs[1].Key, []byte("b")) {
		t.Fatalf("ranged scan = %v", kvs)
	}
}
