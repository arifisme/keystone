package kv

import (
	"bytes"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/arifisme/keystone/internal/keyhash"
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
	kvs, _ := dst.Scan(nil, nil, 0, 0)
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
	kvs, err := sm.Scan(nil, nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 3 || string(kvs[0].Key) != "a" || string(kvs[2].Key) != "c" {
		t.Fatalf("scan = %v", kvs)
	}
	kvs, _ = sm.Scan([]byte("b"), nil, 1, 0)
	if len(kvs) != 1 || string(kvs[0].Key) != "b" {
		t.Fatalf("bounded scan = %v", kvs)
	}
	kvs, _ = sm.Scan([]byte("a"), []byte("c"), 0, 0)
	if len(kvs) != 2 || !bytes.Equal(kvs[1].Key, []byte("b")) {
		t.Fatalf("ranged scan = %v", kvs)
	}
}

func TestScanStopsWithThePairThatReachesTheByteBound(t *testing.T) {
	sm := openSM(t, t.TempDir())
	for i, k := range []string{"a", "b", "c", "d"} {
		sm.Apply(entry(uint64(i+1), put(0, 0, k, "12345")))
	}
	// A pair is six bytes of key and value.
	for _, tc := range []struct {
		maxBytes, want int
	}{
		{0, 4},
		{1, 1},
		{6, 1},
		{7, 2},
		{12, 2},
		{1000, 4},
	} {
		kvs, err := sm.Scan(nil, nil, 0, tc.maxBytes)
		if err != nil || len(kvs) != tc.want {
			t.Errorf("max %d bytes: %d pairs, %v, want %d", tc.maxBytes, len(kvs), err, tc.want)
		}
	}
	res := result(t, sm.Apply(entry(5, &pb.Command{Op: &pb.Command_Scan{Scan: &pb.ScanOp{MaxBytes: 7}}})))
	if len(res.Kvs) != 2 {
		t.Fatalf("scan through the log with 7 bytes: %d pairs, want 2", len(res.Kvs))
	}
}

func importOp(shard int, last bool, key, value string) *pb.Command {
	return &pb.Command{Op: &pb.Command_Import{Import: &pb.ImportOp{
		Shard: uint32(shard),
		Kvs:   []*pb.KeyValue{{Key: []byte(key), Value: []byte(value)}},
		Last:  last,
	}}}
}

// A late or repeated chunk carries the source's frozen copy. Once the
// import has finished, clients write here, and the chunk would put an old
// value over a new one.
func TestImportIsRefusedWhileTheShardIsHeldHere(t *testing.T) {
	sm := openSM(t, t.TempDir())
	shard := keyhash.ShardOf([]byte("k"))
	value := func() string {
		t.Helper()
		v, _, err := sm.Get([]byte("k"))
		if err != nil {
			t.Fatal(err)
		}
		return string(v)
	}

	if !result(t, sm.Apply(entry(1, importOp(shard, false, "k", "copied")))).Success {
		t.Fatal("first chunk refused")
	}
	if !result(t, sm.Apply(entry(2, importOp(shard, false, "k", "copied")))).Success {
		t.Fatal("repeated chunk refused before the import finished")
	}
	if !result(t, sm.Apply(entry(3, importOp(shard, true, "k", "copied")))).Success {
		t.Fatal("last chunk refused")
	}
	sm.Apply(entry(4, put(0, 0, "k", "written after the move")))
	if result(t, sm.Apply(entry(5, importOp(shard, false, "k", "copied")))).Success || value() != "written after the move" {
		t.Fatalf("late chunk accepted over a newer value: k = %q", value())
	}

	sm.Apply(entry(6, &pb.Command{Op: &pb.Command_Freeze{Freeze: &pb.FreezeOp{Shard: uint32(shard)}}}))
	if result(t, sm.Apply(entry(7, importOp(shard, false, "k", "copied")))).Success {
		t.Fatal("chunk accepted into a shard that is being moved away")
	}
	sm.Apply(entry(8, &pb.Command{Op: &pb.Command_Purge{Purge: &pb.PurgeOp{Shard: uint32(shard)}}}))
	if !result(t, sm.Apply(entry(9, importOp(shard, true, "k", "moved back")))).Success {
		t.Fatal("import refused after the shard had left")
	}
	if value() != "moved back" {
		t.Fatalf("k = %q after moving back", value())
	}
}
