package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
)

func TestInternalKeyOrdersUserKeyAscendingThenSequenceDescending(t *testing.T) {
	a1 := makeInternalKey(nil, []byte("a"), 1, kindPut)
	a5 := makeInternalKey(nil, []byte("a"), 5, kindPut)
	ab := makeInternalKey(nil, []byte("ab"), 9, kindPut)
	b := makeInternalKey(nil, []byte("b"), 1, kindDelete)
	if compareInternalKey(a5, a1) >= 0 {
		t.Fatal("newer version should sort first")
	}
	if compareInternalKey(a1, ab) >= 0 {
		t.Fatal("a should precede ab regardless of sequence")
	}
	if compareInternalKey(ab, b) >= 0 {
		t.Fatal("ab should precede b")
	}
	user, seq, kind := splitInternalKey(b)
	if string(user) != "b" || seq != 1 || kind != kindDelete {
		t.Fatalf("split = %q %d %d", user, seq, kind)
	}
}

func TestSkiplistKeepsKeysSorted(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	s := newSkiplist(rng.Uint64())
	var keys []string
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("k%06d", rng.Intn(100000))
		keys = append(keys, k)
		s.insert(makeInternalKey(nil, []byte(k), uint64(i), kindPut), []byte(k))
	}
	sort.Strings(keys)
	i := 0
	for n := s.first(); n != nil; n = n.next[0].Load() {
		if string(userKey(n.key)) != keys[i] {
			t.Fatalf("position %d: %q, want %q", i, userKey(n.key), keys[i])
		}
		i++
	}
	if i != len(keys) {
		t.Fatalf("iterated %d, want %d", i, len(keys))
	}
	n := s.seek(makeInternalKey(nil, []byte("k050000"), ^uint64(0)>>8, kindPut))
	if n == nil || bytes.Compare(userKey(n.key), []byte("k050000")) < 0 {
		t.Fatal("seek landed before target")
	}
}

func TestSkiplistReadersSeeConsistentStateDuringInserts(t *testing.T) {
	s := newSkiplist(7)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var prev []byte
				for n := s.first(); n != nil; n = n.next[0].Load() {
					if prev != nil && compareInternalKey(prev, n.key) >= 0 {
						t.Error("out of order during concurrent insert")
						return
					}
					prev = n.key
				}
			}
		}()
	}
	for i := 0; i < 20000; i++ {
		s.insert(makeInternalKey(nil, []byte(fmt.Sprintf("%08d", i*7919%20000)), uint64(i), kindPut), nil)
	}
	close(stop)
	wg.Wait()
}

func TestMemtableGetReturnsNewestVisibleVersion(t *testing.T) {
	m := newMemtable(1, 1)
	m.put(1, kindPut, []byte("k"), []byte("v1"))
	m.put(2, kindPut, []byte("k"), []byte("v2"))
	m.put(3, kindDelete, []byte("k"), nil)

	v, found, deleted := m.get([]byte("k"), 2)
	if !found || deleted || string(v) != "v2" {
		t.Fatalf("at seq 2: %q %v %v", v, found, deleted)
	}
	_, found, deleted = m.get([]byte("k"), 3)
	if !found || !deleted {
		t.Fatal("tombstone at seq 3 not visible")
	}
	_, found, _ = m.get([]byte("k"), 0)
	if found {
		t.Fatal("nothing should be visible at seq 0")
	}
	_, found, _ = m.get([]byte("kk"), 10)
	if found {
		t.Fatal("prefix key must not match")
	}
}
