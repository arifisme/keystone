package storage

import (
	"fmt"
	"testing"
)

func buildTestBlock(n int) ([]byte, [][]byte) {
	var b blockBuilder
	var keys [][]byte
	for i := 0; i < n; i++ {
		k := makeInternalKey(nil, []byte(fmt.Sprintf("key%05d", i*2)), 1, kindPut)
		keys = append(keys, k)
		b.add(k, []byte(fmt.Sprintf("val%d", i)))
	}
	return b.finish(), keys
}

func TestBlockIterationReturnsEveryEntryInOrder(t *testing.T) {
	block, keys := buildTestBlock(100)
	it, err := newBlockIter(block)
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	for it.seekToFirst(); it.valid(); it.next() {
		if string(it.key()) != string(keys[i]) {
			t.Fatalf("entry %d: %q", i, userKey(it.key()))
		}
		if string(it.value()) != fmt.Sprintf("val%d", i) {
			t.Fatalf("entry %d value %q", i, it.value())
		}
		i++
	}
	if i != 100 {
		t.Fatalf("iterated %d", i)
	}
}

func TestBlockSeekLandsOnFirstKeyAtOrAfterTarget(t *testing.T) {
	block, keys := buildTestBlock(100)
	it, _ := newBlockIter(block)
	cases := []struct {
		target string
		want   int
	}{
		{"key00000", 0},
		{"key00001", 1},
		{"key00034", 17},
		{"key00035", 18},
		{"key00198", 99},
		{"key00199", -1},
		{"a", 0},
	}
	for _, c := range cases {
		it.seek(makeInternalKey(nil, []byte(c.target), ^uint64(0)>>8, kindPut))
		if c.want < 0 {
			if it.valid() {
				t.Fatalf("seek %q should be exhausted, at %q", c.target, userKey(it.key()))
			}
			continue
		}
		if !it.valid() || string(it.key()) != string(keys[c.want]) {
			t.Fatalf("seek %q landed on %q", c.target, userKey(it.key()))
		}
	}
}

func TestBlockRejectsTruncatedData(t *testing.T) {
	block, _ := buildTestBlock(10)
	if _, err := newBlockIter(block[:2]); err == nil {
		t.Fatal("expected error")
	}
	bad := append([]byte(nil), block...)
	bad[len(bad)-1] = 0xff
	if _, err := newBlockIter(bad); err == nil {
		t.Fatal("expected error on bogus restart count")
	}
}
