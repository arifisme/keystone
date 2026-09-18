package storage

import (
	"fmt"
	"testing"
)

func TestBloomContainsEveryInsertedKey(t *testing.T) {
	var keys [][]byte
	for i := 0; i < 10000; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key-%d", i)))
	}
	f := buildBloom(keys)
	for _, k := range keys {
		if !f.mayContain(k) {
			t.Fatalf("false negative for %q", k)
		}
	}
}

func TestBloomFalsePositiveRateIsAboutOnePercent(t *testing.T) {
	var keys [][]byte
	for i := 0; i < 10000; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key-%d", i)))
	}
	f := buildBloom(keys)
	hits := 0
	const probes = 100000
	for i := 0; i < probes; i++ {
		if f.mayContain([]byte(fmt.Sprintf("other-%d", i))) {
			hits++
		}
	}
	rate := float64(hits) / probes
	if rate > 0.02 {
		t.Fatalf("false positive rate %.4f too high", rate)
	}
}

func TestBloomEmptyFilterAdmitsEverything(t *testing.T) {
	if !bloomFilter(nil).mayContain([]byte("x")) {
		t.Fatal("nil filter must not reject")
	}
	f := buildBloom(nil)
	if f.mayContain([]byte("x")) {
		t.Fatal("empty built filter should reject unknown key")
	}
}
