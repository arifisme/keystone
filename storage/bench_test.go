package storage

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

var valueSizes = []int{100, 1024, 10 * 1024}

func benchKey(i int) []byte {
	return []byte(fmt.Sprintf("key%012d", i))
}

func reportLatency(b *testing.B, samples []time.Duration) {
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	b.ReportMetric(float64(samples[len(samples)/2].Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(samples[len(samples)*99/100].Nanoseconds()), "p99-ns")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}

func benchPut(b *testing.B, size int, random bool, sync SyncPolicy) {
	d, err := Open(b.TempDir(), Options{MemtableSize: 64 << 20, Sync: sync})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	value := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(value)
	rng := rand.New(rand.NewSource(2))
	samples := make([]time.Duration, 0, b.N)
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := i
		if random {
			k = rng.Intn(1 << 24)
		}
		start := time.Now()
		if err := d.Put(benchKey(k), value); err != nil {
			b.Fatal(err)
		}
		samples = append(samples, time.Since(start))
	}
	b.StopTimer()
	reportLatency(b, samples)
}

func BenchmarkPut(b *testing.B) {
	for _, size := range valueSizes {
		for _, random := range []bool{false, true} {
			for _, sync := range []struct {
				name   string
				policy SyncPolicy
			}{{"fsync", SyncAlways}, {"nosync", SyncInterval(10 * time.Millisecond)}} {
				order := "seq"
				if random {
					order = "rand"
				}
				b.Run(fmt.Sprintf("%s/%dB/%s", order, size, sync.name), func(b *testing.B) {
					benchPut(b, size, random, sync.policy)
				})
			}
		}
	}
}

func BenchmarkPutParallel(b *testing.B) {
	for _, size := range valueSizes {
		b.Run(fmt.Sprintf("%dB/fsync", size), func(b *testing.B) {
			d, err := Open(b.TempDir(), Options{MemtableSize: 64 << 20, Sync: SyncAlways})
			if err != nil {
				b.Fatal(err)
			}
			defer d.Close()
			value := make([]byte, size)
			b.SetBytes(int64(size))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					if err := d.Put(benchKey(rng.Intn(1<<24)), value); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
		})
	}
}

func benchGet(b *testing.B, size int, random bool) {
	const keys = 100000
	d, err := Open(b.TempDir(), Options{MemtableSize: 8 << 20, Sync: SyncInterval(10 * time.Millisecond)})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	value := make([]byte, size)
	for i := 0; i < keys; i++ {
		if err := d.Put(benchKey(i), value); err != nil {
			b.Fatal(err)
		}
	}
	waitIdleBench(d)
	rng := rand.New(rand.NewSource(3))
	samples := make([]time.Duration, 0, b.N)
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := i % keys
		if random {
			k = rng.Intn(keys)
		}
		start := time.Now()
		if _, err := d.Get(benchKey(k)); err != nil {
			b.Fatal(err)
		}
		samples = append(samples, time.Since(start))
	}
	b.StopTimer()
	reportLatency(b, samples)
}

func waitIdleBench(d DB) {
	db := d.(*db)
	for {
		db.mu.Lock()
		idle := len(db.current.imm) == 0 && pickCompaction(db.current.tables) == nil
		db.mu.Unlock()
		if idle {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func BenchmarkGet(b *testing.B) {
	for _, size := range valueSizes {
		for _, random := range []bool{false, true} {
			order := "seq"
			if random {
				order = "rand"
			}
			b.Run(fmt.Sprintf("%s/%dB", order, size), func(b *testing.B) {
				benchGet(b, size, random)
			})
		}
	}
}

// BenchmarkRecovery measures reopening a database whose WAL holds about
// 1 GB of unflushed writes. The memtable limit is set high enough that
// nothing flushes, so recovery replays the whole log.
func BenchmarkRecovery(b *testing.B) {
	const total = 1 << 30
	dir := b.TempDir()
	d, err := Open(dir, Options{MemtableSize: 4 << 30, Sync: SyncInterval(50 * time.Millisecond)})
	if err != nil {
		b.Fatal(err)
	}
	value := make([]byte, 1024)
	for written := 0; written < total; written += len(value) + 15 {
		if err := d.Put(benchKey(written), value); err != nil {
			b.Fatal(err)
		}
	}
	if err := d.Close(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		d, err := Open(dir, Options{MemtableSize: 4 << 30})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(time.Since(start).Seconds(), "recovery-s")
		b.StopTimer()
		d.Close()
		b.StartTimer()
	}
}
