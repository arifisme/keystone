package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func cmdChart(args []string) {
	fs := flag.NewFlagSet("chart", flag.ExitOnError)
	in := fs.String("in", "bench/results", "directory of JSON results")
	out := fs.String("out", "bench/results", "directory for SVG files")
	fs.Parse(args)
	results, failover := loadResults(*in)
	write := func(name, svg string) {
		if err := os.WriteFile(filepath.Join(*out, name), []byte(svg), 0o644); err != nil {
			fail(err)
		}
	}
	find := func(pred func(Result) bool) map[string]Result {
		m := map[string]Result{}
		for _, r := range results {
			if pred(r) {
				m[r.Name] = r
			}
		}
		return m
	}

	sizes := []string{"100 B", "1 KB", "10 KB"}
	sizeKeys := []int{100, 1024, 10240}
	var tput, p99 []series
	for _, nodes := range []int{3, 5} {
		s := series{name: fmt.Sprintf("%d nodes", nodes)}
		l := series{name: fmt.Sprintf("%d nodes", nodes)}
		for _, v := range sizeKeys {
			r := find(func(r Result) bool { return r.Op == "put" && r.Batched && r.Nodes == nodes && r.ValueSize == v })
			for _, x := range r {
				s.values = append(s.values, x.Throughput)
				l.values = append(l.values, float64(x.P99)/1e6)
			}
		}
		tput = append(tput, s)
		p99 = append(p99, l)
	}
	write("put-throughput.svg", barChart("Put throughput, 32 clients", sizes, tput, "ops/s"))
	write("put-p99.svg", barChart("Put p99 latency, 32 clients", sizes, p99, "ms"))

	var rt, rl []series
	for _, read := range []string{"readindex", "log"} {
		s := series{name: read}
		l := series{name: read}
		for _, nodes := range []int{3, 5} {
			for _, x := range find(func(r Result) bool { return r.Op == "get" && r.Read == read && r.Nodes == nodes }) {
				s.values = append(s.values, x.Throughput)
				l.values = append(l.values, float64(x.P99)/1e6)
			}
		}
		rt = append(rt, s)
		rl = append(rl, l)
	}
	write("get-throughput.svg", barChart("Get throughput, 1 KB values", []string{"3 nodes", "5 nodes"}, rt, "ops/s"))
	write("get-p99.svg", barChart("Get p99 latency, 1 KB values", []string{"3 nodes", "5 nodes"}, rl, "ms"))

	var bt []series
	for _, batched := range []bool{true, false} {
		name := "batched"
		if !batched {
			name = "unbatched"
		}
		s := series{name: name}
		for _, nodes := range []int{3, 5} {
			for _, x := range find(func(r Result) bool {
				return r.Op == "put" && r.ValueSize == 1024 && r.Batched == batched && r.Nodes == nodes
			}) {
				s.values = append(s.values, x.Throughput)
			}
		}
		bt = append(bt, s)
	}
	write("batching.svg", barChart("Put throughput, 1 KB, batched vs unbatched append", []string{"3 nodes", "5 nodes"}, bt, "ops/s"))

	if failover != nil {
		write("failover.svg", timeline(*failover))
	}
}

func loadResults(dir string) ([]Result, *Failover) {
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	var results []Result
	var failover *Failover
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			fail(err)
		}
		if filepath.Base(f) == "failover.json" {
			failover = &Failover{}
			if err := json.Unmarshal(b, failover); err != nil {
				fail(err)
			}
			continue
		}
		var r Result
		if err := json.Unmarshal(b, &r); err != nil {
			fail(err)
		}
		results = append(results, r)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })
	return results, failover
}

type series struct {
	name   string
	values []float64
}

var palette = []string{"#3b6ea5", "#e0a458", "#6aa56b", "#b85c5c"}

// barChart draws grouped bars with the value printed above each bar.
func barChart(title string, labels []string, data []series, unit string) string {
	const (
		w, h    = 640, 320
		left    = 60
		top     = 50
		bottom  = 50
		right   = 20
		fontFam = "font-family='-apple-system, Segoe UI, Helvetica, Arial, sans-serif'"
	)
	maxV := 0.0
	for _, s := range data {
		for _, v := range s.values {
			if v > maxV {
				maxV = v
			}
		}
	}
	if maxV == 0 {
		maxV = 1
	}
	plotW := float64(w - left - right)
	plotH := float64(h - top - bottom)
	groups := len(labels)
	gw := plotW / float64(groups)
	bw := gw * 0.8 / float64(len(data))
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" %s font-size="12">`, w, h, w, h, fontFam)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="white"/>`, w, h)
	fmt.Fprintf(&b, `<text x="%d" y="24" font-size="15" font-weight="600" fill="#222">%s</text>`, left, title)
	for i := 0; i <= 4; i++ {
		y := float64(top) + plotH - plotH*float64(i)/4
		fmt.Fprintf(&b, `<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#e5e5e5"/>`, left, y, w-right, y)
		fmt.Fprintf(&b, `<text x="%d" y="%.1f" text-anchor="end" fill="#666">%s</text>`, left-6, y+4, fmtNum(maxV*float64(i)/4))
	}
	for gi, label := range labels {
		x0 := float64(left) + gw*float64(gi) + gw*0.1
		for si, s := range data {
			if gi >= len(s.values) {
				continue
			}
			v := s.values[gi]
			bh := plotH * v / maxV
			x := x0 + bw*float64(si)
			y := float64(top) + plotH - bh
			fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="%s" rx="2"/>`, x, y, bw-3, bh, palette[si%len(palette)])
			fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle" fill="#333" font-size="11">%s</text>`, x+(bw-3)/2, y-4, fmtNum(v))
		}
		fmt.Fprintf(&b, `<text x="%.1f" y="%d" text-anchor="middle" fill="#333">%s</text>`, x0+gw*0.4, h-bottom+18, label)
	}
	fmt.Fprintf(&b, `<text x="%d" y="%d" fill="#666">%s</text>`, left, h-8, unit)
	lx := w - right - 110
	for si, s := range data {
		y := top - 30 + 16*si
		fmt.Fprintf(&b, `<rect x="%d" y="%d" width="10" height="10" fill="%s"/><text x="%d" y="%d" fill="#333">%s</text>`, lx, y, palette[si%len(palette)], lx+14, y+9, s.name)
	}
	b.WriteString(`</svg>`)
	return b.String()
}

// timeline draws successful writes per bucket with the kill marked.
func timeline(f Failover) string {
	const (
		w, h   = 640, 260
		left   = 50
		top    = 40
		bottom = 40
		right  = 20
	)
	maxV := 0
	for _, v := range f.Timeline {
		if v > maxV {
			maxV = v
		}
	}
	if maxV == 0 {
		maxV = 1
	}
	plotW := float64(w - left - right)
	plotH := float64(h - top - bottom)
	n := len(f.Timeline)
	bw := plotW / float64(n)
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" font-family="-apple-system, Segoe UI, Helvetica, Arial, sans-serif" font-size="12">`, w, h, w, h)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="white"/>`, w, h)
	fmt.Fprintf(&b, `<text x="%d" y="22" font-size="15" font-weight="600" fill="#222">Successful writes per %s while the leader is killed, %d nodes</text>`, left, f.Bucket, f.Nodes)
	for i, v := range f.Timeline {
		bh := plotH * float64(v) / float64(maxV)
		x := float64(left) + bw*float64(i)
		fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="%s"/>`, x, float64(top)+plotH-bh, bw, bh, palette[0])
	}
	kx := float64(left) + plotW*float64(f.KilledAt)/float64(time.Duration(n)*f.Bucket)
	fmt.Fprintf(&b, `<line x1="%.1f" y1="%d" x2="%.1f" y2="%.1f" stroke="%s" stroke-width="2" stroke-dasharray="4 3"/>`, kx, top, kx, float64(top)+plotH, palette[3])
	fmt.Fprintf(&b, `<text x="%.1f" y="%d" fill="%s">leader killed</text>`, kx+6, top+14, palette[3])
	fmt.Fprintf(&b, `<text x="%.1f" y="%d" fill="#333">no writes for %s, new leader after %s</text>`, kx+6, top+30, f.Unavailable.Round(time.Millisecond), (f.NewLeaderAt - f.KilledAt).Round(time.Millisecond))
	for s := 0; s <= int(time.Duration(n)*f.Bucket/time.Second); s++ {
		x := float64(left) + plotW*float64(s)/float64(time.Duration(n)*f.Bucket/time.Second)
		fmt.Fprintf(&b, `<text x="%.1f" y="%d" text-anchor="middle" fill="#666">%ds</text>`, x, h-bottom+16, s)
	}
	b.WriteString(`</svg>`)
	return b.String()
}

func fmtNum(v float64) string {
	switch {
	case v >= 1000000:
		return fmt.Sprintf("%.1fM", v/1e6)
	case v >= 1000:
		return fmt.Sprintf("%.1fk", v/1e3)
	case v >= 100:
		return fmt.Sprintf("%.0f", v)
	case v >= 10:
		return fmt.Sprintf("%.1f", v)
	}
	return fmt.Sprintf("%.2f", v)
}
