package kv

import (
	"context"
	"net/http"
	"net/http/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/arifisme/keystone/raft"
)

// metrics is one node's Prometheus registry. Raft and engine figures are
// read from the groups at scrape time; leader changes are counted by a
// small monitor goroutine, because a scrape can only see the present.
type metrics struct {
	registry *prometheus.Registry
	node     *Node

	requests  *prometheus.HistogramVec
	fsync     prometheus.Histogram
	changes   *prometheus.CounterVec
	term      *prometheus.Desc
	commit    *prometheus.Desc
	applied   *prometheus.Desc
	lag       *prometheus.Desc
	leader    *prometheus.Desc
	flushes   *prometheus.Desc
	compacted *prometheus.Desc
	tables    *prometheus.Desc

	mu      sync.Mutex
	leaders map[uint64]raft.NodeID
	stop    chan struct{}
}

func newMetrics(node *Node) *metrics {
	m := &metrics{
		registry: prometheus.NewRegistry(),
		node:     node,
		requests: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "keystone_request_seconds",
			Help:    "Client request latency by method.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
		}, []string{"method"}),
		fsync: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "keystone_wal_fsync_seconds",
			Help:    "Engine WAL fsync latency.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12),
		}),
		changes:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "keystone_raft_leader_changes_total", Help: "Leader changes observed per group."}, []string{"group"}),
		term:      prometheus.NewDesc("keystone_raft_term", "Current term.", []string{"group"}, nil),
		commit:    prometheus.NewDesc("keystone_raft_commit_index", "Commit index.", []string{"group"}, nil),
		applied:   prometheus.NewDesc("keystone_raft_applied_index", "Applied index.", []string{"group"}, nil),
		lag:       prometheus.NewDesc("keystone_raft_apply_lag", "Committed entries not yet applied.", []string{"group"}, nil),
		leader:    prometheus.NewDesc("keystone_raft_is_leader", "1 if this node leads the group.", []string{"group"}, nil),
		flushes:   prometheus.NewDesc("keystone_storage_flushes_total", "Memtable flushes.", []string{"group"}, nil),
		compacted: prometheus.NewDesc("keystone_storage_compactions_total", "Compactions run.", []string{"group"}, nil),
		tables:    prometheus.NewDesc("keystone_storage_tables", "Live SSTables.", []string{"group"}, nil),
		leaders:   map[uint64]raft.NodeID{},
		stop:      make(chan struct{}),
	}
	m.registry.MustRegister(m.requests, m.fsync, m.changes, m)
	return m
}

func (m *metrics) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{m.term, m.commit, m.applied, m.lag, m.leader, m.flushes, m.compacted, m.tables} {
		ch <- d
	}
}

func (m *metrics) Collect(ch chan<- prometheus.Metric) {
	for _, id := range m.node.Groups() {
		g := m.node.groups[id]
		label := strconv.FormatUint(id, 10)
		st := g.raft.Status()
		es := g.db.Stats()
		isLeader := 0.0
		if st.State == raft.Leader {
			isLeader = 1
		}
		ch <- prometheus.MustNewConstMetric(m.term, prometheus.GaugeValue, float64(st.Term), label)
		ch <- prometheus.MustNewConstMetric(m.commit, prometheus.GaugeValue, float64(st.Commit), label)
		ch <- prometheus.MustNewConstMetric(m.applied, prometheus.GaugeValue, float64(st.Applied), label)
		ch <- prometheus.MustNewConstMetric(m.lag, prometheus.GaugeValue, float64(st.Commit-st.Applied), label)
		ch <- prometheus.MustNewConstMetric(m.leader, prometheus.GaugeValue, isLeader, label)
		ch <- prometheus.MustNewConstMetric(m.flushes, prometheus.CounterValue, float64(es.Flushes), label)
		ch <- prometheus.MustNewConstMetric(m.compacted, prometheus.CounterValue, float64(es.Compactions), label)
		ch <- prometheus.MustNewConstMetric(m.tables, prometheus.GaugeValue, float64(es.Tables), label)
	}
}

func (m *metrics) monitor() {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
		}
		for id, g := range m.node.groups {
			lead := g.raft.Status().Leader
			m.mu.Lock()
			if prev, seen := m.leaders[id]; seen && prev != lead && lead != raft.None {
				m.changes.WithLabelValues(strconv.FormatUint(id, 10)).Inc()
			}
			if lead != raft.None || !m.leadersSeen(id) {
				m.leaders[id] = lead
			}
			m.mu.Unlock()
		}
	}
}

func (m *metrics) leadersSeen(id uint64) bool {
	_, ok := m.leaders[id]
	return ok
}

func (m *metrics) interceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		m.requests.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		return resp, err
	}
}

// handler serves /metrics and /debug/pprof.
func (m *metrics) handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
