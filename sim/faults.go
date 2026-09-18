package sim

import (
	"time"

	"github.com/arifisme/keystone/raft"
)

// faultParams are drawn once per run from the seed within the bounds of
// the chaos level.
type faultParams struct {
	clockJitter float64
	tornTail    float64
	partitions  bool
	crashes     bool
	leaderKills bool
}

func (s *Sim) drawFaults() {
	s.net.minDelay = time.Millisecond
	s.net.maxDelay = 10 * time.Millisecond
	s.faults.clockJitter = 0.05
	if s.cfg.Chaos >= 1 {
		s.net.drop = s.rng.Float64() * 0.15
		s.net.dup = s.rng.Float64() * 0.1
		s.net.maxDelay = 5*time.Millisecond + time.Duration(s.rng.Int63n(int64(45*time.Millisecond)))
		s.faults.partitions = true
		s.faults.clockJitter = 0.1
	}
	if s.cfg.Chaos >= 2 {
		s.faults.crashes = true
	}
	if s.cfg.Chaos >= 3 {
		s.faults.leaderKills = true
		s.faults.tornTail = 0.002
		s.faults.clockJitter = 0.3
	}
}

func (s *Sim) chaos() {
	if s.now >= s.cfg.Faults {
		return
	}
	r := s.rng.Float64()
	switch {
	case r < 0.35 && s.faults.partitions:
		s.partition()
	case r < 0.65 && s.faults.crashes:
		if n := s.randomUp(); n != nil {
			s.crash(n)
			s.schedule(s.restartDelay(), &event{kind: evRestart, node: n.id})
		}
	case r < 0.8 && s.faults.leaderKills:
		if n := s.leader(); n != nil {
			s.stats.LeaderKills++
			s.crash(n)
			s.schedule(s.restartDelay(), &event{kind: evRestart, node: n.id})
		}
	}
	s.schedule(100*time.Millisecond+time.Duration(s.rng.Int63n(int64(500*time.Millisecond))), &event{kind: evChaos})
}

func (s *Sim) restartDelay() time.Duration {
	return 50*time.Millisecond + time.Duration(s.rng.Int63n(int64(time.Second)))
}

func (s *Sim) randomUp() *node {
	var up []*node
	for _, n := range s.nodes {
		if n.up {
			up = append(up, n)
		}
	}
	if len(up) == 0 {
		return nil
	}
	return up[s.rng.Intn(len(up))]
}

func (s *Sim) leader() *node {
	for _, n := range s.nodes {
		if n.up && n.rn.Status().State == raft.Leader {
			return n
		}
	}
	return nil
}

// partition splits the nodes into two sides for a while. Symmetric cuts
// block both directions; asymmetric ones block only messages from side A
// to side B, which is what a half-broken link looks like.
func (s *Sim) partition() {
	s.stats.Partitions++
	n := len(s.nodes)
	k := 1 + s.rng.Intn(n-1)
	perm := s.rng.Perm(n)
	sideA := map[raft.NodeID]bool{}
	for _, i := range perm[:k] {
		sideA[s.ids[i]] = true
	}
	symmetric := s.rng.Float64() < 0.6
	var pairs [][2]raft.NodeID
	for _, a := range s.ids {
		for _, b := range s.ids {
			if a == b || sideA[a] == sideA[b] {
				continue
			}
			if sideA[a] || symmetric {
				pairs = append(pairs, [2]raft.NodeID{a, b})
			}
		}
	}
	s.net.cut(pairs)
	dur := 100*time.Millisecond + time.Duration(s.rng.Int63n(int64(1500*time.Millisecond)))
	s.schedule(dur, &event{kind: evHeal, pairs: pairs})
}
