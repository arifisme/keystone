# Bugs found by the simulator

Every entry is a real defect the deterministic simulation caught. Replay
any of them with the seed:

```
go test ./sim -run 'TestSim$' -seed=<seed> -chaos=<level> -v
```

## 1. Election starvation after a rejected vote request

- **Seed:** 52, chaos level 2, three nodes.
- **Symptom:** after the fault phase the cluster had a majority connected
  for two full seconds and never elected a leader. Node 3 held the longest
  log; nodes 1 and 2 kept starting elections it could not lose but never
  got to stand in.
- **Root cause:** `becomeFollower` restarted the election timer on every
  term increase, including a vote request the node went on to reject
  because the candidate's log was stale. Node 1, whose log was shortest,
  happened to time out slightly faster than node 2 each round; each of
  its doomed campaigns raised the term and reset node 2's countdown, so
  node 2 rarely campaigned, and when it did the response was lost. The
  paper restarts the timer only on contact from the current leader or on
  granting a vote; a rejected request should leave it running.
- **Fix:** 27654a7. The timer restarts only when a leader is known or the
  node is stepping down from candidate or leader.
