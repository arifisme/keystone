#!/usr/bin/env bash
# Live fault demo against the docker compose cluster. A helper container
# with NET_ADMIN joins a node's network namespace and uses iptables and tc
# there, so the distroless node image needs no tools of its own.
set -euo pipefail

ENDPOINTS=${ENDPOINTS:-127.0.0.1:7101,127.0.0.1:7105,127.0.0.1:7109}
PROJECT=${PROJECT:-$(basename "$(pwd)")}
CTL=${CTL:-./keystonectl}
[ -x "$CTL" ] || go build -o "$CTL" ./cmd/keystonectl

netns_exec() {
  local node=$1; shift
  docker run --rm --network "container:${PROJECT}-${node}-1" --cap-add NET_ADMIN \
    alpine:3.20 sh -c "apk add -q iptables iproute2 >/dev/null 2>&1; $*"
}

writes() {
  local label=$1 secs=$2 end=$((SECONDS + secs))
  while [ $SECONDS -lt $end ]; do
    for k in a b c d e f g h; do
      local t0=$(date +%s%N)
      if "$CTL" -endpoints "$ENDPOINTS" -timeout 3s put "$k" "$label-$SECONDS" >/dev/null 2>&1; then
        printf '%s put %s ok   %4d ms\n' "$label" "$k" $(( ($(date +%s%N) - t0) / 1000000 ))
      else
        printf '%s put %s FAIL %4d ms\n' "$label" "$k" $(( ($(date +%s%N) - t0) / 1000000 ))
      fi
    done
  done
}

echo "== baseline"
writes baseline 5

echo "== partition node4 from every other node for 25 s (group 3: nodes 4,5,6)"
netns_exec node4 "iptables -I INPUT -p tcp --dport 7000 -j DROP; iptables -I OUTPUT -p tcp --dport 7000 -j DROP; sleep 25; iptables -F" &
writes partition 25
wait

echo "== heal, then 200 ms delay and 10% loss on node7 for 20 s (group 4: nodes 7,8,9)"
netns_exec node7 "tc qdisc add dev eth0 root netem delay 200ms loss 10%; sleep 20; tc qdisc del dev eth0 root" &
writes netem 20
wait

echo "== recovered"
writes after 5
"$CTL" -endpoints "$ENDPOINTS" scan
echo "leader changes per group:"
for p in 9101 9104 9107; do
  curl -s "http://127.0.0.1:$p/metrics" | grep '^keystone_raft_leader_changes_total' | sed "s/^/  :$p /"
done
