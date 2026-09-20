#!/usr/bin/env bash
# Reference numbers from etcd on the same machine, using etcd's own
# benchmark tool so the client side is etcd's native gRPC path.
#
#   ETCD=/path/to/etcd BENCHMARK=/path/to/benchmark ./bench/etcd.sh bench/results/etcd
#
# benchmark comes from the etcd source tree: go build ./tools/benchmark
set -euo pipefail
OUT=${1:-bench/results/etcd}
ETCD=${ETCD:-etcd}
BENCHMARK=${BENCHMARK:-benchmark}
DATA=$(mktemp -d)
mkdir -p "$OUT"
trap 'pkill -P $$ || true; rm -rf "$DATA"' EXIT

start_cluster() {
  local n=$1 cluster="" i
  for i in $(seq 1 "$n"); do
    cluster+="n$i=http://127.0.0.1:$((2380 + i * 10)),"
  done
  cluster=${cluster%,}
  for i in $(seq 1 "$n"); do
    "$ETCD" --name "n$i" --data-dir "$DATA/n$i" \
      --listen-client-urls "http://127.0.0.1:$((2379 + i * 10))" \
      --advertise-client-urls "http://127.0.0.1:$((2379 + i * 10))" \
      --listen-peer-urls "http://127.0.0.1:$((2380 + i * 10))" \
      --initial-advertise-peer-urls "http://127.0.0.1:$((2380 + i * 10))" \
      --initial-cluster "$cluster" --initial-cluster-state new \
      --log-level error > "$DATA/n$i.log" 2>&1 &
  done
  sleep 3
}

endpoints() {
  local n=$1 eps="" i
  for i in $(seq 1 "$n"); do eps+="127.0.0.1:$((2379 + i * 10)),"; done
  echo "${eps%,}"
}

for n in 3 5; do
  start_cluster "$n"
  eps=$(endpoints "$n")
  for val in 100 1024 10240; do
    "$BENCHMARK" --endpoints="$eps" --conns=64 --clients=64 put \
      --key-size=11 --val-size="$val" --key-space-size=100000 --total=20000 \
      > "$OUT/put-n$n-v$val.txt" 2>&1
  done
  "$BENCHMARK" --endpoints="$eps" --conns=64 --clients=64 put \
    --key-size=11 --val-size=1024 --key-space-size=10000 --total=10000 > /dev/null 2>&1
  "$BENCHMARK" --endpoints="$eps" --conns=64 --clients=64 range key \
    --consistency=l --total=100000 > "$OUT/range-linearizable-n$n.txt" 2>&1
  "$BENCHMARK" --endpoints="$eps" --conns=64 --clients=64 range key \
    --consistency=s --total=100000 > "$OUT/range-serializable-n$n.txt" 2>&1
  pkill -P $$ etcd || true
  sleep 1
  rm -rf "$DATA"/n*
done
