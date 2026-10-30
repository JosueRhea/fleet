#!/bin/bash
# Full stack on one host: real hostd agent (Firecracker VMs) + real edge + Temporal Cloud worker.
# Needs scripts/build.sh, docker, sudo, /dev/kvm and TEMPORAL_API_KEY. Prints request outcomes per phase.
set -u
E=$(cd "$(dirname "$0")" && pwd); R=$E/../target/release; C=${CELL_DIR:-/tmp/cell}; mkdir -p $C/bin; cd $C
for a in v1 v2 err crash; do docker build -q -t webapp:$a $E/app-$a >/dev/null; done
(cd $E/../orchestrator && go build -o $C/bin/ ./cmd/worker ./cmd/deploy)
export TEMPORAL_ADDRESS=us-west-2.aws.api.temporal.io:7233 TEMPORAL_NAMESPACE=ccloud-dev.nhipi
sudo rm -rf $C/host.json $C/edge.json $C/*status.json $C/edge $C/hostd-state; rm -f $C/*.log $C/codes
echo '{"svc-web":{"host":"web.local","port":8000,"health_path":"/health"}}' > services.json
sudo $R/hostd --state-dir $C/hostd-state agent --doc $C/host.json --status $C/host-status.json > hostd.log 2>&1 & 
$R/edge --doc $C/edge.json --status $C/edge-status.json --state-dir $C/edge --listen 0.0.0.0:9000 > edge.log 2>&1 & EP=$!
$C/bin/worker -dir $C > worker.log 2>&1 & WP=$!
sleep 3
echo setup > phase
load() { while [ -e $C/loadon ]; do p=$(cat $C/phase); r=$(curl -s -m3 -H 'Host: web.local' -w ' %{http_code}' localhost:9000/cgi-bin/app || echo " 000"); echo "$p $r"; sleep 0.01; done >> $C/codes; }
dep() { echo "$1" > phase; echo "== $1"; $C/bin/deploy -dir $C -service svc-web "${@:2}" 2>&1 | grep -E '"Status"|"Reason"|replacing|failed' ; }
fc() { sleep 1; pgrep -c -x firecracker || true; }

dep first   -image webapp:v1 -replicas 2 -bake 5s
touch loadon; load & LP=$!
dep v1-to-v2 -image webapp:v2 -replicas 2 -max-surge 1 -bake 10s
echo "procs after v2 bake (expect 2): $(fc)"
dep crash   -image webapp:crash -replicas 2 -bake 5s
echo "procs (expect 2): $(fc)"
dep err500  -image webapp:err -replicas 2 -bake 8s
echo "procs (expect 2): $(fc)"
dep manual  -image webapp:v1 -replicas 2 -bake 30s -rollback-after 8s
echo "procs (expect 2): $(fc)"
echo after > phase; sleep 3; rm -f loadon; wait $LP
echo "== responses by phase (phase body code)"; sort codes | uniq -c
echo "== shutdown"; sudo kill -INT $(pgrep -x hostd); kill $WP $EP; sleep 4
echo "procs=$(fc) taps=$(ip -br link | grep -c '^mr') vmdirs=$(sudo ls $C/hostd-state/vms 2>/dev/null | wc -l)"
