#!/bin/bash
# Reliability run: same real cell as e2e/cell.sh, but breaks one component at a time under live traffic
# and checks the service keeps serving, the platform converges, and nothing leaks.
# Needs scripts/build.sh, docker, sudo, /dev/kvm, TEMPORAL_API_KEY and a minio binary (MINIO_BIN).
set -u
E=$(cd "$(dirname "$0")" && pwd); R=$E/../target/release; C=${CELL_DIR:-/tmp/chaos}; SOAK=${SOAK:-6}
export TEMPORAL_ADDRESS=us-west-2.aws.api.temporal.io:7233 TEMPORAL_NAMESPACE=ccloud-dev.nhipi
export CCLOUD_S3_ENDPOINT=http://127.0.0.1:9100 CCLOUD_BUCKET=ccloud-chaos-$$ AWS_ACCESS_KEY_ID=ccloud AWS_SECRET_ACCESS_KEY=ccloud-dev-secret
B=s3://$CCLOUD_BUCKET
sudo rm -rf $C; mkdir -p $C/bin; cd $C
minio_up() { MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY setsid ${MINIO_BIN:-minio} server /tmp/minio-data --address 127.0.0.1:9100 >> minio.log 2>&1 &
  for i in $(seq 30); do curl -sf -o /dev/null $CCLOUD_S3_ENDPOINT/minio/health/live && return; sleep 0.5; done; echo "minio did not start"; }
minio_down() { kill $(pgrep -x minio); while pgrep -x minio >/dev/null; do sleep 0.2; done; }
mkdir -p /tmp/minio-data
curl -sf -o /dev/null $CCLOUD_S3_ENDPOINT/minio/health/live || minio_up
for a in v1 v2 crash; do docker build -q -t webapp:$a $E/app-$a >/dev/null; done
(cd $E/../orchestrator && go build -o $C/bin/ ./cmd/worker ./cmd/deploy ./cmd/keygen ./cmd/statectl)
bin/keygen -key signing.key -pub signing.pub
echo '{"svc-web":{"host":"web.local","port":8000,"health_path":"/health"}}' > services.json
W="-services services.json -key signing.key"
worker() { bin/worker $W >> worker.log 2>&1 & WP=$!; }
worker; sleep 3
PE=--preserve-env=CCLOUD_S3_ENDPOINT,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY
host() { sudo $PE $R/hostd --state-dir $C/$1 --slots $2 agent --host-id $1 --memory-mib 1024 --trust signing.pub \
  --doc $B/docs/hosts/$1.json --status $B/status/hosts/$1.json >> $1.log 2>&1 & }
edge() { $R/edge --trust signing.pub --doc $B/docs/edge.json --status $B/status/edges/edge-1.json --state-dir $C/edge --listen 0.0.0.0:9000 >> edge.log 2>&1 & EP=$!; }
host host-a 1-100; host host-b 101-200; edge; sleep 4
load() { while [ -e $C/loadon ]; do p=$(cat $C/phase); r=$(curl -s -m3 -H 'Host: web.local' -w ' %{http_code}' localhost:9000/cgi-bin/app || echo " 000"); echo "$p $r"; sleep 0.01; done >> $C/codes; }
dep() { bin/deploy $W -service svc-web "$@" 2>&1 | grep -E '"Status"|"Reason"|Fatal|waiting' | tr -s ' ' | cut -c1-200; }
phase() { echo "$1" > phase; echo; echo "== $1"; }
placement() { for h in host-a host-b; do echo "  $h: $(bin/statectl get status/hosts/$h.json 2>/dev/null | grep -o '"state":"[a-z]*"' | sort | uniq -c | tr -s ' \n' ' ')"; done; }
routes() { bin/statectl get docs/edge.json | python3 -c "import json,sys,base64;d=json.loads(base64.b64decode(json.load(sys.stdin)['payload']));print(' '.join(sorted(b['host']+':'+b['state'] for r in d['routes'] for b in r['backends'])))"; }
repairs() { grep -c "repair started" worker.log; }
fc() { sleep 1; pgrep -c -x firecracker || true; }
check() { echo "  routes: $(routes)"; placement; echo "  firecracker procs: $(fc) (expect $1)"; }

phase setup; dep -image webapp:v1 -replicas 2 -bake 3s
touch loadon; load & LP=$!; sleep 2

phase worker-killed-mid-deploy   # Temporal must resume the rollout on a new worker
dep -image webapp:v2 -replicas 2 -bake 5s > dep.out & DP=$!
sleep 4; kill -9 $WP; echo "  worker killed"; sleep 6; worker; echo "  worker restarted"; wait $DP; cat dep.out; check 2

phase bucket-down   # running traffic must not depend on the state bucket
r0=$(repairs); minio_down; echo "  minio down for 20s"; sleep 20
phase bucket-back; minio_up; echo "  minio back"; sleep 12
echo "  repairs started by the outage: $(( $(repairs) - r0 )) (expect 0)"; check 2
phase deploy-after-bucket; dep -image webapp:v1 -replicas 2 -bake 3s; check 2

phase edge-restart   # single edge: expect a short gap, then last-good routes from its state dir
kill -9 $EP; sleep 1; edge; sleep 3; echo "  edge restarted"; check 2

phase hostd-restart   # agent crash (VMs keep running), back before it goes stale
sudo kill -9 $(pgrep -f "hostd --state-dir $C/host-a"); sleep 3; host host-a 1-100; sleep 8; check 2

phase host-dies-mid-deploy   # rollout must finish or roll back, then repair fills host-b's gap
dep -image webapp:v2 -replicas 2 -bake 5s > dep.out & DP=$!
sleep 3; sudo kill -9 $(pgrep -f "hostd --state-dir $C/host-b"); sudo kill -9 $(pgrep -f "firecracker.*$C/host-b") 2>/dev/null; echo "  host-b killed"
wait $DP; cat dep.out; sleep 20; check 2
phase host-b-back; host host-b 101-200; sleep 8; check 2

phase soak
for i in $(seq $SOAK); do img=v$(( i % 2 + 1 )); echo "  deploy $i ($img): $(dep -image webapp:$img -replicas 2 -bake 2s | grep -o '"Status": "[a-z_]*"')"; done
dep -image webapp:crash -replicas 2 -bake 2s | grep -o '"Status": "[a-z_]*"' | sed 's/^/  crash deploy: /'; check 2

phase after; sleep 2; rm -f loadon; wait $LP
echo; echo "== responses by phase (phase code)"; awk '{print $1, $NF}' codes | sort | uniq -c
echo "== non-200: $(awk '$NF!=200' codes | wc -l) of $(wc -l < codes)"
echo "== shutdown"; sudo kill -INT $(pgrep -x hostd); kill $WP $EP; sleep 4
echo "procs=$(pgrep -c -x firecracker) taps=$(ip -br link | grep -c '^mr') vmdirs=$(sudo sh -c "ls $C/host-*/vms 2>/dev/null" | grep -vc ':$\|^$')"
