#!/bin/bash
# Reliability run: same real cell as e2e/cell.sh, but breaks one component at a time under live traffic
# and checks the service keeps serving, the platform converges, and nothing leaks.
# Needs scripts/build.sh, docker, sudo, /dev/kvm, TEMPORAL_API_KEY (or TEMPORAL_ADDRESS=localhost:7233 for a dev server) and a minio binary (MINIO_BIN).
set -u
E=$(cd "$(dirname "$0")" && pwd); R=$E/../target/release; C=${CELL_DIR:-/tmp/chaos}; SOAK=${SOAK:-6}
export TEMPORAL_ADDRESS=${TEMPORAL_ADDRESS:-us-west-2.aws.api.temporal.io:7233} TEMPORAL_NAMESPACE=${TEMPORAL_NAMESPACE:-ccloud-dev.nhipi}
export CCLOUD_S3_ENDPOINT=http://127.0.0.1:9100 CCLOUD_BUCKET=ccloud-chaos-$$ AWS_ACCESS_KEY_ID=ccloud AWS_SECRET_ACCESS_KEY=ccloud-dev-secret
B=s3://$CCLOUD_BUCKET
sudo rm -rf $C; mkdir -p $C/bin; cd $C
minio_up() { MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY setsid ${MINIO_BIN:-minio} server /tmp/minio-data --address 127.0.0.1:9100 >> minio.log 2>&1 &
  for i in $(seq 30); do curl -sf -o /dev/null $CCLOUD_S3_ENDPOINT/minio/health/live && return; sleep 0.5; done; echo "minio did not start"; }
minio_down() { kill $(pgrep -x minio); while pgrep -x minio >/dev/null; do sleep 0.2; done; }
mkdir -p /tmp/minio-data
curl -sf -o /dev/null $CCLOUD_S3_ENDPOINT/minio/health/live || minio_up
for a in v1 v2 crash err; do docker build -q -t webapp:$a $E/app-$a >/dev/null; done
(cd $E/../orchestrator && go build -o $C/bin/ ./cmd/worker ./cmd/deploy ./cmd/keygen ./cmd/statectl ./cmd/hostctl)
bin/keygen -key signing.key -pub signing.pub
echo '{"svc-web":{"host":"web.local","port":8000,"health_path":"/health"}}' > services.json
W="-services services.json -key signing.key"
worker() { bin/worker $W >> worker.log 2>&1 & WP=$!; }
worker; sleep 3
PE=--preserve-env=CCLOUD_S3_ENDPOINT,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY
# Register each host's box and sign keys, read on the host itself (not from its status report).
reg() { bin/hostctl add -key signing.key -id $1 -box-pub $(sudo $R/hostd --state-dir $C/$1 box-pub) -sign-pub $(sudo $R/hostd --state-dir $C/$1 sign-pub) >/dev/null; }
host() { sudo $PE $R/hostd --state-dir $C/$1 --slots $2 agent --host-id $1 --memory-mib 1024 --trust signing.pub \
  --doc $B/docs/hosts/$1.json --status $B/status/hosts/$1.json >> $1.log 2>&1 & }
# Two edges behind a health-checking L4 balancer on :9000 (stand-in for a cloud NLB / anycast).
edge() { $R/edge --trust signing.pub --doc $B/docs/edge.json --status $B/status/edges/$1.json --state-dir $C/$1 --listen 127.0.0.1:$2 >> $1.log 2>&1 & }
cat > lb.cfg <<CFG
defaults
  mode tcp
  timeout connect 1s
  timeout client 30s
  timeout server 30s
  retries 3
  option redispatch
frontend web
  bind 0.0.0.0:9000
  default_backend edges
backend edges
  balance roundrobin
  default-server inter 300ms fall 1 rise 2 on-marked-down shutdown-sessions
  server edge-1 127.0.0.1:9001 check
  server edge-2 127.0.0.1:9002 check
CFG
haproxy -f lb.cfg > lb.log 2>&1 & LBP=$!
reg host-a; reg host-b; host host-a 1-100; host host-b 101-200; edge edge-1 9001; edge edge-2 9002; sleep 4
load() { while [ -e $C/loadon ]; do p=$(cat $C/phase); r=$(curl -s -m3 -H 'Host: web.local' -w ' %{http_code}' localhost:9000/cgi-bin/app || echo " 000"); echo "$p $r"; sleep 0.01; done >> $C/codes; }
dep() { bin/deploy $W -service svc-web "$@" 2>&1 | grep -E '"Status"|"Reason"|Fatal|waiting' | tr -s ' ' | cut -c1-200; }
phase() { echo "$1" > phase; echo; echo "== $1"; }
# A host's status report, unwrapped from its signed envelope.
hstatus() { bin/statectl get status/hosts/$1.json 2>/dev/null | python3 -c "import json,sys,base64;d=json.load(sys.stdin);print(base64.b64decode(d['payload']).decode() if 'payload' in d else json.dumps(d))" 2>/dev/null; }
placement() { for h in host-a host-b; do echo "  $h: $(hstatus $h | grep -o '"state":"[a-z]*"' | sort | uniq -c | tr -s ' \n' ' ')"; done; }
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

phase edge-dies   # the balancer fails over to edge-2; deploys go on once edge-1's status goes stale
kill -9 $(pgrep -f "state-dir $C/edge-1"); echo "  edge-1 killed"; sleep 2
dep -image webapp:v2 -replicas 2 -bake 3s; check 2
phase edge-back; edge edge-1 9001; sleep 3; echo "  edge-1 restarted with last-good routes"; check 2

phase hostd-restart   # agent crash (VMs keep running), back before it goes stale
sudo kill -9 $(pgrep -f "hostd --state-dir $C/host-a"); sleep 3; host host-a 1-100; sleep 8; check 2

phase host-dies-mid-deploy   # the batch on host-b is re-placed on host-a and the rollout still goes active
dep -image webapp:v2 -replicas 2 -bake 5s > dep.out & DP=$!
sleep 3; sudo kill -9 $(pgrep -f "hostd --state-dir $C/host-b"); sudo kill -9 $(pgrep -f "firecracker.*$C/host-b") 2>/dev/null; echo "  host-b killed"
wait $DP; cat dep.out; sleep 20; check 2
phase host-b-back; host host-b 101-200; sleep 8; check 2

phase forged-alive   # host-b dies; someone keeps writing fresh unsigned "alive" reports for it. They must not be
                     # believed: the worker repairs its instance onto host-a anyway, and no route keeps pointing at it.
dep -image webapp:v1 -replicas 2 -bake 2s | grep -o '"Status": "[a-z_]*"' | sed 's/^/  spread deploy: /'
echo "  routes before: $(routes) (expect one on host-b)"
last=$(hstatus host-b)
sudo kill -9 $(pgrep -f "hostd --state-dir $C/host-b"); sudo kill -9 $(pgrep -f "firecracker.*$C/host-b") 2>/dev/null; echo "  host-b killed"
touch forging; (while [ -e forging ]; do echo "$last" | python3 -c "import json,sys,time;d=json.load(sys.stdin);d['reported_at']=int(time.time());print(json.dumps(d))" | bin/statectl put status/hosts/host-b.json; sleep 1; done) & FP=$!
r0=$(repairs)
for i in $(seq 60); do [ "$(routes)" = "host-a:active host-a:active" ] && break; sleep 1; done
echo "  routes after ${i}s of forged reports: $(routes) (expect host-a:active host-a:active)"
echo "  repairs started: $(( $(repairs) - r0 )) (expect >= 1)"
rm forging; wait $FP; host host-b 101-200; sleep 8; check 2

phase soak
for i in $(seq $SOAK); do img=v$(( i % 2 + 1 )); echo "  deploy $i ($img): $(dep -image webapp:$img -replicas 2 -bake 2s | grep -o '"Status": "[a-z_]*"')"; done
dep -image webapp:crash -replicas 2 -bake 2s | grep -o '"Status": "[a-z_]*"' | sed 's/^/  crash deploy: /'; check 2

phase canary-bad   # app answers 500: only the ~10% canary share sees it, old instances never drain
dep -image webapp:err -replicas 2 -bake 2s -canary 10 -canary-bake 8s; check 2
phase canary-good; dep -image webapp:v1 -replicas 2 -bake 2s -canary 10 -canary-bake 5s; check 2

phase after; sleep 2; rm -f loadon; wait $LP
echo; echo "== responses by phase (phase code)"; awk '{print $1, $NF}' codes | sort | uniq -c
echo "== draining backends left in the edge doc: $(routes | grep -o draining | wc -l) (expect 0)"
echo "== canary-bad non-200: $(awk '$1=="canary-bad" && $NF!=200' codes | wc -l) of $(grep -c '^canary-bad ' codes) (canary gets ~10%; the edge ejects it after a few failures)"
echo "== non-200 outside canary-bad: $(awk '$1!="canary-bad" && $NF!=200' codes | wc -l) of $(wc -l < codes)"
echo "== shutdown"; sudo kill -INT $(pgrep -x hostd); kill $WP $LBP; pkill -f "state-dir $C/edge-"; sleep 4
echo "procs=$(pgrep -c -x firecracker) taps=$(ip -br link | grep -c '^mr') vmdirs=$(sudo sh -c "ls $C/host-*/vms 2>/dev/null" | grep -vc ':$\|^$')"
