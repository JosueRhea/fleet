#!/bin/bash
# One cell, no shortcuts: worker (Temporal Cloud) publishes signed docs to an S3 bucket (MinIO);
# two hostd agents and an edge pull them and report status there. Traffic goes through the edge.
# Needs scripts/build.sh, docker, sudo, /dev/kvm, TEMPORAL_API_KEY (or TEMPORAL_ADDRESS=localhost:7233 for a dev server) and a minio binary (MINIO_BIN).
set -u
E=$(cd "$(dirname "$0")" && pwd); R=$E/../target/release; C=${CELL_DIR:-/tmp/cell2}
export TEMPORAL_ADDRESS=${TEMPORAL_ADDRESS:-us-west-2.aws.api.temporal.io:7233} TEMPORAL_NAMESPACE=${TEMPORAL_NAMESPACE:-ccloud-dev.nhipi}
export CCLOUD_S3_ENDPOINT=http://127.0.0.1:9100 CCLOUD_BUCKET=ccloud-e2e-$$ AWS_ACCESS_KEY_ID=ccloud AWS_SECRET_ACCESS_KEY=ccloud-dev-secret
B=s3://$CCLOUD_BUCKET
sudo rm -rf $C; mkdir -p $C/bin; cd $C
if ! curl -sf -o /dev/null $CCLOUD_S3_ENDPOINT/minio/health/live; then
  mkdir -p /tmp/minio-data
  MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY ${MINIO_BIN:-minio} server /tmp/minio-data --address 127.0.0.1:9100 > minio.log 2>&1 &
  sleep 3
fi
for a in v1 v2 crash; do docker build -q -t webapp:$a $E/app-$a >/dev/null; done
(cd $E/../orchestrator && go build -o $C/bin/ ./cmd/worker ./cmd/deploy ./cmd/keygen ./cmd/statectl ./cmd/hostctl)
bin/keygen -key signing.key -pub signing.pub
echo '{"svc-web":{"host":"web.local","port":8000,"health_path":"/health"}}' > services.json
W="-services services.json -key signing.key"
bin/worker $W > worker.log 2>&1 & WP=$!
sleep 3   # worker creates the bucket
PE=--preserve-env=CCLOUD_S3_ENDPOINT,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY
# Register each host's box and sign keys, read on the host itself (not from its status report).
reg() { bin/hostctl add -key signing.key -id $1 -box-pub $(sudo $R/hostd --state-dir $C/$1 box-pub) -sign-pub $(sudo $R/hostd --state-dir $C/$1 sign-pub) >/dev/null; }
host() { sudo $PE $R/hostd --state-dir $C/$1 --slots $2 agent --host-id $1 --memory-mib 1024 --trust signing.pub \
  --doc $B/docs/hosts/$1.json --status $B/status/hosts/$1.json >> $1.log 2>&1 & }
reg host-a; reg host-b; host host-a 1-100; host host-b 101-200
$R/edge --trust signing.pub --doc $B/docs/edge.json --status $B/status/edges/edge-1.json --state-dir $C/edge --listen 0.0.0.0:9000 > edge.log 2>&1 & EP=$!
sleep 4
load() { while [ -e $C/loadon ]; do p=$(cat $C/phase); r=$(curl -s -m3 -H 'Host: web.local' -w ' %{http_code}' localhost:9000/cgi-bin/app || echo " 000"); echo "$p $r"; sleep 0.01; done >> $C/codes; }
dep() { echo "$1" > phase; echo "== $1"; bin/deploy $W -service svc-web "${@:2}" 2>&1 | grep -E '"Status"|"Reason"|replacing|Fatal|error' | cut -c1-220; }
# A host's status report, unwrapped from its signed envelope.
hstatus() { bin/statectl get status/hosts/$1.json 2>/dev/null | python3 -c "import json,sys,base64;d=json.load(sys.stdin);print(base64.b64decode(d['payload']).decode() if 'payload' in d else json.dumps(d))" 2>/dev/null; }
placement() { for h in host-a host-b; do echo "$h: $(hstatus $h | grep -o '"state":"[a-z]*"' | sort | uniq -c | tr '\n' ' ')"; done; }
fc() { sleep 1; pgrep -c -x firecracker || true; }

echo setup > phase
dep first -image webapp:v1 -replicas 2 -bake 5s; placement
touch loadon; load & LP=$!
dep v1-to-v2 -image webapp:v2 -replicas 2 -bake 8s; placement; echo "firecracker procs (expect 2): $(fc)"
dep crash -image webapp:crash -replicas 2 -bake 5s; echo "firecracker procs (expect 2): $(fc)"

echo tamper > phase; echo "== tamper: rewrite edge doc payload without re-signing"
bin/statectl get docs/edge.json > edge.orig
python3 -c "
import json,base64,sys
e=json.load(open('edge.orig')); d=json.loads(base64.b64decode(e['payload']))
d['version']+=1; d['routes']=[]
e['payload']=base64.b64encode(json.dumps(d).encode()).decode(); json.dump(e,open('edge.bad','w'))"
bin/statectl put docs/edge.json < edge.bad
sleep 3; grep -c "unusable" edge.log | sed 's/^/edge rejections logged: /'
dep deploy-on-tampered -image webapp:v1 -replicas 2 -bake 5s
bin/statectl put docs/edge.json < edge.orig

echo host-b-dies > phase; sleep 3; grep -c "unusable" edge.log | sed 's/^/edge rejections after restore (expect still 1): /'
echo "== kill host-b (agent + its VMs)"
sudo kill -9 $(pgrep -f "hostd --state-dir $C/host-b"); sudo kill -9 $(pgrep -f "firecracker.*$C/host-b") 2>/dev/null
t0=$(date +%s); echo "== waiting for the worker to repair (no deploy)"
for i in $(seq 60); do
  r=$(bin/statectl get docs/edge.json | python3 -c "import json,sys,base64;d=json.loads(base64.b64decode(json.load(sys.stdin)['payload']));print(' '.join(sorted(b['host'] for r in d['routes'] for b in r['backends'] if b['state']=='active')))")
  [ "$r" = "host-a host-a" ] && break; sleep 1
done
echo "routes: $r after $(( $(date +%s) - t0 ))s"; grep -m1 "repair started" worker.log | cut -c21-; placement
echo after-host-loss > phase
dep deploy-after-host-loss -image webapp:v1 -replicas 2 -bake 5s; placement

echo host-b-back > phase; echo "== restart host-b: restores last-good doc, then applies tombstones"
host host-b 101-200; sleep 8; placement
echo after > phase; sleep 2; rm -f loadon; wait $LP
echo "== responses by phase (phase body code)"; sort codes | uniq -c
echo "== shutdown"; sudo kill -INT $(pgrep -x hostd); kill $WP $EP; sleep 4
echo "procs=$(pgrep -c -x firecracker) taps=$(ip -br link | grep -c '^mr') vmdirs=$(sudo sh -c "ls $C/host-*/vms 2>/dev/null" | grep -vc ':$\|^$')"
