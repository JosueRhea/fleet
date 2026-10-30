#!/bin/bash
# Secrets end to end: secretctl seals a set to the cell key; the worker re-seals it to the host that
# runs each instance; hostd opens it right before boot. Checks the app sees it, no plaintext is in the
# state bucket, and rotating (new version = new deploy) has no downtime.
# Needs scripts/build.sh, docker, sudo, /dev/kvm, TEMPORAL_API_KEY (or TEMPORAL_ADDRESS=localhost:7233 for a dev server) and a minio binary (MINIO_BIN).
set -u
E=$(cd "$(dirname "$0")" && pwd); R=$E/../target/release; C=${CELL_DIR:-/tmp/secrets}
export TEMPORAL_ADDRESS=${TEMPORAL_ADDRESS:-us-west-2.aws.api.temporal.io:7233} TEMPORAL_NAMESPACE=${TEMPORAL_NAMESPACE:-ccloud-dev.nhipi}
export CCLOUD_S3_ENDPOINT=http://127.0.0.1:9100 CCLOUD_BUCKET=ccloud-secrets-$$ AWS_ACCESS_KEY_ID=ccloud AWS_SECRET_ACCESS_KEY=ccloud-dev-secret
B=s3://$CCLOUD_BUCKET
sudo rm -rf $C; mkdir -p $C/bin /tmp/minio-data; cd $C
curl -sf -o /dev/null $CCLOUD_S3_ENDPOINT/minio/health/live || { MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY setsid ${MINIO_BIN:-minio} server /tmp/minio-data --address 127.0.0.1:9100 >> minio.log 2>&1 & sleep 3; }
docker build -q -t webapp:secret $E/app-secret >/dev/null
(cd $E/../orchestrator && go build -o $C/bin/ ./cmd/worker ./cmd/deploy ./cmd/keygen ./cmd/statectl ./cmd/secretctl ./cmd/hostctl)
bin/keygen -key signing.key -pub signing.pub; bin/secretctl keygen -key secrets.key -pub secrets.pub
echo '{"svc-web":{"host":"web.local","port":8000,"health_path":"/health"}}' > services.json
W="-services services.json -key signing.key"
bin/worker $W -secrets-key secrets.key >> worker.log 2>&1 & WP=$!
PE=--preserve-env=CCLOUD_S3_ENDPOINT,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY
# A host's status report, unwrapped from its signed envelope.
hstatus() { bin/statectl get status/hosts/$1.json 2>/dev/null | python3 -c "import json,sys,base64;d=json.load(sys.stdin);print(base64.b64decode(d['payload']).decode() if 'payload' in d else json.dumps(d))" 2>/dev/null; }
host() { sudo $PE $R/hostd --state-dir $C/$1 --slots $2 agent --host-id $1 --memory-mib 1024 --trust signing.pub \
  --doc $B/docs/hosts/$1.json --status $B/status/hosts/$1.json >> $1.log 2>&1 & }
for h in host-a:1-100 host-b:101-200; do id=${h%%:*}
  # Register the box and sign keys, read on the host itself (not from its status report).
  bin/hostctl add -key signing.key -id $id -box-pub $(sudo $R/hostd --state-dir $C/$id box-pub) -sign-pub $(sudo $R/hostd --state-dir $C/$id sign-pub) >/dev/null
  host $id ${h#*:}
done
$R/edge --trust signing.pub --doc $B/docs/edge.json --status $B/status/edges/edge-1.json --state-dir $C/edge --listen 0.0.0.0:9000 >> edge.log 2>&1 & EP=$!
sleep 5
dep() { bin/deploy $W -service svc-web -image webapp:secret -replicas 2 -bake 3s "$@" 2>&1 | grep -E '"Status"|"Reason"|secret set|Fatal' | tr -s ' '; }
get() { curl -s -m3 -H 'Host: web.local' localhost:9000/cgi-bin/app; }
# Looks in raw objects and inside signed envelopes' base64 payloads.
leaks() { n=0; for k in $(bin/statectl list ""); do
    bin/statectl get $k | python3 -c "import sys,json,base64;b=sys.stdin.buffer.read();p=b
try: p+=base64.b64decode(json.loads(b)['payload'])
except Exception: pass
sys.exit(0 if sys.argv[1].encode() in p else 1)" "$1" && { echo "    plaintext in $k"; n=$((n+1)); }; done
  echo "  objects in the bucket containing \"$1\": $n of $(bin/statectl list "" | wc -l) (expect 0)"; }

echo "== v1"; bin/secretctl set -pub secrets.pub -service svc-web DB_PASSWORD=hunter2-v1; dep
echo "  app sees: $(get)"; leaks hunter2-v1
echo "  host-a doc sealed_env present: $(bin/statectl get docs/hosts/host-a.json | python3 -c "import json,sys,base64;d=json.loads(base64.b64decode(json.load(sys.stdin)['payload']));print(all(i.get('sealed_env') for i in d['instances'] if i['desired']=='running'))")"

echo "== rotate under load"; touch loadon
(while [ -e loadon ]; do c=$(curl -s -m3 -H 'Host: web.local' -o body -w '%{http_code}' localhost:9000/cgi-bin/app || echo 000); echo "$(tr -d '\n' < body) $c"; sleep 0.01; done > codes) & LP=$!
sleep 1; bin/secretctl set -pub secrets.pub -service svc-web DB_PASSWORD=hunter2-v2; dep
sleep 1; rm loadon; wait $LP
echo "  app sees: $(get)"; leaks hunter2-v2
echo "  responses: $(awk '{print $1, $NF}' codes | sort | uniq -c | tr -s ' \n' ' ')"
echo "  non-200: $(awk '$NF!=200' codes | wc -l) of $(wc -l < codes)"

echo "== pinned old version (rollback to secrets v1)"; dep -secrets 1; echo "  app sees: $(get)"
echo "== missing version (expect rolled_back, v1 keeps serving)"; dep -secrets 9; echo "  app sees: $(get)"

echo "== impostor host: unregistered status with an attacker box key and lots of free memory"
bin/secretctl keygen -key evil.key -pub evil.pub
imp() { while [ -e impon ]; do printf '{"host_id":"evil","box_pub":"%s","reported_at":%d,"memory_mib_total":999999,"memory_mib_used":0,"applied_version":0,"instances":[]}' \
  "$(cat evil.pub)" "$(date +%s)" | bin/statectl put status/hosts/evil.json; sleep 1; done; }
touch impon; imp & IP=$!; sleep 2; dep -secrets 1; rm impon; wait $IP
echo "  doc published for evil: $(bin/statectl get docs/hosts/evil.json >/dev/null 2>&1 && echo yes || echo no) (expect no)"
echo "  app sees: $(get)"

echo "== app prints its password and exits (expect rolled_back, no plaintext anywhere)"
docker build -q -t webapp:secret-crash $E/app-secret-crash >/dev/null
# The console tail of a failed boot goes into the host's status report: capture those while it fails.
# The failed report lives ~1s (until the rollback tombstones the instance): poll each host in its own tight loop.
touch caton; CPS=""; for h in host-a host-b; do (while [ -e caton ]; do bin/statectl get status/hosts/$h.json; echo; done > crash-raw-$h) & CPS="$CPS $!"; done
r=$(bin/deploy $W -service svc-web -image webapp:secret-crash -replicas 2 -bake 3s -secrets 1 2>&1)
rm caton; wait $CPS; cat crash-raw-host-* > crash-raw
# Reports are signed envelopes: decode them after the fact so the capture loop stays fast.
python3 -c "
import json,sys,base64
for l in open('crash-raw'):
    try: d=json.loads(l); print(base64.b64decode(d['payload']).decode() if 'payload' in d else l.strip())
    except Exception: pass" > crash-status
echo "$r" | grep -E '"Status"' | tr -s ' '
echo "  status reports with the redacted console line: $(grep -c 'password \*\*\*' crash-status) (expect > 0)"
echo "  plaintext in status reports: $(grep -c hunter2 crash-status), in deploy result: $(echo "$r" | grep -c hunter2) (expect 0, 0)"
leaks hunter2-v1
echo "  worker log lines containing a secret: $(grep -c hunter2 worker.log)"

echo "== host-b's box key changes (state dir wiped); its instance can't be opened any more and is repaired elsewhere"
sudo kill -9 $(pgrep -f "hostd --state-dir $C/host-b"); sudo rm -f $C/host-b/host-box.key; sleep 1; host host-b 101-200
for i in $(seq 90); do
  r=$(bin/statectl get docs/edge.json | python3 -c "import json,sys,base64;d=json.loads(base64.b64decode(json.load(sys.stdin)['payload']));print(' '.join(sorted(b['host'] for r in d['routes'] for b in r['backends'] if b['state']=='active')))")
  [ "$r" = "host-a host-a" ] && break; sleep 1; done
echo "  routes after ${i}s: $r (expect host-a host-a)"
echo "  host-b reports: $(hstatus host-b | grep -o 'cannot open sealed_env[^"]*' | head -1)"
echo "  repairs started: $(grep -c 'repair started' worker.log)"
echo "  app sees: $(get)"

echo "== host disk"; echo "  cached images containing a secret: $(sudo grep -rl hunter2 $C/host-a/images $C/host-b/images | wc -l) (expect 0)"
echo "== shutdown"; sudo kill -INT $(pgrep -x hostd); kill $WP $EP; sleep 4
echo "procs=$(pgrep -c -x firecracker) taps=$(ip -br link | grep -c '^mr') vmdirs=$(sudo ls $C/host-a/vms $C/host-b/vms 2>/dev/null | grep -vc ':$\|^$')"
echo "  files on host disk containing a secret: $(sudo grep -rl hunter2 $C/host-a $C/host-b | wc -l) (expect 0)"
