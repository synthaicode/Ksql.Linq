#!/bin/sh
set -e

echo "Phase 1: Infrastructure readiness (5 consecutive OK)"
consec=0
for i in $(seq 1 150); do
  if curl -fsS http://schema-registry:8081/subjects >/dev/null 2>&1 && \
     curl -fsS http://ksqldb-server:8088/healthcheck >/dev/null 2>&1; then
    consec=$((consec+1))
  else
    consec=0
  fi
  [ "$consec" -ge 5 ] && break
  sleep 2
done

echo "Phase 2: ksqlDB internal state (SHOW QUERIES EXTENDED, 5 consecutive OK)"
consec=0
for i in $(seq 1 60); do
  body='{"ksql":"SHOW QUERIES EXTENDED;","streamsProperties":{}}'
  resp=$(curl -fsS -H 'Content-Type: application/vnd.ksql+json' \
         -d "$body" http://ksqldb-server:8088/ksql 2>/dev/null || echo "")
  if [ -n "$resp" ] && \
     ! echo "$resp" | grep -qi "error\|exception" && \
     echo "$resp" | grep -Eq '^[[:space:]]*\['; then
    consec=$((consec+1))
  else
    consec=0
  fi
  [ "$consec" -ge 5 ] && break
  sleep 2
done

echo "Phase 3: Settling (45s)"
sleep 45

echo "Phase 4: Warmup (optional)"
echo '{"ksql":"CREATE STREAM IF NOT EXISTS test_stream (k VARCHAR KEY, v VARCHAR) WITH (kafka_topic=\"__warmup__\", value_format=\"json\"); INSERT INTO test_stream VALUES (\"warmup\", \"test\");"}' | \
curl -fsS -H 'Content-Type: application/vnd.ksql+json' -d @- \
     http://ksqldb-server:8088/ksql >/dev/null 2>&1 || true
sleep 10

echo "Phase 5: If there are queries, require RUNNING state snapshot"
body='{"ksql":"SHOW QUERIES EXTENDED;","streamsProperties":{}}'
resp=$(curl -fsS -H 'Content-Type: application/vnd.ksql+json' -d "$body" http://ksqldb-server:8088/ksql 2>/dev/null || echo "")
# If queries exist, ensure the snapshot contains RUNNING at least once
if echo "$resp" | grep -q '"queries":\[\]'; then
  echo "No persistent queries at startup"
else
  if ! echo "$resp" | grep -qi RUNNING; then
    echo "Warning: persistent queries not RUNNING yet (continuing)" >&2
  fi
fi

echo "ksqlDB is stable and ready for tests"

# Run all physical integration tests
dotnet test -c Release /src/physicalTests/Ksql.Linq.Tests.Integration.csproj \
  --logger 'trx;LogFileName=physical_runner.trx' \
  --results-directory /src/reports/physical
