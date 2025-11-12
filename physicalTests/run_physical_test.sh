#!/usr/bin/env bash
set -euo pipefail

# Usage: physicalTests/run_physical_test.sh "FullyQualifiedName~SchemaRegistryResetTests"

here="$(cd "$(dirname "$0")" && pwd)"
cd "$here"

echo "[INFO] Restarting docker compose (down -v --remove-orphans)"
docker compose down -v --remove-orphans || true

echo "[INFO] Starting docker compose (up -d)"
docker compose up -d

wait_url() {
  local url="$1"; shift
  local name="$1"; shift
  local max=${1:-120}
  echo "[WAIT] ${name}: $url"
  for i in $(seq 1 "$max"); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "[OK] ${name} ready"
      return 0
    fi
    sleep 2
  done
  echo "[ERR] ${name} not ready in ${max} tries" >&2
  return 1
}

wait_port() {
  local host="$1"; shift
  local port="$1"; shift
  local name="$1"; shift
  local max=${1:-120}
  echo "[WAIT] ${name}: ${host}:${port}"
  for i in $(seq 1 "$max"); do
    if (echo > "/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      echo "[OK] ${name} ready"
      return 0
    fi
    sleep 2
  done
  echo "[ERR] ${name} port not open in ${max} tries" >&2
  return 1
}

# Wait for core services exposed on localhost by docker-compose
wait_port 127.0.0.1 39092 "kafka"
wait_url  "http://127.0.0.1:18081/subjects" "schema-registry"
wait_url  "http://127.0.0.1:18088/healthcheck" "ksqldb-server"

cd "$here/.."
filter_expr=${1:-""}
if [[ -z "$filter_expr" ]]; then
  echo "[ERR] Filter expression is required. Example: \"FullyQualifiedName~SchemaRegistryResetTests\"" >&2
  exit 2
fi

echo "[INFO] Applying relaxed RUNNING wait settings for physical tests"
export KSQL_QUERY_RUNNING_TIMEOUT_SECONDS=${KSQL_QUERY_RUNNING_TIMEOUT_SECONDS:-300}
export KSQL_QUERY_RUNNING_CONSECUTIVE=${KSQL_QUERY_RUNNING_CONSECUTIVE:-1}
export KSQL_QUERY_RUNNING_STABILITY_WINDOW_SECONDS=${KSQL_QUERY_RUNNING_STABILITY_WINDOW_SECONDS:-0}

echo "[INFO] Running physical test with filter: $filter_expr"
dotnet test physicalTests/Ksql.Linq.Tests.Integration.csproj -nologo -v q --filter "$filter_expr"
