#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "$0")/.." && pwd)
workspace_dir=$(cd "$repo_dir/.." && pwd)
network=${CODEX11_NETWORK:-codex11}
image=${LOAD_RUST_IMAGE:-rust:1.92-bookworm}
server_target=${SERVER_TARGET_VOLUME:-codex9-server-target}
load_target=${LOAD_TARGET_VOLUME:-codex9-load-target}
stamp=$(date +%Y%m%d-%H%M%S)
evidence_dir="$repo_dir/evidence"
gate_log="$evidence_dir/codex11-$stamp.log"
duplicate_log="$evidence_dir/codex11-duplicate-$stamp.log"

mkdir -p "$evidence_dir"

containers=(
  codex11-gate codex11-duplicate codex11-node-a codex11-node-b
  codex11-pg codex11-redis
)

cleanup_runtime() {
  docker rm -f codex11-gate codex11-duplicate >/dev/null 2>&1 || true
}
trap cleanup_runtime EXIT INT TERM

for container in "${containers[@]}"; do
  docker rm -f "$container" >/dev/null 2>&1 || true
done
docker network inspect "$network" >/dev/null 2>&1 || docker network create "$network" >/dev/null

docker run -d --name codex11-pg --network "$network" \
  -e POSTGRES_USER=privchat \
  -e POSTGRES_PASSWORD=privchat \
  -e POSTGRES_DB=privchat \
  postgres:16-bookworm >/dev/null
docker run -d --name codex11-redis --network "$network" \
  redis:7-bookworm redis-server --save '' --appendonly no >/dev/null

for _ in $(seq 1 60); do
  docker exec codex11-pg pg_isready -U privchat -d privchat >/dev/null 2>&1 && break
  sleep 1
done
docker exec codex11-pg pg_isready -U privchat -d privchat >/dev/null

for migration in "$workspace_dir"/privchat-server/migrations/*.sql; do
  docker exec -i codex11-pg psql -v ON_ERROR_STOP=1 -U privchat -d privchat \
    < "$migration" >/dev/null
done

start_node() {
  local name=$1
  local node_id=$2
  local machine_id=$3
  local data_volume="${name}-data"
  local logs_volume="${name}-logs"
  docker volume create "$data_volume" >/dev/null
  docker volume create "$logs_volume" >/dev/null
  docker run -d --name "$name" --network "$network" --memory 4g \
    -e DATABASE_URL=postgres://privchat:privchat@codex11-pg:5432/privchat \
    -e REDIS_URL=redis://codex11-redis:6379 \
    -e PRIVCHAT_CLUSTER_MODE=multi \
    -e PRIVCHAT_NODE_ID="$node_id" \
    -e SNOWFLAKE_MACHINE_ID="$machine_id" \
    -e PRIVCHAT_JWT_SECRET=your_jwt_secret_here \
    -e SERVICE_MASTER_KEY=your_service_master_key_here \
    -e PUSH_ENABLED=false \
    -v "$workspace_dir:/workspace:ro" \
    -v "$server_target:/workspace/privchat-server/target" \
    -v "$data_volume:/workspace/privchat-server/data" \
    -v "$logs_volume:/workspace/privchat-server/logs" \
    -w /workspace/privchat-server "$image" \
    /workspace/privchat-server/target/release/privchat \
      --host 0.0.0.0 --tcp-port 9001 --ws-port 9002 --quic-port 9001 \
      --enable-metrics --metrics-port 9083 --log-level info >/dev/null
}

start_node codex11-node-a node-a 31
start_node codex11-node-b node-b 32

for node in codex11-node-a codex11-node-b; do
  for _ in $(seq 1 90); do
    if docker exec "$node" bash -lc \
      'curl -fsS http://127.0.0.1:9083/metrics >/dev/null' 2>/dev/null; then
      break
    fi
    if [[ $(docker inspect -f '{{.State.Running}}' "$node") != true ]]; then
      docker logs "$node" >&2
      exit 1
    fi
    sleep 1
  done
  docker exec "$node" bash -lc \
    'curl -fsS http://127.0.0.1:9083/metrics >/dev/null'
done

# The load process uses node-a's network and PID namespaces. Thus 127.0.0.1 is
# the primary endpoint and LOAD_TEST_SERVER_PID=1 samples the actual server,
# while node-b remains independently reachable through Docker DNS.
set +e
docker run --name codex11-gate --rm \
  --network container:codex11-node-a \
  --pid container:codex11-node-a \
  --memory 4g \
  -v "$workspace_dir:/workspace:ro" \
  -v "$load_target:/workspace/privchat-load/target" \
  -v codex11-load-data:/workspace/privchat-load/data \
  -w /workspace/privchat-load \
  -e LOAD_TEST_USER_PREFIX="codex11_${stamp}" \
  -e LOAD_TEST_SERVER_PID=1 \
  -e LOAD_TEST_METRICS_URL=http://127.0.0.1:9083/metrics \
  -e LOAD_TEST_SOAK_PROFILE=cross_node \
  -e LOAD_TEST_SECONDARY_SERVER_HOST=codex11-node-b \
  -e LOAD_TEST_SECONDARY_SERVER_PORT=9001 \
  -e RUST_LOG=privchat_load=info \
  "$image" \
  /workspace/privchat-load/target/release/privchat-load \
    --config /workspace/privchat-load/cross_node_gate.conf \
  2>&1 | tee "$gate_log"
gate_status=${PIPESTATUS[0]}
set -e
if [[ $gate_status -ne 0 ]]; then
  exit "$gate_status"
fi

# A second process claiming node-a must fail closed while the original lease is
# alive. Successful startup here would permit split ownership and duplicate
# realtime delivery.
set +e
docker run --name codex11-duplicate --network "$network" \
  -e DATABASE_URL=postgres://privchat:privchat@codex11-pg:5432/privchat \
  -e REDIS_URL=redis://codex11-redis:6379 \
  -e PRIVCHAT_CLUSTER_MODE=multi \
  -e PRIVCHAT_NODE_ID=node-a \
  -e SNOWFLAKE_MACHINE_ID=33 \
  -e PRIVCHAT_JWT_SECRET=your_jwt_secret_here \
  -e SERVICE_MASTER_KEY=your_service_master_key_here \
  -e PUSH_ENABLED=false \
  -v "$workspace_dir:/workspace:ro" \
  -v "$server_target:/workspace/privchat-server/target" \
  -v codex11-dup-data:/workspace/privchat-server/data \
  -v codex11-dup-logs:/workspace/privchat-server/logs \
  -w /workspace/privchat-server "$image" \
  /workspace/privchat-server/target/release/privchat \
    --host 0.0.0.0 --tcp-port 9001 --ws-port 9002 --quic-port 9001 \
    --log-level warn >"$duplicate_log" 2>&1
duplicate_status=$?
set -e
if [[ $duplicate_status -eq 0 ]]; then
  echo "duplicate node_id unexpectedly started" >&2
  exit 1
fi
if ! grep -q 'already leased' "$duplicate_log"; then
  cat "$duplicate_log" >&2
  echo "duplicate node_id failed for an unexpected reason" >&2
  exit 1
fi

echo "CODEX-11 PASS: directional cross-node delivery and duplicate node_id fail-closed"
echo "evidence: $gate_log"
echo "evidence: $duplicate_log"
