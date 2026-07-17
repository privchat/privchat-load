#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <g7|soak-1h|soak-4h> <server-container>" >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
gate=$1
server_container=$2
repo_dir=$(cd "$(dirname "$0")/.." && pwd)
workspace_dir=$(cd "$repo_dir/.." && pwd)
target_volume=${LOAD_TARGET_VOLUME:-codex9-load-target}
image=${LOAD_RUST_IMAGE:-rust:1.92-bookworm}
cpuset=${LOAD_CPUSET:-}
memory=${LOAD_MEMORY:-8g}
stamp=$(date +%s)

case "$gate" in
  g7)
    config=test_b_gate.conf
    profile=
    ;;
  soak-1h)
    config=soak_1h_gate.conf
    profile=gate_1h
    ;;
  soak-4h)
    config=soak_4h_gate.conf
    profile=gate_4h
    ;;
  *) usage ;;
esac

container="privchat-${gate}-${stamp}"
data_volume="${container}-data"
prefix="${gate//-/_}_${stamp}"
docker volume create "$data_volume" >/dev/null
cleanup() {
  docker rm -f "$container" >/dev/null 2>&1 || true
  docker volume rm "$data_volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

limits=(--memory "$memory")
if [[ -n "$cpuset" ]]; then
  limits+=(--cpuset-cpus "$cpuset")
fi
profile_env=()
if [[ -n "$profile" ]]; then
  profile_env=(-e "LOAD_TEST_SOAK_PROFILE=$profile")
fi

# Bash 3.2 treats an empty array expansion as unbound under `set -u`.
# Disable nounset only around the Docker argv expansion; all script inputs above
# remain guarded by strict mode.
set +u
# Sharing the server network and PID namespaces makes 127.0.0.1 in the checked-in
# configs portable and lets the harness sample the actual server process as PID 1.
docker run --name "$container" --rm \
  --network "container:$server_container" \
  --pid "container:$server_container" \
  "${limits[@]}" \
  -v "$workspace_dir:/workspace:ro" \
  -v "$target_volume:/workspace/privchat-load/target" \
  -v "$data_volume:/workspace/privchat-load/data" \
  -w /workspace/privchat-load \
  -e "LOAD_TEST_USER_PREFIX=$prefix" \
  -e LOAD_TEST_SERVER_PID=1 \
  -e LOAD_TEST_METRICS_URL=http://127.0.0.1:9083/metrics \
  -e RUST_LOG=privchat_load=info \
  "${profile_env[@]}" \
  "$image" \
  /workspace/privchat-load/target/release/privchat-load \
  --config "/workspace/privchat-load/$config"
status=$?
set -u
exit "$status"
