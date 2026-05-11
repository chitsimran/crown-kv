#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
RING_HOSTS_FILE="$SCRIPT_DIR/prod_hosts.csv"
CLIENT_HOSTS_FILE="$SCRIPT_DIR/client_hosts.csv"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env not found at $ENV_FILE"
    echo "Copy setup/.env.example to setup/.env and fill it in."
    exit 1
fi
if [[ ! -f "$RING_HOSTS_FILE" ]]; then
    echo "Error: ring hosts file not found at $RING_HOSTS_FILE"
    exit 1
fi

# shellcheck source=/dev/null
set -a
source "$ENV_FILE"
set +a

SSH_USER="${SSH_USER:?Missing SSH_USER in .env}"
REPO_URL="${REPO_URL:?Missing REPO_URL in .env}"
REPO_BRANCH="${REPO_BRANCH:-main}"
REMOTE_BASE_DIR="${REMOTE_BASE_DIR:-/home/$SSH_USER}"
REPO_NAME="${REPO_NAME:-$(basename "${REPO_URL%.git}")}"
PROJECT_SUBDIR="${PROJECT_SUBDIR:-.}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
PROTOCOL_MODE="${PROTOCOL_MODE:-CROWN}"
METADATA_PORT="${METADATA_PORT:-50050}"
METADATA_HOST="${METADATA_HOST:?Missing METADATA_HOST in .env}"
NODE_BIND_HOST="${NODE_BIND_HOST:-0.0.0.0}"
NODE_PORT="${NODE_PORT:-50051}"
CLIENT_ACK_PORT="${CLIENT_ACK_PORT:-6000}"
TMUX_SOCKET="${TMUX_SOCKET:-/tmp/crown-shared/tmux.sock}"
RUN_SCOPE="${RUN_SCOPE:-shared}"
SKIP_BUILD="${SKIP_BUILD:-0}"
CROWN_CHAIN_READS="${CROWN_CHAIN_READS:-0}"

METADATA_ADDR="${METADATA_HOST}:${METADATA_PORT}"

SSH_OPTS=(-o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new)
if [[ -n "${SSH_KEY_LOCAL:-}" && -f "${SSH_KEY_LOCAL:-/dev/null}" ]]; then
    SSH_OPTS=(-i "$SSH_KEY_LOCAL" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new)
fi

read_hosts_file() {
    local file="$1"
    tr ',\n' '\n\n' < "$file" |
        sed 's/#.*$//' |
        sed 's/^[[:space:]]*//;s/[[:space:]]*$//' |
        awk 'NF > 0'
}

mapfile -t DATA_HOSTS < <(read_hosts_file "$RING_HOSTS_FILE")
if [[ "${#DATA_HOSTS[@]}" -eq 0 ]]; then
    echo "Error: no ring hosts found in $RING_HOSTS_FILE"
    exit 1
fi

CLIENT_HOSTS=()
if [[ -f "$CLIENT_HOSTS_FILE" ]]; then
    mapfile -t CLIENT_HOSTS < <(read_hosts_file "$CLIENT_HOSTS_FILE")
fi

build_members_arg() {
    local members=()
    local host
    for host in "${DATA_HOSTS[@]}"; do
        members+=("${host}@${host}:${NODE_PORT}")
    done
    local IFS=,
    echo "${members[*]}"
}

MEMBERS_ARG="$(build_members_arg)"

# Collect all unique hosts that need setup/build across all roles.
declare -A _seen_hosts
ALL_BUILD_HOSTS=()
for _h in "$METADATA_HOST" "${DATA_HOSTS[@]}" "${CLIENT_HOSTS[@]}"; do
    if [[ -z "${_seen_hosts[$_h]+x}" ]]; then
        _seen_hosts[$_h]=1
        ALL_BUILD_HOSTS+=("$_h")
    fi
done

usage() {
    local client_hosts_display="${CLIENT_HOSTS[*]:-none}"
    cat <<EOF
Usage: $0 <setup|start|rerun|kill|repl-info> [--skip-build]

Options:
  --skip-build, -n   Skip git pull and build on the VMs; reuse the existing binary.
                     Equivalent to setting SKIP_BUILD=1 in the environment.

Ring hosts file:   $RING_HOSTS_FILE
Client hosts file: $CLIENT_HOSTS_FILE (optional)
Metadata host:     $METADATA_HOST
Metadata addr:     $METADATA_ADDR
Ring nodes:        ${DATA_HOSTS[*]}
Client nodes:      $client_hosts_display
Members:           $MEMBERS_ARG
EOF
}

remote_env_args() {
    printf '%q ' \
        "SSH_USER=$SSH_USER" \
        "REPO_URL=$REPO_URL" \
        "REPO_BRANCH=$REPO_BRANCH" \
        "REMOTE_BASE_DIR=$REMOTE_BASE_DIR" \
        "REPO_NAME=$REPO_NAME" \
        "PROJECT_SUBDIR=$PROJECT_SUBDIR" \
        "BUILD_TYPE=$BUILD_TYPE" \
        "PROTOCOL_MODE=$PROTOCOL_MODE" \
        "METADATA_PORT=$METADATA_PORT" \
        "NODE_BIND_HOST=$NODE_BIND_HOST" \
        "NODE_PORT=$NODE_PORT" \
        "METADATA_ADDR=$METADATA_ADDR" \
        "MEMBERS_ARG=$MEMBERS_ARG" \
        "TMUX_SOCKET=$TMUX_SOCKET" \
        "RUN_SCOPE=$RUN_SCOPE" \
        "SKIP_BUILD=$SKIP_BUILD" \
        "CROWN_CHAIN_READS=$CROWN_CHAIN_READS"
}

copy_and_run() {
    local host="$1"
    local local_script="$2"
    shift 2
    local remote_script="/home/${SSH_USER}/$(basename "$local_script")"
    local server="${SSH_USER}@${host}"

    echo "==> $server"
    echo " -> copying $(basename "$local_script")"
    scp "${SSH_OPTS[@]}" "$local_script" "$server:$remote_script"
    echo " -> running $remote_script"
    ssh -t "${SSH_OPTS[@]}" "$server" "$(remote_env_args) $* bash '$remote_script'"
}

action="${1:-}"
shift || true
for arg in "$@"; do
    case "$arg" in
        --skip-build|-n) SKIP_BUILD=1 ;;
        *)
            echo "Unknown option: $arg"
            usage
            exit 1
            ;;
    esac
done
export SKIP_BUILD

case "$action" in
    setup)
        # Install system packages on every host that will run any part of the system.
        for host in "${ALL_BUILD_HOSTS[@]}"; do
            copy_and_run "$host" "$SCRIPT_DIR/setup.bash"
        done
        ;;
    start|build|deploy)
        # 1. Metadata store (also builds all binaries on that host).
        copy_and_run "$METADATA_HOST" "$SCRIPT_DIR/start_metadata.bash"
        sleep 1
        # 2. Ring server nodes.
        for host in "${DATA_HOSTS[@]}"; do
            copy_and_run "$host" "$SCRIPT_DIR/start_server.bash" "NODE_ID=$(printf '%q' "$host")"
        done
        # 3. Client-only nodes: clone + build, no process started.
        #    Skip hosts that are also the metadata host (already built above).
        for host in "${CLIENT_HOSTS[@]}"; do
            if [[ "$host" == "$METADATA_HOST" ]]; then
                echo "==> $host (client role: already built as metadata host, skipping duplicate build)"
                continue
            fi
            copy_and_run "$host" "$SCRIPT_DIR/build_only.bash"
        done
        ;;
    rerun)
        "$0" kill
        "$0" start
        ;;
    kill)
        # Kill metadata and ring nodes. Client nodes have no persistent process.
        declare -A _kill_seen
        for host in "$METADATA_HOST" "${DATA_HOSTS[@]}"; do
            if [[ -z "${_kill_seen[$host]+x}" ]]; then
                _kill_seen[$host]=1
                copy_and_run "$host" "$SCRIPT_DIR/kill.bash"
            fi
        done
        ;;
    repl-info)
        local_client_list=""
        for host in "${CLIENT_HOSTS[@]}"; do
            local_client_list+="  build/client --metadata $METADATA_ADDR --listen ${host}:${CLIENT_ACK_PORT} --repl"$'\n'
        done
        if [[ -z "$local_client_list" ]]; then
            local_client_list="  (no client_hosts.csv — replace <host> with a reachable hostname)"$'\n'
            local_client_list+="  build/client --metadata $METADATA_ADDR --listen <host>:${CLIENT_ACK_PORT} --repl"$'\n'
        fi
        cat <<EOF
Metadata address:
  $METADATA_ADDR

Ring nodes:
  ${DATA_HOSTS[*]}

Client commands (SSH into each host, then run):
$local_client_list
Current members:
  $MEMBERS_ARG
EOF
        ;;
    ""|-h|--help|help)
        usage
        ;;
    *)
        echo "Invalid action: $action"
        usage
        exit 1
        ;;
esac
