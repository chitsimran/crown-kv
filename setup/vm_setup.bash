#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
HOSTS_FILE="$SCRIPT_DIR/prod_hosts.csv"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env not found at $ENV_FILE"
    echo "Copy setup/.env.example to setup/.env and fill it in."
    exit 1
fi
if [[ ! -f "$HOSTS_FILE" ]]; then
    echo "Error: host file not found at $HOSTS_FILE"
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
NODE_BIND_HOST="${NODE_BIND_HOST:-0.0.0.0}"
NODE_PORT="${NODE_PORT:-50051}"
CLIENT_ACK_PORT="${CLIENT_ACK_PORT:-6000}"
TMUX_SOCKET="${TMUX_SOCKET:-/tmp/crown-shared/tmux.sock}"
RUN_SCOPE="${RUN_SCOPE:-shared}"

SSH_OPTS=(-o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new)
if [[ -n "${SSH_KEY_LOCAL:-}" && -f "${SSH_KEY_LOCAL:-/dev/null}" ]]; then
    SSH_OPTS=(-i "$SSH_KEY_LOCAL" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new)
fi

read_hosts() {
    tr ',\n' '\n\n' < "$HOSTS_FILE" |
        sed 's/#.*$//' |
        sed 's/^[[:space:]]*//;s/[[:space:]]*$//' |
        awk 'NF > 0'
}

mapfile -t HOSTS < <(read_hosts)
if [[ "${#HOSTS[@]}" -eq 0 ]]; then
    echo "Error: no hosts found in $HOSTS_FILE"
    exit 1
fi

METADATA_HOST="${METADATA_HOST:-${HOSTS[0]}}"
METADATA_ADDR="${METADATA_HOST}:${METADATA_PORT}"

DATA_HOSTS=()
for host in "${HOSTS[@]}"; do
    if [[ "$host" != "$METADATA_HOST" ]]; then
        DATA_HOSTS+=("$host")
    fi
done
if [[ "${#DATA_HOSTS[@]}" -eq 0 ]]; then
    echo "Error: no data hosts remain after excluding metadata host: $METADATA_HOST"
    echo "Add at least one non-metadata host to $HOSTS_FILE."
    exit 1
fi

ALL_SETUP_HOSTS=("$METADATA_HOST")
for host in "${DATA_HOSTS[@]}"; do
    if [[ "$host" != "$METADATA_HOST" ]]; then
        ALL_SETUP_HOSTS+=("$host")
    fi
done

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

usage() {
    cat <<EOF
Usage: $0 <setup|start|rerun|kill|repl-info>

Hosts:         $HOSTS_FILE
Metadata host: $METADATA_HOST
Metadata addr: $METADATA_ADDR
Data hosts:    ${DATA_HOSTS[*]}
Members:       $MEMBERS_ARG
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
        "RUN_SCOPE=$RUN_SCOPE"
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
case "$action" in
    setup)
        for host in "${ALL_SETUP_HOSTS[@]}"; do
            copy_and_run "$host" "$SCRIPT_DIR/setup.bash"
        done
        ;;
    start|build|deploy)
        copy_and_run "$METADATA_HOST" "$SCRIPT_DIR/start_metadata.bash"
        sleep 1
        for host in "${DATA_HOSTS[@]}"; do
            copy_and_run "$host" "$SCRIPT_DIR/start_server.bash" "NODE_ID=$(printf '%q' "$host")"
        done
        ;;
    rerun)
        "$0" kill
        "$0" start
        ;;
    kill)
        for host in "${ALL_SETUP_HOSTS[@]}"; do
            copy_and_run "$host" "$SCRIPT_DIR/kill.bash"
        done
        ;;
    repl-info)
        cat <<EOF
Metadata address:
  $METADATA_ADDR

Run the REPL from a machine reachable by the servers for CommitAck callbacks.
Replace <reachable-client-host> with that machine's hostname or IP:

  build/client --metadata $METADATA_ADDR --listen <reachable-client-host>:$CLIENT_ACK_PORT --repl

Current members:
  $MEMBERS_ARG

Data hosts:
  ${DATA_HOSTS[*]}
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
