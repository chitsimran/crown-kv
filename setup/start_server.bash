#!/usr/bin/env bash
set -euo pipefail

DEPLOY_USER="${SSH_USER:-$(whoami)}"
REPO_URL="${REPO_URL:?REPO_URL is required}"
REPO_BRANCH="${REPO_BRANCH:-main}"
REMOTE_BASE_DIR="${REMOTE_BASE_DIR:-/home/$DEPLOY_USER}"
REPO_NAME="${REPO_NAME:-$(basename "${REPO_URL%.git}")}"
PROJECT_SUBDIR="${PROJECT_SUBDIR:-.}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
NODE_ID="${NODE_ID:?NODE_ID is required}"
NODE_BIND_HOST="${NODE_BIND_HOST:-0.0.0.0}"
NODE_PORT="${NODE_PORT:-50051}"
METADATA_ADDR="${METADATA_ADDR:?METADATA_ADDR is required}"
RUN_SCOPE="${RUN_SCOPE:-shared}"
TMUX_SOCKET="${TMUX_SOCKET:-/tmp/crown-shared/tmux.sock}"
SESSION_NAME="crown_node_${NODE_PORT}"

echo "=== CROWN-KV server deploy starting (user: $DEPLOY_USER, node: $NODE_ID) ==="

ensure_dir_writable() {
    local dir="$1"
    if [[ ! -d "$dir" ]]; then
        if mkdir -p "$dir" 2>/dev/null; then
            return
        fi
        if command -v sudo >/dev/null 2>&1; then
            if ! sudo -n true 2>/dev/null && [[ -t 0 ]]; then
                echo "sudo needs your VM password to create shared directory: $dir"
                sudo -v
            fi
            sudo mkdir -p "$dir"
            sudo chown "$DEPLOY_USER":"$DEPLOY_USER" "$dir"
            sudo chmod 2775 "$dir"
            return
        fi
        echo "ERROR: cannot create $dir. Configure REMOTE_BASE_DIR or sudo."
        exit 1
    fi

    if [[ ! -w "$dir" ]]; then
        if command -v sudo >/dev/null 2>&1; then
            if ! sudo -n true 2>/dev/null && [[ -t 0 ]]; then
                echo "sudo needs your VM password to make shared directory writable: $dir"
                sudo -v
            fi
            sudo chown "$DEPLOY_USER":"$DEPLOY_USER" "$dir"
            sudo chmod 2775 "$dir"
            return
        fi
        echo "ERROR: $dir is not writable. Configure REMOTE_BASE_DIR or sudo."
        exit 1
    fi
}

prepare_repo() {
    ensure_dir_writable "$REMOTE_BASE_DIR"
    cd "$REMOTE_BASE_DIR"

    if [[ -e "$REPO_NAME" && ! -w "$REPO_NAME" ]]; then
        if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
            sudo chown -R "$DEPLOY_USER":"$DEPLOY_USER" "$REPO_NAME"
            sudo chmod -R u+rwX,g+rwX,o+rX "$REPO_NAME"
        else
            echo "ERROR: $REMOTE_BASE_DIR/$REPO_NAME is not writable."
            exit 1
        fi
    fi

    if [[ -d "$REPO_NAME/.git" ]]; then
        echo "Repo exists; pulling branch $REPO_BRANCH"
        git -C "$REPO_NAME" fetch --all --prune
        git -C "$REPO_NAME" checkout -f "$REPO_BRANCH"
        git -C "$REPO_NAME" pull --ff-only origin "$REPO_BRANCH"
    else
        echo "Cloning fresh: $REPO_URL"
        git clone -b "$REPO_BRANCH" "$REPO_URL" "$REPO_NAME"
    fi
    chmod -R u+rwX,g+rwX,o+rX "$REPO_NAME" 2>/dev/null || true
}

resolve_project_dir() {
    local primary="$REMOTE_BASE_DIR/$REPO_NAME/$PROJECT_SUBDIR"
    local root="$REMOTE_BASE_DIR/$REPO_NAME"
    if [[ -f "$primary/CMakeLists.txt" ]]; then
        PROJECT_DIR="$primary"
    elif [[ -f "$root/CMakeLists.txt" ]]; then
        PROJECT_DIR="$root"
    else
        echo "ERROR: could not locate project CMakeLists.txt."
        echo "Checked: $primary and $root"
        exit 1
    fi
}

build_project() {
    cd "$PROJECT_DIR"
    for tool in git cmake g++ tmux; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            echo "ERROR: $tool is not installed. Run ./setup/vm_setup.bash setup first."
            exit 1
        fi
    done
    cmake -S . -B build -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
    local jobs="2"
    if command -v nproc >/dev/null 2>&1; then
        jobs="$(nproc)"
    fi
    cmake --build build -j "$jobs"
    if [[ ! -x build/server ]]; then
        echo "ERROR: build/server not found after build."
        exit 1
    fi
}

start_server() {
    cd "$PROJECT_DIR"
    local run_dir="$PROJECT_DIR/run/$RUN_SCOPE"
    mkdir -p "$run_dir"
    local pid_file="$run_dir/server_${NODE_PORT}.pid"
    local log_file="$run_dir/server_${NODE_PORT}.log"
    local out_file="$run_dir/server_${NODE_PORT}.out"

    local socket_dir
    socket_dir="$(dirname "$TMUX_SOCKET")"
    mkdir -p "$socket_dir"
    chmod 1777 "$socket_dir" 2>/dev/null || sudo chmod 1777 "$socket_dir" 2>/dev/null || true
    if [[ -S "$TMUX_SOCKET" ]]; then
        rm -f "$TMUX_SOCKET" 2>/dev/null || sudo rm -f "$TMUX_SOCKET" 2>/dev/null || true
    fi
    local tmux_cmd=(tmux -S "$TMUX_SOCKET")

    if "${tmux_cmd[@]}" has-session -t "$SESSION_NAME" 2>/dev/null; then
        echo "Stopping existing tmux session: $SESSION_NAME"
        "${tmux_cmd[@]}" kill-session -t "$SESSION_NAME" || true
        sleep 1
    fi
    if [[ -f "$pid_file" ]]; then
        local old_pid
        old_pid="$(cat "$pid_file" 2>/dev/null || true)"
        if [[ -n "$old_pid" ]] && kill -0 "$old_pid" 2>/dev/null; then
            kill "$old_pid" || true
            sleep 1
        fi
    fi
    pkill -u "$DEPLOY_USER" -f "server .*--node-id ${NODE_ID} .*--listen .*:${NODE_PORT}" >/dev/null 2>&1 || true

    : > "$log_file"
    : > "$out_file"

    local server_cmd
    server_cmd="cd '$PROJECT_DIR' && echo '[started] '\"\$(date -Is)\" && echo '[host] '\"\$(hostname -f 2>/dev/null || hostname)\" && echo '[cmd] build/server --node-id ${NODE_ID} --listen ${NODE_BIND_HOST}:${NODE_PORT} --metadata ${METADATA_ADDR}' && exec build/server --node-id '${NODE_ID}' --listen '${NODE_BIND_HOST}:${NODE_PORT}' --metadata '${METADATA_ADDR}' 2>&1"
    echo "Run command: $server_cmd"
    {
        echo "[launch] $(date -Is)"
        echo "[host] $(hostname -f 2>/dev/null || hostname)"
        echo "[session] $SESSION_NAME"
        echo "[cmd] $server_cmd"
    } | tee -a "$log_file" >> "$out_file"
    "${tmux_cmd[@]}" new-session -d -s "$SESSION_NAME" "bash -lc $(printf '%q' "$server_cmd")"
    chmod 666 "$TMUX_SOCKET" 2>/dev/null || true
    "${tmux_cmd[@]}" pipe-pane -o -t "$SESSION_NAME:0.0" "cat >> '$log_file'"
    "${tmux_cmd[@]}" display-message -p -t "$SESSION_NAME:0.0" "#{pane_pid}" > "$pid_file"

    echo "Server started in tmux session: $SESSION_NAME"
    echo "log: $log_file"
    echo "out: $out_file"
    echo "pid: $pid_file"
    echo "attach: tmux -S $TMUX_SOCKET attach -t $SESSION_NAME"
}

prepare_repo
resolve_project_dir
build_project
start_server
