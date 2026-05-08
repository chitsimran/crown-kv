#!/usr/bin/env bash
set -euo pipefail

DEPLOY_USER="${SSH_USER:-$(whoami)}"
METADATA_PORT="${METADATA_PORT:-50050}"
NODE_PORT="${NODE_PORT:-50051}"
RUN_SCOPE="${RUN_SCOPE:-shared}"
TMUX_SOCKET="${TMUX_SOCKET:-/tmp/crown-shared/tmux.sock}"

echo "=== CROWN-KV stop (user: $DEPLOY_USER on $(hostname -f 2>/dev/null || hostname)) ==="

if command -v tmux >/dev/null 2>&1; then
    TMUX_CMD=(tmux -S "$TMUX_SOCKET")
    while IFS= read -r session_name; do
        [[ -n "$session_name" ]] || continue
        if [[ "$session_name" =~ ^crown_node_ || "$session_name" =~ ^crown_metadata_ ]]; then
            echo "Killing tmux session: $session_name"
            "${TMUX_CMD[@]}" kill-session -t "$session_name" >/dev/null 2>&1 || true
        fi
    done < <("${TMUX_CMD[@]}" list-sessions -F "#{session_name}" 2>/dev/null || true)
fi

echo "Killing CROWN-KV processes for user $DEPLOY_USER"
pkill -u "$DEPLOY_USER" -f 'build/server|/server --node-id|metadata_store' >/dev/null 2>&1 || true
pkill -u "$DEPLOY_USER" -f "server .*--listen .*:${NODE_PORT}" >/dev/null 2>&1 || true
pkill -u "$DEPLOY_USER" -f "metadata_store .*--listen .*:${METADATA_PORT}" >/dev/null 2>&1 || true

echo "Cleaning pid files"
for base_dir in "$HOME" "$HOME/research" "$HOME/crown-kv" "$(find "$HOME" -maxdepth 2 -name crown-kv -type d 2>/dev/null || true)"; do
    [[ -d "$base_dir" ]] || continue
    for pidfile in "$base_dir"/*/run/"$RUN_SCOPE"/*.pid "$base_dir"/run/"$RUN_SCOPE"/*.pid; do
        [[ -f "$pidfile" ]] || continue
        echo "Removing $pidfile"
        rm -f "$pidfile" || true
    done
done

echo "Done."
