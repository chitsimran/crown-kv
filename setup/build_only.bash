#!/usr/bin/env bash
set -euo pipefail

DEPLOY_USER="${SSH_USER:-$(whoami)}"
REPO_URL="${REPO_URL:?REPO_URL is required}"
REPO_BRANCH="${REPO_BRANCH:-main}"
REMOTE_BASE_DIR="${REMOTE_BASE_DIR:-/home/$DEPLOY_USER}"
REPO_NAME="${REPO_NAME:-$(basename "${REPO_URL%.git}")}"
PROJECT_SUBDIR="${PROJECT_SUBDIR:-.}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
SKIP_BUILD="${SKIP_BUILD:-0}"

echo "=== CROWN-KV client build (user: $DEPLOY_USER on $(hostname -f 2>/dev/null || hostname)) ==="

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
    if [[ ! -x build/client ]]; then
        echo "ERROR: build/client not found after build."
        exit 1
    fi
}

if [[ "$SKIP_BUILD" == "1" ]]; then
    echo "SKIP_BUILD=1: skipping git pull and build; reusing existing binary"
    resolve_project_dir
    if [[ ! -x "$PROJECT_DIR/build/client" ]]; then
        echo "ERROR: $PROJECT_DIR/build/client not found; cannot skip build"
        exit 1
    fi
else
    prepare_repo
    resolve_project_dir
    build_project
fi

echo "Build complete on $(hostname -f 2>/dev/null || hostname)."
echo "Run the client manually once the ring is up:"
echo "  cd $PROJECT_DIR && build/client --metadata \$METADATA_ADDR --listen \$(hostname -f):6000 --repl"
