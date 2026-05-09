#!/usr/bin/env bash
set -euo pipefail

DEPLOY_USER="${SSH_USER:-$(whoami)}"

echo "=== CROWN-KV VM dependency setup (user: $DEPLOY_USER) ==="

run_as_root() {
    if [[ "$(id -u)" -eq 0 ]]; then
        "$@"
        return
    fi
    if command -v sudo >/dev/null 2>&1; then
        sudo "$@"
        return
    fi
    return 1
}

have_required_tools() {
    local missing=0
    for tool in git cmake make g++ tmux pkg-config protoc grpc_cpp_plugin python3; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            missing=1
        fi
    done
    if ! python3 -c 'import matplotlib' >/dev/null 2>&1; then
        missing=1
    fi
    return "$missing"
}

ensure_sudo() {
    if [[ "$(id -u)" -eq 0 ]]; then
        return
    fi
    if ! command -v sudo >/dev/null 2>&1; then
        echo "ERROR: sudo is not installed. Run as root or install sudo first."
        exit 1
    fi
    if sudo -n true 2>/dev/null; then
        return
    fi
    if [[ -t 0 ]]; then
        echo "sudo needs your VM password to install missing packages."
        sudo -v
        return
    fi
    echo "ERROR: sudo requires a password for $DEPLOY_USER, but no interactive terminal is available."
    echo "Run this script with an SSH TTY, install packages manually, or configure passwordless sudo."
    exit 1
}

if have_required_tools; then
    echo "Required build tools already appear to be installed; skipping package install."
    exit 0
fi

echo "Installing system packages..."
if command -v apt-get >/dev/null 2>&1; then
    ensure_sudo
    run_as_root apt-get update -y
    run_as_root apt-get install -y \
        git cmake make g++ rsync openssh-client wget vim tmux pkg-config \
        libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
        python3 python3-matplotlib
elif command -v dnf >/dev/null 2>&1; then
    ensure_sudo
    run_as_root dnf install -y \
        git cmake make gcc-c++ rsync openssh-clients wget vim tmux pkgconf-pkg-config \
        grpc-devel protobuf-devel protobuf-compiler \
        python3 python3-matplotlib
else
    echo "ERROR: no supported package manager found; expected apt-get or dnf."
    exit 1
fi

echo "=== VM dependency setup completed ==="
