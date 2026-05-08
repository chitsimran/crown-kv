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

ensure_passwordless_sudo() {
    if [[ "$(id -u)" -eq 0 ]]; then
        return
    fi
    if ! command -v sudo >/dev/null 2>&1; then
        echo "ERROR: sudo is not installed. Run as root or install sudo first."
        exit 1
    fi
    if ! sudo -n true 2>/dev/null; then
        echo "ERROR: sudo requires a password for $DEPLOY_USER."
        echo "Configure passwordless sudo, or run this script as root."
        exit 1
    fi
}

echo "Installing system packages..."
if command -v apt-get >/dev/null 2>&1; then
    ensure_passwordless_sudo
    run_as_root apt-get update -y
    run_as_root apt-get install -y \
        git cmake make g++ rsync openssh-client wget vim tmux pkg-config \
        libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc
elif command -v dnf >/dev/null 2>&1; then
    ensure_passwordless_sudo
    run_as_root dnf install -y \
        git cmake make gcc-c++ rsync openssh-clients wget vim tmux pkgconf-pkg-config \
        grpc-devel protobuf-devel protobuf-compiler
else
    echo "ERROR: no supported package manager found; expected apt-get or dnf."
    exit 1
fi

echo "=== VM dependency setup completed ==="
