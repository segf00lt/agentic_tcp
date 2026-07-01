#!/bin/sh

set -eu

if command -v go >/dev/null 2>&1; then
  printf 'Go is already installed: %s\n' "$(go version)"
  exit 0
fi

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) printf 'Unsupported OS: %s\n' "$os" >&2; exit 1 ;;
esac

case "$arch" in
  x86_64|amd64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *) printf 'Unsupported architecture: %s\n' "$arch" >&2; exit 1 ;;
esac

ver="1.26.4"
tarball="go${ver}.${os}-${arch}.tar.gz"
url="https://go.dev/dl/${tarball}"

tmpdir="${TMPDIR:-/tmp}/go-install.$$"
mkdir -p "$tmpdir"
trap 'rm -rf "$tmpdir"' 0 1 2 15

curl -fsSL "$url" -o "$tmpdir/$tarball"

sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf "$tmpdir/$tarball"

printf 'Installed: %s\n' "$(/usr/local/go/bin/go version)"
