#!/usr/bin/env bash
# Usage: install-zig.sh <arch-os>
# Installs Zig $ZIG_VERSION for <arch-os> under $RUNNER_TEMP and adds it to
# $GITHUB_PATH. An archive already there (restored by actions/cache) is
# reused when its checksum matches.
#
# ziglang.org asks CI to download from its community mirrors, and it has
# served runners at 40-60 KB/s (issue #465). So each mirror is tried in
# turn, and one that stays under 500 KB/s for 30 s is abandoned; ziglang.org
# itself is the last resort. Any copy must match the SHA-256 pinned below
# (from ziglang.org/download/index.json), so a mirror is never trusted.
set -euo pipefail

arch_os=$1
case "$arch_os" in
  *-windows) ext=zip ;;
  *) ext=tar.xz ;;
esac
name="zig-$arch_os-$ZIG_VERSION"
file="$name.$ext"

case "$file" in
  zig-x86_64-linux-0.17.0.tar.xz) want=1cbe9df9f27e6b78d14ccbca43b6703a404ef79ef1c463de901d7f088d4e2026 ;;
  zig-aarch64-linux-0.17.0.tar.xz) want=9e8d11661d4ae3bd57702a3832781e23ad151dde5798e16a5ccd503f65234ff8 ;;
  zig-aarch64-macos-0.17.0.tar.xz) want=b607e9b9234790a008116ae5bdb71c6243b84b9fb42a53a9e70fde41c06c536a ;;
  zig-x86_64-windows-0.17.0.zip) want=b5663f69581dcf391293fbf16c06cb80d81d806545ce618b4d0bab7f0eb8c428 ;;
  *)
    echo "::error::no pinned SHA-256 for $file; add it to $0 from ziglang.org/download/index.json"
    exit 1
    ;;
esac

sha256_of() {
  if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1
}

# fetch <url> [curl options...]
fetch() {
  local url=$1
  shift
  rm -f "$file.part"
  curl -fL --connect-timeout 15 "$@" -o "$file.part" "$url" || return 1
  if [ "$(sha256_of "$file.part")" != "$want" ]; then
    echo "::warning::$url does not match the pinned SHA-256"
    return 1
  fi
  mv "$file.part" "$file"
}

cd "$RUNNER_TEMP"
if [ -f "$file" ] && [ "$(sha256_of "$file")" = "$want" ]; then
  echo "Using cached $file"
else
  rm -f "$file"
  mirrors=$(curl -fsSL --max-time 30 https://ziglang.org/download/community-mirrors.txt | tr -d '\r' | grep '^https://' || true)
  count=$(printf '%s\n' "$mirrors" | grep -c . || true)
  if [ "$count" -gt 0 ]; then
    # Start at a random mirror so runners spread their load.
    start=$((RANDOM % count))
    mirrors=$(printf '%s\n' "$mirrors" | awk -v s="$start" '{ m[NR - 1] = $0 } END { for (i = 0; i < NR; i++) print m[(s + i) % NR] }')
  fi
  # The next mirror is the retry: one slow mirror costs 30 s, not more.
  for mirror in $mirrors; do
    fetch "$mirror/$file?source=github-thindb-ci" --speed-limit 500000 --speed-time 30 && break
  done
  if [ ! -f "$file" ] && ! fetch "https://ziglang.org/download/$ZIG_VERSION/$file" --retry 3 --retry-all-errors; then
    echo "::error::could not download $file from any mirror or from ziglang.org"
    exit 1
  fi
fi

if [ "$ext" = zip ]; then unzip -q -o "$file"; else tar -xJf "$file"; fi
dir="$PWD/$name"
if command -v cygpath >/dev/null; then dir=$(cygpath -w "$dir"); fi
echo "$dir" >> "$GITHUB_PATH"
