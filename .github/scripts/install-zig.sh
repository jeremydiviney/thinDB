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
  zig-x86_64-linux-0.16.0.tar.xz) want=70e49664a74374b48b51e6f3fdfbf437f6395d42509050588bd49abe52ba3d00 ;;
  zig-aarch64-linux-0.16.0.tar.xz) want=ea4b09bfb22ec6f6c6ceac57ab63efb6b46e17ab08d21f69f3a48b38e1534f17 ;;
  zig-aarch64-macos-0.16.0.tar.xz) want=b23d70deaa879b5c2d486ed3316f7eaa53e84acf6fc9cc747de152450d401489 ;;
  zig-x86_64-windows-0.16.0.zip) want=68659eb5f1e4eb1437a722f1dd889c5a322c9954607f5edcf337bc3684a75a7e ;;
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
  for mirror in $mirrors; do
    fetch "$mirror/$file?source=github-thindb-ci" --retry 1 --retry-all-errors \
      --speed-limit 500000 --speed-time 30 && break
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
