#!/usr/bin/env bash
# Usage: test-watchdog.sh
# Runs `zig build test` and stops it when it stalls or exhausts memory, so a
# hung or runaway test ends as a failed step with something to read.
#
# A runner whose job starves it loses contact with GitHub and takes the whole
# log with it, and a test that only hangs runs to the job timeout and says
# nothing. So past $TEST_WATCHDOG_SECONDS, or with under 1 GiB of memory left,
# this prints the process table and every thread's stack in each test binary,
# then aborts the binaries. `zig build` then reports the step with the output
# it captured, which ends at the name of the test that was running.
set -uo pipefail

# A green run takes 8 to 15 minutes on the Linux runners and over 30 on the
# macOS one, where this limit at 1800 stopped a healthy run.
if [ "$(uname)" = Darwin ]; then default_limit_s=3600; else default_limit_s=1800; fi
limit_s=${TEST_WATCHDOG_SECONDS:-$default_limit_s}
min_avail_kb=$((1024 * 1024))

available_kb() {
  if [ -r /proc/meminfo ]; then
    awk '/^MemAvailable:/ { print $2 }' /proc/meminfo
  else
    echo $((min_avail_kb * 16))
  fi
}

test_pids() {
  # The test binaries themselves, not run_test_quiet, which names one in its
  # arguments and must outlive it to print the log.
  pgrep -f '^[^ ]*\.zig-cache/o/[0-9a-f]+/test( |$)' || true
}

dump_stacks() {
  local pid=$1
  echo "--- threads of test process $pid"
  if [ "$(uname)" = Darwin ]; then
    sample "$pid" 3 2>&1 | head -400 || true
    return
  fi
  if ! command -v gdb >/dev/null 2>&1; then
    sudo apt-get install -y -q gdb >/dev/null 2>&1 || true
  fi
  if command -v gdb >/dev/null 2>&1; then
    sudo timeout 120 gdb -p "$pid" -batch -ex 'thread apply all bt 25' 2>&1 | grep -v '^\[New LWP' | head -600 || true
  else
    echo "gdb is not available; thread states only"
    ps -L -o tid,stat,pcpu,comm -p "$pid" || true
  fi
}

zig build test &
build=$!
reason=""
runaway=0
while kill -0 "$build" 2>/dev/null; do
  sleep 15
  if [ "$SECONDS" -ge "$limit_s" ]; then
    reason="still running after ${SECONDS}s"
  elif [ "$(available_kb)" -lt "$min_avail_kb" ]; then
    reason="available memory is down to $(($(available_kb) / 1024)) MiB"
    runaway=1
  fi
  [ -n "$reason" ] && break
done

if [ -z "$reason" ]; then
  wait "$build"
  exit $?
fi

echo "::error::test suite stopped by the watchdog: $reason"
ps -eo pid,ppid,rss,pcpu,etime,args | sort -k3 -n -r | head -20
pids=$(test_pids)
# A runaway is stopped first: its stacks are still readable while stopped,
# and it can't take the last of the memory while they are printed.
if [ "$runaway" = 1 ]; then
  for pid in $pids; do kill -STOP "$pid" 2>/dev/null || true; done
fi
for pid in $pids; do dump_stacks "$pid"; done
for pid in $pids; do kill -ABRT "$pid" 2>/dev/null || true; kill -CONT "$pid" 2>/dev/null || true; done
wait "$build"
exit 1
