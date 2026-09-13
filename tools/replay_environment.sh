#!/usr/bin/env bash
set -euo pipefail

preset="${1:-kafka-debug}"
cmake --preset "${preset}"
cmake --build --preset "${preset}"
ctest --preset "${preset}" --output-on-failure

if [[ -f deploy/sql/001_core.sql ]] && command -v mariadb >/dev/null 2>&1; then
  mariadb -u phototask -h 127.0.0.1 phototask_test < deploy/sql/001_core.sql
fi

runtime="build/${preset}/apps/photo_runtime"
if [[ -x "${runtime}" ]] && command -v curl >/dev/null 2>&1; then
  port=18081
  "${runtime}" --listen "127.0.0.1:${port}" >/tmp/phototask-replay-runtime.log 2>&1 &
  runtime_pid=$!
  cleanup() {
    kill "${runtime_pid}" 2>/dev/null || true
    wait "${runtime_pid}" 2>/dev/null || true
  }
  trap cleanup EXIT
  for _ in $(seq 1 20); do
    if curl --fail --silent "http://127.0.0.1:${port}/readyz" >/dev/null; then
      break
    fi
    sleep 0.05
  done
  curl --fail --silent "http://127.0.0.1:${port}/livez" >/dev/null
  curl --fail --silent "http://127.0.0.1:${port}/metrics" >/dev/null
fi
