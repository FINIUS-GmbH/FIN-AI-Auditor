#!/usr/bin/env bash
# =============================================================================
# FIN-AI Auditor – Stop all components
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

for pidfile in .pids/*.pid; do
    [[ -f "$pidfile" ]] || continue
    name=$(basename "$pidfile" .pid)
    pid=$(cat "$pidfile" 2>/dev/null || true)
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        echo "⏹  Stopping $name (PID $pid)..."
        kill "$pid" 2>/dev/null || true
        sleep 1
        kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
done
echo "✅ All components stopped."
