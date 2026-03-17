#!/usr/bin/env bash
# =============================================================================
# FIN-AI Auditor – Start / Restart all components
# Usage: ./start.sh          (start all)
#        ./start.sh restart  (kill running, then start all)
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

PIDFILE_API=".pids/api.pid"
PIDFILE_WORKER=".pids/worker.pid"
PIDFILE_WEB=".pids/web.pid"
LOGDIR="logs"

mkdir -p .pids "$LOGDIR"

# ── Helpers ─────────────────────────────────────────────────────────────────
kill_component() {
    local name="$1" pidfile="$2"
    if [[ -f "$pidfile" ]]; then
        local pid
        pid=$(cat "$pidfile" 2>/dev/null || true)
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            echo "⏹  Stopping $name (PID $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 1
            kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$pidfile"
    fi
}

wait_for_port() {
    local port="$1" name="$2" max_wait="${3:-15}"
    local waited=0
    while ! lsof -iTCP:"$port" -sTCP:LISTEN -P -n >/dev/null 2>&1; do
        sleep 1
        waited=$((waited + 1))
        if [[ $waited -ge $max_wait ]]; then
            echo "⚠️  $name did not start on port $port within ${max_wait}s"
            return 1
        fi
    done
    echo "✅ $name is running on port $port"
}

# ── Restart mode ────────────────────────────────────────────────────────────
if [[ "${1:-}" == "restart" ]]; then
    echo "🔄 Restarting all components..."
    kill_component "API Server" "$PIDFILE_API"
    kill_component "Worker" "$PIDFILE_WORKER"
    kill_component "Web Dev Server" "$PIDFILE_WEB"
    sleep 1
fi

# ── Activate venv ───────────────────────────────────────────────────────────
if [[ -f .venv/bin/activate ]]; then
    source .venv/bin/activate
fi

# ── 1. API Server (FastAPI/Uvicorn) ─────────────────────────────────────────
if [[ -f "$PIDFILE_API" ]] && kill -0 "$(cat "$PIDFILE_API" 2>/dev/null)" 2>/dev/null; then
    echo "✅ API Server already running (PID $(cat "$PIDFILE_API"))"
else
    echo "🚀 Starting API Server..."
    nohup python -m fin_ai_auditor.main \
        > "$LOGDIR/api.log" 2>&1 &
    echo $! > "$PIDFILE_API"
    wait_for_port 8088 "API Server" 15
fi

# ── 2. Worker ───────────────────────────────────────────────────────────────
if [[ -f "$PIDFILE_WORKER" ]] && kill -0 "$(cat "$PIDFILE_WORKER" 2>/dev/null)" 2>/dev/null; then
    echo "✅ Worker already running (PID $(cat "$PIDFILE_WORKER"))"
else
    echo "🚀 Starting Worker..."
    nohup python -m fin_ai_auditor.worker.main \
        > "$LOGDIR/worker.log" 2>&1 &
    echo $! > "$PIDFILE_WORKER"
    sleep 2
    if kill -0 "$(cat "$PIDFILE_WORKER")" 2>/dev/null; then
        echo "✅ Worker is running (PID $(cat "$PIDFILE_WORKER"))"
    else
        echo "⚠️  Worker failed to start – check $LOGDIR/worker.log"
    fi
fi

# ── 3. Web Dev Server (Vite) ───────────────────────────────────────────────
if [[ -f "$PIDFILE_WEB" ]] && kill -0 "$(cat "$PIDFILE_WEB" 2>/dev/null)" 2>/dev/null; then
    echo "✅ Web Dev Server already running (PID $(cat "$PIDFILE_WEB"))"
else
    echo "🚀 Starting Web Dev Server..."
    cd web
    nohup npm run dev > "../$LOGDIR/web.log" 2>&1 &
    echo $! > "../$PIDFILE_WEB"
    cd ..
    wait_for_port 5174 "Web Dev Server" 20
fi

echo ""
echo "═══════════════════════════════════════════════════"
echo "  FIN-AI Auditor – All components running"
echo "  API:    http://127.0.0.1:8088"
echo "  Web:    http://127.0.0.1:5174"
echo "  Worker: background (check $LOGDIR/worker.log)"
echo "═══════════════════════════════════════════════════"
echo ""
echo "Logs:  tail -f $LOGDIR/api.log $LOGDIR/worker.log $LOGDIR/web.log"
echo "Stop:  ./stop.sh"
