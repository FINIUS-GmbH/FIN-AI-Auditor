#!/usr/bin/env bash
# dev.sh — Start FIN-AI Auditor Backend + Worker + Frontend
# Usage: ./dev.sh

set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$DIR/logs"

# Colors
G='\033[0;32m'; Y='\033[1;33m'; R='\033[0;31m'; N='\033[0m'

cleanup() {
  echo -e "\n${Y}Stopping...${N}"
  kill ${BE_PID:-} ${WK_PID:-} ${FE_PID:-} 2>/dev/null || true
  wait ${BE_PID:-} ${WK_PID:-} ${FE_PID:-} 2>/dev/null || true
  echo -e "${G}Done.${N}"
}
trap cleanup EXIT INT TERM

mkdir -p "$LOG_DIR"

# Kill any existing processes on our ports
lsof -ti:8088 2>/dev/null | xargs kill -9 2>/dev/null || true
lsof -ti:5174 2>/dev/null | xargs kill -9 2>/dev/null || true
sleep 0.5

# ── Backend ──
echo -e "${G}▶ Starting Backend (127.0.0.1:8088)...${N}"
cd "$DIR"
source .venv/bin/activate
python -m fin_ai_auditor.main &
BE_PID=$!

# Wait for backend to be ready
for i in $(seq 1 20); do
  if curl -s http://127.0.0.1:8088/api/bootstrap > /dev/null 2>&1; then
    echo -e "${G}✓ Backend ready${N}"
    break
  fi
  if [ $i -eq 20 ]; then
    echo -e "${R}✗ Backend failed to start${N}"
    exit 1
  fi
  sleep 0.5
done

# ── Worker ──
echo -e "${G}▶ Starting Worker...${N}"
python -m fin_ai_auditor.worker.main > "$LOG_DIR/worker.log" 2>&1 &
WK_PID=$!
sleep 1
if ! kill -0 "$WK_PID" 2>/dev/null; then
  echo -e "${R}✗ Worker failed to start${N}"
  exit 1
fi
echo -e "${G}✓ Worker ready${N}"

# ── Frontend ──
echo -e "${G}▶ Starting Frontend (127.0.0.1:5174)...${N}"
cd "$DIR/web"
npm run dev &
FE_PID=$!
sleep 2
echo ""
echo -e "${G}═══════════════════════════════════════${N}"
echo -e "${G}  FIN-AI Auditor Dev Server${N}"
echo -e "${G}  Backend:  http://127.0.0.1:8088${N}"
echo -e "${G}  Frontend: http://127.0.0.1:5174${N}"
echo -e "${G}  Worker:   background (logs/worker.log)${N}"
echo -e "${G}═══════════════════════════════════════${N}"
echo -e "${Y}  Press Ctrl+C to stop both${N}"
echo ""

wait
