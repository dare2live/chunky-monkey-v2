#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
PORT=8000

find_port_pids() {
  lsof -nP -iTCP:"$PORT" -sTCP:LISTEN -t 2>/dev/null | sort -u
}

pid_belongs_to_project() {
  local pid="$1"
  local cwd
  cwd="$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p' | head -n 1)"
  [[ "$cwd" == "$BACKEND_DIR" ]]
}

stop_project_server() {
  local matched=()
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    if pid_belongs_to_project "$pid"; then
      matched+=("$pid")
    fi
  done < <(find_port_pids)

  if [[ ${#matched[@]} -eq 0 ]]; then
    return 0
  fi

  echo "检测到旧的 Chunky Monkey 实例正在占用 $PORT 端口，准备重启..."
  kill "${matched[@]}" 2>/dev/null || true

  for _ in {1..20}; do
    sleep 0.3
    local still_running=0
    for pid in "${matched[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then
        still_running=1
        break
      fi
    done
    [[ $still_running -eq 0 ]] && return 0
  done

  echo "旧实例未能及时退出，强制结束..."
  kill -9 "${matched[@]}" 2>/dev/null || true
}

check_port_conflict() {
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    if ! pid_belongs_to_project "$pid"; then
      echo "端口 $PORT 已被其他程序占用（PID: $pid），未自动处理。"
      echo "请先关闭占用该端口的程序，或改用其他端口启动。"
      exit 1
    fi
  done < <(find_port_pids)
}

cd "$BACKEND_DIR"

stop_project_server
check_port_conflict

echo "========================================"
echo "  Chunky Monkey v2 启动中..."
echo "  地址: http://localhost:$PORT"
echo "  API:  http://localhost:$PORT/docs"
echo "  按 Ctrl+C 停止"
echo "========================================"
python3 -m uvicorn main:app --host 0.0.0.0 --port "$PORT" --reload
