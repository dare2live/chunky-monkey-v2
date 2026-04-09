#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
echo "=========================================="
echo "  救生艇 — 机构新进股票查询"
echo "=========================================="
python3 fetch_and_report.py
echo ""
echo "正在打开报告..."
open report.html
