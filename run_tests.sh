#!/usr/bin/env bash
# 运行全部自测：引擎单元测试 + FastAPI 端到端测试
set -euo pipefail
cd "$(dirname "$0")"
PY=.venv/bin/python
if [ ! -x "$PY" ]; then
  PY=python3
fi
"$PY" -m unittest discover -s tests -v
