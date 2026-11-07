#!/bin/bash
cd "$(dirname "$0")"
if [ -f settings.env ]; then set -a; . ./settings.env; set +a; fi
if [ ! -x .venv/bin/python ]; then
  echo "First run: installing the Anthropic SDK (one-off)..."
  python3 -m venv .venv && .venv/bin/pip install -q anthropic
fi
(sleep 3; open http://localhost:8000) &
.venv/bin/python app.py
