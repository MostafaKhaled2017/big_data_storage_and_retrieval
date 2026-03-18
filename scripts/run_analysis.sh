#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PATH="${REPO_ROOT}/venv"

if [ ! -d "${VENV_PATH}" ]; then
  echo "Error: venv not found at ${VENV_PATH}"
  echo "Create it with: python3 -m venv venv"
  exit 1
fi

source "${VENV_PATH}/bin/activate"

echo "Installing Python dependencies..."
python -m pip install --upgrade pip >/dev/null
python -m pip install -r "${REPO_ROOT}/requirements.txt" >/dev/null

echo "Running analysis scripts..."
python "${SCRIPT_DIR}/analyze_psql.py"
python "${SCRIPT_DIR}/analyze_mongodb.py"
python "${SCRIPT_DIR}/analyze_graph.py"
python "${SCRIPT_DIR}/build_analysis_summary.py"

echo "All analyses finished."
