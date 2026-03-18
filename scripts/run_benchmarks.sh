#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PATH="${REPO_ROOT}/venv"
RUNS="${1:-1}"

if [ ! -d "${VENV_PATH}" ]; then
  echo "Error: venv not found at ${VENV_PATH}"
  echo "Create it with: python3 -m venv venv"
  exit 1
fi

if ! [[ "${RUNS}" =~ ^[0-9]+$ ]] || [ "${RUNS}" -lt 1 ]; then
  echo "Error: N must be a positive integer."
  echo "Usage: ./scripts/run_benchmarks.sh [N]"
  exit 1
fi

source "${VENV_PATH}/bin/activate"

echo "Running benchmarks with N=${RUNS}..."
python "${SCRIPT_DIR}/benchmark_psql.py" "${RUNS}"
python "${SCRIPT_DIR}/benchmark_mongo.py" "${RUNS}"
python "${SCRIPT_DIR}/benchmark_graph.py" "${RUNS}"

echo "Done. Results saved in ${REPO_ROOT}/benchmark_results/"
