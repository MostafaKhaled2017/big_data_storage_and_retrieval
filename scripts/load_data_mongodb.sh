#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TARGET_DB="${TARGET_DB:-customer_campaign_analytics}"
DATA_DIR="${DATA_DIR:-${REPO_ROOT}/data/processed}"
MONGO_URI="${MONGO_URI:-}"
CAMPAIGNS_FILE="${CAMPAIGNS_FILE:-${DATA_DIR}/campaigns.csv}"
CLIENTS_FILE="${CLIENTS_FILE:-${DATA_DIR}/client_first_purchase_date.csv}"
EVENTS_FILE="${EVENTS_FILE:-${DATA_DIR}/events.csv}"
MESSAGES_FILE="${MESSAGES_FILE:-${DATA_DIR}/messages.csv}"
FRIENDS_FILE="${FRIENDS_FILE:-${DATA_DIR}/friends.csv}"

if ! command -v mongosh >/dev/null 2>&1; then
  echo "Error: mongosh is not installed or not in PATH."
  exit 1
fi

if [ ! -d "${DATA_DIR}" ]; then
  echo "Error: data directory not found: ${DATA_DIR}"
  exit 1
fi

for f in \
  "${CAMPAIGNS_FILE}" \
  "${CLIENTS_FILE}" \
  "${EVENTS_FILE}" \
  "${MESSAGES_FILE}" \
  "${FRIENDS_FILE}"; do
  if [ ! -f "${f}" ]; then
    echo "Error: expected CSV file not found: ${f}"
    exit 1
  fi
done

echo "Loading MongoDB model and data..."
echo "  Target DB: ${TARGET_DB}"
echo "  Data dir: ${DATA_DIR}"
echo "  campaigns.csv: ${CAMPAIGNS_FILE}"
echo "  client_first_purchase_date.csv: ${CLIENTS_FILE}"
echo "  events.csv: ${EVENTS_FILE}"
echo "  messages.csv: ${MESSAGES_FILE}"
echo "  friends.csv: ${FRIENDS_FILE}"

export TARGET_DB
export DATA_DIR
export CAMPAIGNS_FILE
export CLIENTS_FILE
export EVENTS_FILE
export MESSAGES_FILE
export FRIENDS_FILE

if [ -n "${MONGO_URI}" ]; then
  mongosh "${MONGO_URI}" --file "${SCRIPT_DIR}/load_data_mongodb.js"
else
  mongosh --file "${SCRIPT_DIR}/load_data_mongodb.js"
fi

echo "Done."
