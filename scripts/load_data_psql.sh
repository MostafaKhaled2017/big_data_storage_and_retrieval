#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TARGET_DB="${TARGET_DB:-customer_campaign_analytics}"
ADMIN_DB="${ADMIN_DB:-postgres}"
DATA_DIR="${DATA_DIR:-${REPO_ROOT}/data/processed}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
CAMPAIGNS_FILE="${CAMPAIGNS_FILE:-${DATA_DIR}/campaigns.csv}"
CLIENTS_FILE="${CLIENTS_FILE:-${DATA_DIR}/client_first_purchase_date.csv}"
EVENTS_FILE="${EVENTS_FILE:-${DATA_DIR}/events.csv}"
MESSAGES_FILE="${MESSAGES_FILE:-${DATA_DIR}/messages.csv}"
FRIENDS_FILE="${FRIENDS_FILE:-${DATA_DIR}/friends.csv}"
SQL_TEMPLATE="${SCRIPT_DIR}/load_data_psql.sql"

escape_sed_replacement() {
  printf '%s' "$1" | sed -e 's/[&|\\]/\\&/g'
}

if ! command -v psql >/dev/null 2>&1; then
  echo "Error: psql is not installed or not in PATH."
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

echo "Loading PostgreSQL model and data..."
echo "  Admin DB: ${ADMIN_DB}"
echo "  Target DB: ${TARGET_DB}"
echo "  Data dir: ${DATA_DIR}"
echo "  PGHOST: ${PGHOST}"
echo "  PGPORT: ${PGPORT}"
echo "  PGUSER: ${PGUSER}"
echo "  campaigns.csv: ${CAMPAIGNS_FILE}"
echo "  client_first_purchase_date.csv: ${CLIENTS_FILE}"
echo "  events.csv: ${EVENTS_FILE}"
echo "  messages.csv: ${MESSAGES_FILE}"
echo "  friends.csv: ${FRIENDS_FILE}"

TMP_SQL="$(mktemp "${TMPDIR:-/tmp}/load_data_psql.XXXXXX.sql")"
trap 'rm -f "${TMP_SQL}"' EXIT

sed \
  -e "s|__CAMPAIGNS_FILE__|$(escape_sed_replacement "${CAMPAIGNS_FILE}")|g" \
  -e "s|__CLIENTS_FILE__|$(escape_sed_replacement "${CLIENTS_FILE}")|g" \
  -e "s|__EVENTS_FILE__|$(escape_sed_replacement "${EVENTS_FILE}")|g" \
  -e "s|__MESSAGES_FILE__|$(escape_sed_replacement "${MESSAGES_FILE}")|g" \
  -e "s|__FRIENDS_FILE__|$(escape_sed_replacement "${FRIENDS_FILE}")|g" \
  "${SQL_TEMPLATE}" > "${TMP_SQL}"

psql \
  -h "${PGHOST}" \
  -p "${PGPORT}" \
  -U "${PGUSER}" \
  -d "${ADMIN_DB}" \
  -v ON_ERROR_STOP=1 \
  -v target_db="${TARGET_DB}" \
  -v data_dir="${DATA_DIR}" \
  -v campaigns_file="${CAMPAIGNS_FILE}" \
  -v clients_file="${CLIENTS_FILE}" \
  -v events_file="${EVENTS_FILE}" \
  -v messages_file="${MESSAGES_FILE}" \
  -v friends_file="${FRIENDS_FILE}" \
  -f "${TMP_SQL}"

echo "Done."
