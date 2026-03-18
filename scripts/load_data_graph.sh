#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DATA_DIR="${DATA_DIR:-${REPO_ROOT}/data/processed}"
CYPHER_TEMPLATE="${SCRIPT_DIR}/load_data_graph.cypherl"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/load_data_graph.XXXXXX")"
trap 'rm -rf "${TMP_DIR}"' EXIT
MESSAGE_CLIENTS_STAGE_TEMPLATE="${TMP_DIR}/load_data_graph_messages_clients.cypher"
EVENTS_STAGE_TEMPLATE="${TMP_DIR}/load_data_graph_events.cypher"
MESSAGES_STAGE_TEMPLATE="${TMP_DIR}/load_data_graph_messages.cypher"
FRIENDS_STAGE_TEMPLATE="${TMP_DIR}/load_data_graph_friends.cypher"
COUNTS_TEMPLATE="${TMP_DIR}/load_data_graph_counts.cypher"

NEO4J_URI="${NEO4J_URI:-bolt://localhost:7687}"
CONTAINER_NEO4J_URI="${CONTAINER_NEO4J_URI:-bolt://localhost:7687}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-neo4jneo4j}"
NEO4J_DATABASE="${NEO4J_DATABASE:-neo4j}"
BATCH_SIZE="${BATCH_SIZE:-500}"
DELETE_REL_BATCH_SIZE="${DELETE_REL_BATCH_SIZE:-2000}"
DELETE_BATCH_SIZE="${DELETE_BATCH_SIZE:-500}"
CLIENT_BATCH_SIZE="${CLIENT_BATCH_SIZE:-${BATCH_SIZE}}"
CAMPAIGN_BATCH_SIZE="${CAMPAIGN_BATCH_SIZE:-200}"
EVENT_BATCH_SIZE="${EVENT_BATCH_SIZE:-25}"
EVENT_CHUNK_ROWS="${EVENT_CHUNK_ROWS:-10000}"
MESSAGE_CLIENT_BATCH_SIZE="${MESSAGE_CLIENT_BATCH_SIZE:-25}"
MESSAGE_CLIENT_CHUNK_ROWS="${MESSAGE_CLIENT_CHUNK_ROWS:-10000}"
MESSAGE_BATCH_SIZE="${MESSAGE_BATCH_SIZE:-10}"
MESSAGE_CHUNK_ROWS="${MESSAGE_CHUNK_ROWS:-10000}"
FRIEND_BATCH_SIZE="${FRIEND_BATCH_SIZE:-100}"
FRIEND_CHUNK_ROWS="${FRIEND_CHUNK_ROWS:-10000}"
USER_EVENT_BATCH_SIZE="${USER_EVENT_BATCH_SIZE:-100}"
USER_MESSAGE_BATCH_SIZE="${USER_MESSAGE_BATCH_SIZE:-10}"
USER_FRIEND_BATCH_SIZE="${USER_FRIEND_BATCH_SIZE:-50}"
NEO4J_LOAD_MODE="${NEO4J_LOAD_MODE:-auto}"
NEO4J_CONTAINER="${NEO4J_CONTAINER:-neo4j_server}"
DOCKER_IMPORT_DIR="${DOCKER_IMPORT_DIR:-/var/lib/neo4j/import}"
NEO4J_CONNECT_RETRIES="${NEO4J_CONNECT_RETRIES:-4}"
NEO4J_CONNECT_WAIT_SECONDS="${NEO4J_CONNECT_WAIT_SECONDS:-5}"
NEO4J_CYPHER_EXEC_MODE="${NEO4J_CYPHER_EXEC_MODE:-auto}"
CONTAINER_CYPHER_SHELL_BIN=""

CAMPAIGNS_FILE="${CAMPAIGNS_FILE:-${DATA_DIR}/campaigns.csv}"
CLIENTS_FILE="${CLIENTS_FILE:-${DATA_DIR}/client_first_purchase_date.csv}"
EVENTS_FILE="${EVENTS_FILE:-${DATA_DIR}/events.csv}"
MESSAGES_FILE="${MESSAGES_FILE:-${DATA_DIR}/messages.csv}"
FRIENDS_FILE="${FRIENDS_FILE:-${DATA_DIR}/friends.csv}"
EVENTS_USERS_FILE="${TMP_DIR}/load_data_graph_events_users.csv"
MESSAGES_USERS_FILE="${TMP_DIR}/load_data_graph_messages_users.csv"
FRIENDS_USERS_FILE="${TMP_DIR}/load_data_graph_friends_users.csv"
MESSAGES_CLIENTS_FILE="${TMP_DIR}/load_data_graph_messages_clients.csv"
EVENTS_PRODUCTS_FILE="${TMP_DIR}/load_data_graph_events_products.csv"
EVENTS_CATEGORIES_FILE="${TMP_DIR}/load_data_graph_events_categories.csv"
EVENTS_PRODUCT_CATEGORIES_FILE="${TMP_DIR}/load_data_graph_events_product_categories.csv"
MESSAGES_CLIENTS_CHUNK_DIR="${TMP_DIR}/messages_clients_chunks"
EVENTS_CHUNK_DIR="${TMP_DIR}/events_chunks"
MESSAGES_CHUNK_DIR="${TMP_DIR}/messages_chunks"
FRIENDS_CHUNK_DIR="${TMP_DIR}/friends_chunks"

escape_sed_replacement() {
  printf '%s' "$1" | sed -e 's/[&|\\]/\\&/g'
}

is_git_lfs_pointer() {
  local file="$1"
  local head_line
  head_line="$(head -n 1 "$file" 2>/dev/null || true)"
  [[ "$head_line" == "version https://git-lfs.github.com/spec/v1" ]]
}

to_file_uri() {
  local file="$1"
  local abs
  abs="$(cd "$(dirname "$file")" && pwd)/$(basename "$file")"
  abs="${abs//\\//}"
  printf 'file://%s' "$abs"
}

container_is_running() {
  docker ps --format '{{.Names}}' | grep -Fxq "$1"
}

container_exists() {
  docker ps -a --format '{{.Names}}' | grep -Fxq "$1"
}

container_oom_killed() {
  local container_name="$1"
  local oom_flag
  oom_flag="$(docker inspect --format '{{.State.OOMKilled}}' "${container_name}" 2>/dev/null || true)"
  [ "${oom_flag}" = "true" ]
}

is_wsl() {
  grep -qi microsoft /proc/version 2>/dev/null
}

resolve_container_cypher_shell_bin() {
  if docker exec "${NEO4J_CONTAINER}" sh -lc "command -v cypher-shell >/dev/null 2>&1"; then
    printf 'cypher-shell'
    return 0
  fi

  for candidate in /var/lib/neo4j/bin/cypher-shell /opt/neo4j/bin/cypher-shell; do
    if docker exec "${NEO4J_CONTAINER}" sh -lc "[ -x '${candidate}' ]"; then
      printf '%s' "${candidate}"
      return 0
    fi
  done

  return 1
}

is_connection_error() {
  local text="$1"
  [[ "$text" == *"Unable to establish connection"* ]] || \
    [[ "$text" == *"Unable to connect to"* ]] || \
    [[ "$text" == *"Connection refused"* ]] || \
    [[ "$text" == *"Connection reset"* ]] || \
    [[ "$text" == *"ServiceUnavailable"* ]]
}

is_auth_error() {
  local text="$1"
  [[ "$text" == *"unauthorized due to authentication failure"* ]] || \
    [[ "$text" == *"The client is unauthorized"* ]] || \
    [[ "$text" == *"Authentication failed"* ]]
}

run_cypher_command_file() {
  local cypher_file="$1"

  if [ "${CYPHER_EXEC_MODE}" = "container" ]; then
    cat "${cypher_file}" | docker exec -i "${NEO4J_CONTAINER}" "${CONTAINER_CYPHER_SHELL_BIN:-cypher-shell}" \
      -a "${CONTAINER_NEO4J_URI}" \
      -u "${NEO4J_USER}" \
      -p "${NEO4J_PASSWORD}" \
      -d "${NEO4J_DATABASE}"
  else
    cypher-shell \
      -a "${NEO4J_URI}" \
      -u "${NEO4J_USER}" \
      -p "${NEO4J_PASSWORD}" \
      -d "${NEO4J_DATABASE}" \
      -f "${cypher_file}"
  fi
}

run_cypher_file() {
  local stage_name="$1"
  local cypher_file="$2"
  local attempt=1
  local output
  local container_running
  local container_exit_code

  while true; do
    if output="$(run_cypher_command_file "${cypher_file}" 2>&1)"; then
      if [ -n "${output}" ]; then
        printf '%s\n' "${output}"
      fi
      return 0
    fi

    printf '%s\n' "${output}" >&2

    if is_auth_error "${output}"; then
      echo "Error: authentication failed for ${stage_name}. Check NEO4J_USER/NEO4J_PASSWORD for the active Neo4j instance." >&2
      return 1
    fi

    if ! is_connection_error "${output}"; then
      return 1
    fi

    if [ "${attempt}" -ge "${NEO4J_CONNECT_RETRIES}" ]; then
      echo "Error: ${stage_name} failed after ${NEO4J_CONNECT_RETRIES} connection attempts." >&2
      if [ "${LOAD_MODE:-}" = "docker_import" ] && command -v docker >/dev/null 2>&1; then
        echo "Neo4j container logs (last 80 lines):" >&2
        docker logs --tail 80 "${NEO4J_CONTAINER}" >&2 || true
        if docker logs --tail 200 "${NEO4J_CONTAINER}" 2>/dev/null | grep -Eq 'OutOfMemoryError|Java heap space'; then
          echo "Detected Neo4j memory errors in container logs. Restart the container, then retry with smaller loader batch sizes." >&2
        fi
      fi
      if command -v docker >/dev/null 2>&1 && container_exists "${NEO4J_CONTAINER}"; then
        container_running="$(docker inspect --format '{{.State.Running}}' "${NEO4J_CONTAINER}" 2>/dev/null || true)"
        if [ "${container_running}" != "true" ]; then
          container_exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${NEO4J_CONTAINER}" 2>/dev/null || true)"
          echo "Detected Neo4j container '${NEO4J_CONTAINER}' exists but is not running (exit code: ${container_exit_code:-unknown})." >&2
          if container_oom_killed "${NEO4J_CONTAINER}"; then
            echo "Container was OOM-killed. Increase Docker/WSL memory and restart the container before rerunning." >&2
          fi
          echo "Start it with: docker start ${NEO4J_CONTAINER}" >&2
        fi
      fi
      if [ "${LOAD_MODE:-}" = "host" ] && [ "${NEO4J_URI}" = "bolt://localhost:7687" ] && is_wsl; then
        echo "WSL detected. If Neo4j runs on Windows host, try: NEO4J_URI='bolt://host.docker.internal:7687'" >&2
      fi
      return 1
    fi

    echo "Connection to Neo4j is not ready for ${stage_name} (attempt ${attempt}/${NEO4J_CONNECT_RETRIES}). Retrying in ${NEO4J_CONNECT_WAIT_SECONDS}s..." >&2
    sleep "${NEO4J_CONNECT_WAIT_SECONDS}"
    attempt=$((attempt + 1))
  done
}

extract_last_integer_line() {
  local text="$1"
  printf '%s\n' "${text}" | tr -d '\r' | awk '/^[0-9]+$/ {value=$1} END {if (value != "") print value}'
}

write_delete_batch_cypher() {
  local target="$1"
  local batch_size="$2"
  local output_file="$3"

  if [ "${target}" = "relationships" ]; then
    cat > "${output_file}" <<EOF
MATCH ()-[r]-()
WITH r LIMIT ${batch_size}
DELETE r
RETURN count(*) AS deleted;
EOF
    return 0
  fi

  if [ "${target}" = "nodes" ]; then
    cat > "${output_file}" <<EOF
MATCH (n)
WITH n LIMIT ${batch_size}
DELETE n
RETURN count(*) AS deleted;
EOF
    return 0
  fi

  echo "Error: unsupported delete target '${target}'." >&2
  return 1
}

run_batched_delete_stage() {
  local target="$1"
  local batch_size="$2"
  local stage_name="$3"
  local query_file="$4"
  local total_deleted=0
  local batch_count=0
  local output
  local deleted_count

  write_delete_batch_cypher "${target}" "${batch_size}" "${query_file}"

  while true; do
    output="$(run_cypher_file "${stage_name}" "${query_file}")" || return 1
    deleted_count="$(extract_last_integer_line "${output}")"

    if [ -z "${deleted_count}" ]; then
      echo "Error: could not parse Neo4j delete count for ${stage_name}." >&2
      if [ -n "${output}" ]; then
        printf '%s\n' "${output}" >&2
      fi
      return 1
    fi

    if [ "${deleted_count}" -eq 0 ]; then
      echo "  Cleared ${total_deleted} ${target}."
      return 0
    fi

    total_deleted=$((total_deleted + deleted_count))
    batch_count=$((batch_count + 1))

    if [ $((batch_count % 20)) -eq 0 ] || [ "${deleted_count}" -lt "${batch_size}" ]; then
      echo "  Cleared ${total_deleted} ${target} so far..."
    fi
  done
}

reset_graph_data() {
  local rel_delete_file="${TMP_DIR}/load_data_graph_delete_relationships.cypher"
  local node_delete_file="${TMP_DIR}/load_data_graph_delete_nodes.cypher"

  echo "Resetting existing Neo4j data in bounded batches..."
  run_batched_delete_stage "relationships" "${DELETE_REL_BATCH_SIZE}" "delete existing relationships" "${rel_delete_file}"
  run_batched_delete_stage "nodes" "${DELETE_BATCH_SIZE}" "delete existing nodes" "${node_delete_file}"
}

python_cmd() {
  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
  elif command -v python >/dev/null 2>&1; then
    echo "python"
  else
    echo ""
  fi
}

build_messages_users_csv() {
  local python_bin="$1"
  "${python_bin}" - "${MESSAGES_FILE}" "${MESSAGES_USERS_FILE}" <<'PY'
import csv
import sys

source_path, output_path = sys.argv[1], sys.argv[2]
seen = set()

with open(source_path, newline="", encoding="utf-8") as src, open(output_path, "w", newline="", encoding="utf-8") as dst:
    reader = csv.DictReader(src)
    writer = csv.writer(dst)
    writer.writerow(["user_id"])
    for row in reader:
        user_id = (row.get("user_id") or "").strip()
        if user_id and user_id not in seen:
            seen.add(user_id)
            writer.writerow([user_id])
PY
}

build_events_users_csv() {
  local python_bin="$1"
  "${python_bin}" - "${EVENTS_FILE}" "${EVENTS_USERS_FILE}" <<'PY'
import csv
import sys

source_path, output_path = sys.argv[1], sys.argv[2]
seen = set()

with open(source_path, newline="", encoding="utf-8") as src, open(output_path, "w", newline="", encoding="utf-8") as dst:
    reader = csv.DictReader(src)
    writer = csv.writer(dst)
    writer.writerow(["user_id"])
    for row in reader:
        user_id = (row.get("user_id") or "").strip()
        if user_id and user_id not in seen:
            seen.add(user_id)
            writer.writerow([user_id])
PY
}

build_friends_users_csv() {
  local python_bin="$1"
  "${python_bin}" - "${FRIENDS_FILE}" "${FRIENDS_USERS_FILE}" <<'PY'
import csv
import sys

source_path, output_path = sys.argv[1], sys.argv[2]
seen = set()

with open(source_path, newline="", encoding="utf-8") as src, open(output_path, "w", newline="", encoding="utf-8") as dst:
    reader = csv.DictReader(src)
    writer = csv.writer(dst)
    writer.writerow(["user_id"])
    for row in reader:
        for column in ("friend1", "friend2"):
            user_id = (row.get(column) or "").strip()
            if user_id and user_id not in seen:
                seen.add(user_id)
                writer.writerow([user_id])
PY
}

build_messages_clients_csv() {
  local python_bin="$1"
  "${python_bin}" - "${MESSAGES_FILE}" "${MESSAGES_CLIENTS_FILE}" <<'PY'
import csv
import sys

source_path, output_path = sys.argv[1], sys.argv[2]
client_device = {}
pairs = set()

with open(source_path, newline="", encoding="utf-8") as src:
    reader = csv.DictReader(src)
    for row in reader:
        client_id = (row.get("client_id") or "").strip()
        if not client_id:
            continue

        user_id = (row.get("user_id") or "").strip()
        user_device_id = (row.get("user_device_id") or "").strip()

        if user_device_id and client_id not in client_device:
            client_device[client_id] = user_device_id

        pairs.add((client_id, user_id))

with open(output_path, "w", newline="", encoding="utf-8") as dst:
    writer = csv.writer(dst)
    writer.writerow(["client_id", "user_id", "user_device_id"])
    for client_id, user_id in pairs:
        writer.writerow([client_id, user_id, client_device.get(client_id, "")])
PY
}

build_events_dimension_csvs() {
  local python_bin="$1"
  "${python_bin}" - \
    "${EVENTS_FILE}" \
    "${EVENTS_PRODUCTS_FILE}" \
    "${EVENTS_CATEGORIES_FILE}" \
    "${EVENTS_PRODUCT_CATEGORIES_FILE}" <<'PY'
import csv
import sys

source_path, products_path, categories_path, product_categories_path = sys.argv[1:5]
products = {}
categories = {}
product_categories = set()

with open(source_path, newline="", encoding="utf-8") as src:
    reader = csv.DictReader(src)
    for row in reader:
        product_id = (row.get("product_id") or "").strip()
        brand = (row.get("brand") or "").strip()
        category_id = (row.get("category_id") or "").strip()
        category_code = (row.get("category_code") or "").strip()

        if product_id:
            if product_id not in products:
                products[product_id] = brand
            elif not products[product_id] and brand:
                products[product_id] = brand

        if category_id:
            if category_id not in categories:
                categories[category_id] = category_code
            elif not categories[category_id] and category_code:
                categories[category_id] = category_code

        if product_id and category_id:
            product_categories.add((product_id, category_id))

with open(products_path, "w", newline="", encoding="utf-8") as dst:
    writer = csv.writer(dst)
    writer.writerow(["product_id", "brand"])
    for product_id, brand in products.items():
        writer.writerow([product_id, brand])

with open(categories_path, "w", newline="", encoding="utf-8") as dst:
    writer = csv.writer(dst)
    writer.writerow(["category_id", "category_code"])
    for category_id, category_code in categories.items():
        writer.writerow([category_id, category_code])

with open(product_categories_path, "w", newline="", encoding="utf-8") as dst:
    writer = csv.writer(dst)
    writer.writerow(["product_id", "category_id"])
    for product_id, category_id in product_categories:
        writer.writerow([product_id, category_id])
PY
}

split_csv_with_header() {
  local python_bin="$1"
  local source_path="$2"
  local output_dir="$3"
  local chunk_rows="$4"

  mkdir -p "${output_dir}"

  "${python_bin}" - "${source_path}" "${output_dir}" "${chunk_rows}" <<'PY'
import csv
import sys
from pathlib import Path

source_path = Path(sys.argv[1])
output_dir = Path(sys.argv[2])
chunk_rows = int(sys.argv[3])
output_dir.mkdir(parents=True, exist_ok=True)

with source_path.open(newline="", encoding="utf-8") as src:
    reader = csv.reader(src)
    header = next(reader)
    writer = None
    handle = None
    count = 0
    chunk_index = 0

    def open_chunk(index: int):
        path = output_dir / f"{source_path.stem}_{index:05d}.csv"
        dst = path.open("w", newline="", encoding="utf-8")
        chunk_writer = csv.writer(dst)
        chunk_writer.writerow(header)
        return dst, chunk_writer

    for row in reader:
        if writer is None or count >= chunk_rows:
            if handle is not None:
                handle.close()
            chunk_index += 1
            handle, writer = open_chunk(chunk_index)
            count = 0
        writer.writerow(row)
        count += 1

    if handle is not None:
        handle.close()
PY
}

write_runtime_cypher_templates() {
  cat > "${MESSAGE_CLIENTS_STAGE_TEMPLATE}" <<'EOF'
CALL {
  LOAD CSV WITH HEADERS FROM '__MESSAGES_CLIENTS_URI__' AS row
  WITH
    trim(coalesce(row.client_id, '')) AS client_id,
    trim(coalesce(row.user_id, '')) AS user_id,
    trim(coalesce(row.user_device_id, '')) AS user_device_id_raw
  WHERE client_id <> ''
  MERGE (c:Client {client_id: client_id})
  SET c.user_device_id = CASE
      WHEN user_device_id_raw = '' THEN c.user_device_id
      WHEN c.user_device_id IS NULL THEN toInteger(user_device_id_raw)
      ELSE c.user_device_id
    END
  FOREACH (_ IN CASE WHEN user_id <> '' THEN [1] ELSE [] END |
    MERGE (u:User {user_id: user_id})
    ON CREATE SET u.has_first_purchase = false
    MERGE (u)-[:HAS_CLIENT]->(c)
  )
} IN TRANSACTIONS OF __MESSAGE_CLIENT_BATCH_SIZE__ ROWS;
EOF

  cat > "${EVENTS_STAGE_TEMPLATE}" <<'EOF'
CALL {
  LOAD CSV WITH HEADERS FROM '__EVENTS_URI__' AS row
  WITH
    trim(coalesce(row.user_id, '')) AS user_id,
    trim(coalesce(row.product_id, '')) AS product_id,
    trim(coalesce(row.event_type, '')) AS event_type,
    trim(coalesce(row.user_session, '')) AS session_id,
    trim(coalesce(row.event_time, '')) AS event_time_raw,
    trim(coalesce(row.price, '')) AS price_raw
  WHERE user_id <> '' AND product_id <> ''
  MATCH (u:User {user_id: user_id})
  MATCH (p:Product {product_id: product_id})
  WITH u, p, toLower(event_type) AS event_type, session_id, event_time_raw, price_raw
  FOREACH (_ IN CASE WHEN event_type = 'view' THEN [1] ELSE [] END |
    CREATE (u)-[:VIEWED {
      session_id: CASE WHEN session_id = '' THEN null ELSE session_id END,
      event_time: CASE WHEN event_time_raw = '' THEN null ELSE datetime(replace(event_time_raw, ' ', 'T') + 'Z') END,
      price: CASE WHEN price_raw = '' THEN null ELSE toFloat(price_raw) END
    }]->(p)
  )
  FOREACH (_ IN CASE WHEN event_type = 'cart' THEN [1] ELSE [] END |
    CREATE (u)-[:CARTED {
      session_id: CASE WHEN session_id = '' THEN null ELSE session_id END,
      event_time: CASE WHEN event_time_raw = '' THEN null ELSE datetime(replace(event_time_raw, ' ', 'T') + 'Z') END,
      price: CASE WHEN price_raw = '' THEN null ELSE toFloat(price_raw) END
    }]->(p)
  )
  FOREACH (_ IN CASE WHEN event_type = 'purchase' THEN [1] ELSE [] END |
    CREATE (u)-[:PURCHASED {
      session_id: CASE WHEN session_id = '' THEN null ELSE session_id END,
      event_time: CASE WHEN event_time_raw = '' THEN null ELSE datetime(replace(event_time_raw, ' ', 'T') + 'Z') END,
      price: CASE WHEN price_raw = '' THEN null ELSE toFloat(price_raw) END
    }]->(p)
  )
} IN TRANSACTIONS OF __EVENT_BATCH_SIZE__ ROWS;
EOF

  cat > "${MESSAGES_STAGE_TEMPLATE}" <<'EOF'
CALL {
  LOAD CSV WITH HEADERS FROM '__MESSAGES_URI__' AS row
  WITH
    trim(coalesce(row.message_id, '')) AS message_id,
    trim(coalesce(row.id, '')) AS row_id,
    trim(coalesce(row.message_type, '')) AS message_type,
    trim(coalesce(row.channel, '')) AS channel,
    trim(coalesce(row.category, '')) AS category,
    trim(coalesce(row.platform, '')) AS platform,
    trim(coalesce(row.email_provider, '')) AS email_provider,
    trim(coalesce(row.stream, '')) AS stream,
    trim(coalesce(row.date, '')) AS date_raw,
    trim(coalesce(row.sent_at, '')) AS sent_at_raw,
    trim(coalesce(row.created_at, '')) AS created_at_raw,
    trim(coalesce(row.updated_at, '')) AS updated_at_raw,
    trim(coalesce(row.is_opened, '')) AS is_opened_raw,
    trim(coalesce(row.opened_first_time_at, '')) AS opened_first_time_at_raw,
    trim(coalesce(row.opened_last_time_at, '')) AS opened_last_time_at_raw,
    trim(coalesce(row.is_clicked, '')) AS is_clicked_raw,
    trim(coalesce(row.clicked_first_time_at, '')) AS clicked_first_time_at_raw,
    trim(coalesce(row.clicked_last_time_at, '')) AS clicked_last_time_at_raw,
    trim(coalesce(row.is_unsubscribed, '')) AS is_unsubscribed_raw,
    trim(coalesce(row.unsubscribed_at, '')) AS unsubscribed_at_raw,
    trim(coalesce(row.is_hard_bounced, '')) AS is_hard_bounced_raw,
    trim(coalesce(row.hard_bounced_at, '')) AS hard_bounced_at_raw,
    trim(coalesce(row.is_soft_bounced, '')) AS is_soft_bounced_raw,
    trim(coalesce(row.soft_bounced_at, '')) AS soft_bounced_at_raw,
    trim(coalesce(row.is_complained, '')) AS is_complained_raw,
    trim(coalesce(row.complained_at, '')) AS complained_at_raw,
    trim(coalesce(row.is_blocked, '')) AS is_blocked_raw,
    trim(coalesce(row.blocked_at, '')) AS blocked_at_raw,
    trim(coalesce(row.is_purchased, '')) AS is_purchased_raw,
    trim(coalesce(row.purchased_at, '')) AS purchased_at_raw,
    trim(coalesce(row.client_id, '')) AS client_id,
    trim(coalesce(row.campaign_id, '')) AS campaign_id_raw
  WHERE message_id <> '' AND client_id <> '' AND campaign_id_raw <> '' AND message_type <> ''
  MERGE (m:Message {message_id: message_id})
  SET m.row_id = CASE WHEN row_id = '' THEN m.row_id ELSE row_id END,
      m.message_type = message_type,
      m.channel = CASE WHEN channel = '' THEN null ELSE channel END,
      m.category = CASE WHEN category = '' THEN null ELSE category END,
      m.platform = CASE WHEN platform = '' THEN null ELSE platform END,
      m.email_provider = CASE WHEN email_provider = '' THEN null ELSE email_provider END,
      m.stream = CASE WHEN stream = '' THEN null ELSE stream END,
      m.date = CASE WHEN date_raw = '' THEN null ELSE date(date_raw) END,
      m.sent_at = CASE WHEN sent_at_raw = '' THEN null ELSE datetime(replace(sent_at_raw, ' ', 'T') + 'Z') END,
      m.created_at = CASE WHEN created_at_raw = '' THEN null ELSE datetime(replace(created_at_raw, ' ', 'T') + 'Z') END,
      m.updated_at = CASE WHEN updated_at_raw = '' THEN null ELSE datetime(replace(updated_at_raw, ' ', 'T') + 'Z') END,
      m.is_opened = CASE
        WHEN toLower(is_opened_raw) IN ['1','t','true','yes','y'] THEN true
        WHEN toLower(is_opened_raw) IN ['0','f','false','no','n'] THEN false
        ELSE null
      END,
      m.opened_first_time_at = CASE WHEN opened_first_time_at_raw = '' THEN null ELSE datetime(replace(opened_first_time_at_raw, ' ', 'T') + 'Z') END,
      m.opened_last_time_at = CASE WHEN opened_last_time_at_raw = '' THEN null ELSE datetime(replace(opened_last_time_at_raw, ' ', 'T') + 'Z') END,
      m.is_clicked = CASE
        WHEN toLower(is_clicked_raw) IN ['1','t','true','yes','y'] THEN true
        WHEN toLower(is_clicked_raw) IN ['0','f','false','no','n'] THEN false
        ELSE null
      END,
      m.clicked_first_time_at = CASE WHEN clicked_first_time_at_raw = '' THEN null ELSE datetime(replace(clicked_first_time_at_raw, ' ', 'T') + 'Z') END,
      m.clicked_last_time_at = CASE WHEN clicked_last_time_at_raw = '' THEN null ELSE datetime(replace(clicked_last_time_at_raw, ' ', 'T') + 'Z') END,
      m.is_unsubscribed = CASE
        WHEN toLower(is_unsubscribed_raw) IN ['1','t','true','yes','y'] THEN true
        WHEN toLower(is_unsubscribed_raw) IN ['0','f','false','no','n'] THEN false
        ELSE null
      END,
      m.unsubscribed_at = CASE WHEN unsubscribed_at_raw = '' THEN null ELSE datetime(replace(unsubscribed_at_raw, ' ', 'T') + 'Z') END,
      m.is_hard_bounced = CASE
        WHEN toLower(is_hard_bounced_raw) IN ['1','t','true','yes','y'] THEN true
        WHEN toLower(is_hard_bounced_raw) IN ['0','f','false','no','n'] THEN false
        ELSE null
      END,
      m.hard_bounced_at = CASE WHEN hard_bounced_at_raw = '' THEN null ELSE datetime(replace(hard_bounced_at_raw, ' ', 'T') + 'Z') END,
      m.is_soft_bounced = CASE
        WHEN toLower(is_soft_bounced_raw) IN ['1','t','true','yes','y'] THEN true
        WHEN toLower(is_soft_bounced_raw) IN ['0','f','false','no','n'] THEN false
        ELSE null
      END,
      m.soft_bounced_at = CASE WHEN soft_bounced_at_raw = '' THEN null ELSE datetime(replace(soft_bounced_at_raw, ' ', 'T') + 'Z') END,
      m.is_complained = CASE
        WHEN toLower(is_complained_raw) IN ['1','t','true','yes','y'] THEN true
        WHEN toLower(is_complained_raw) IN ['0','f','false','no','n'] THEN false
        ELSE null
      END,
      m.complained_at = CASE WHEN complained_at_raw = '' THEN null ELSE datetime(replace(complained_at_raw, ' ', 'T') + 'Z') END,
      m.is_blocked = CASE
        WHEN toLower(is_blocked_raw) IN ['1','t','true','yes','y'] THEN true
        WHEN toLower(is_blocked_raw) IN ['0','f','false','no','n'] THEN false
        ELSE null
      END,
      m.blocked_at = CASE WHEN blocked_at_raw = '' THEN null ELSE datetime(replace(blocked_at_raw, ' ', 'T') + 'Z') END,
      m.is_purchased = CASE
        WHEN toLower(is_purchased_raw) IN ['1','t','true','yes','y'] THEN true
        WHEN toLower(is_purchased_raw) IN ['0','f','false','no','n'] THEN false
        ELSE null
      END,
      m.purchased_at = CASE WHEN purchased_at_raw = '' THEN null ELSE datetime(replace(purchased_at_raw, ' ', 'T') + 'Z') END
  MERGE (c:Client {client_id: client_id})
  MERGE (c)-[:RECEIVED]->(m)
  WITH m, campaign_id_raw, message_type
  MERGE (camp:Campaign {campaign_key: campaign_id_raw + '|' + message_type})
  ON CREATE SET camp.campaign_id = toInteger(campaign_id_raw),
                camp.campaign_type = message_type
  MERGE (m)-[:BELONGS_TO]->(camp)
} IN TRANSACTIONS OF __MESSAGE_BATCH_SIZE__ ROWS;
EOF

  cat > "${FRIENDS_STAGE_TEMPLATE}" <<'EOF'
CALL {
  LOAD CSV WITH HEADERS FROM '__FRIENDS_URI__' AS row
  WITH
    trim(coalesce(row.friend1, '')) AS friend1,
    trim(coalesce(row.friend2, '')) AS friend2
  WHERE friend1 <> '' AND friend2 <> '' AND friend1 <> friend2
  MATCH (u1:User {user_id: friend1})
  MATCH (u2:User {user_id: friend2})
  MERGE (u1)-[:FRIEND_OF]->(u2)
} IN TRANSACTIONS OF __FRIEND_BATCH_SIZE__ ROWS;
EOF

  cat > "${COUNTS_TEMPLATE}" <<'EOF'
MATCH (u:User) RETURN 'User' AS label, count(u) AS count
UNION ALL
MATCH (c:Client) RETURN 'Client' AS label, count(c) AS count
UNION ALL
MATCH (ca:Campaign) RETURN 'Campaign' AS label, count(ca) AS count
UNION ALL
MATCH (m:Message) RETURN 'Message' AS label, count(m) AS count
UNION ALL
MATCH (p:Product) RETURN 'Product' AS label, count(p) AS count
UNION ALL
MATCH (cat:Category) RETURN 'Category' AS label, count(cat) AS count;
EOF
}

if [ ! -f "${CYPHER_TEMPLATE}" ]; then
  echo "Error: Cypher template not found: ${CYPHER_TEMPLATE}"
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
  if is_git_lfs_pointer "${f}"; then
    echo "Error: ${f} is a Git LFS pointer, not dataset contents. Run: git lfs pull"
    exit 1
  fi
done

PYTHON_BIN="$(python_cmd)"
if [ -z "${PYTHON_BIN}" ]; then
  echo "Error: python3 or python is required to build compact helper CSVs."
  exit 1
fi

LOAD_MODE="${NEO4J_LOAD_MODE}"
if [ "${LOAD_MODE}" = "auto" ]; then
  if command -v docker >/dev/null 2>&1 && container_is_running "${NEO4J_CONTAINER}"; then
    LOAD_MODE="docker_import"
  else
    LOAD_MODE="host"
  fi
fi

CYPHER_EXEC_MODE="${NEO4J_CYPHER_EXEC_MODE}"
if [ "${CYPHER_EXEC_MODE}" = "auto" ]; then
  if [ "${LOAD_MODE}" = "docker_import" ]; then
    CYPHER_EXEC_MODE="container"
  else
    CYPHER_EXEC_MODE="host"
  fi
fi

if [ "${LOAD_MODE}" = "docker_import" ]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "Error: docker is required for NEO4J_LOAD_MODE=docker_import."
    exit 1
  fi
  if ! container_is_running "${NEO4J_CONTAINER}"; then
    echo "Error: Neo4j container is not running: ${NEO4J_CONTAINER}"
    exit 1
  fi
  if [ "${CYPHER_EXEC_MODE}" = "container" ]; then
    if ! CONTAINER_CYPHER_SHELL_BIN="$(resolve_container_cypher_shell_bin)"; then
      echo "Error: cypher-shell not found inside container '${NEO4J_CONTAINER}'."
      exit 1
    fi
  fi
elif [ "${LOAD_MODE}" = "host" ]; then
  if [ "${CYPHER_EXEC_MODE}" = "container" ]; then
    echo "Error: NEO4J_CYPHER_EXEC_MODE=container requires NEO4J_LOAD_MODE=docker_import."
    exit 1
  fi
  if ! command -v cypher-shell >/dev/null 2>&1; then
    echo "Error: cypher-shell is not installed or not in PATH."
    exit 1
  fi
fi

if [ "${CYPHER_EXEC_MODE}" != "host" ] && [ "${CYPHER_EXEC_MODE}" != "container" ]; then
  echo "Error: invalid NEO4J_CYPHER_EXEC_MODE '${NEO4J_CYPHER_EXEC_MODE}'. Use: auto, host, container"
  exit 1
fi

TMP_PING_CYPHER="${TMP_DIR}/load_data_graph_ping.cypher"
printf 'RETURN 1 AS ok;\n' > "${TMP_PING_CYPHER}"
echo "Checking Neo4j connection before preparing helper CSVs..."
if ! run_cypher_file "connection preflight" "${TMP_PING_CYPHER}"; then
  echo "Connection preflight failed. Fix connectivity first, then rerun the loader."
  exit 1
fi

write_runtime_cypher_templates

build_messages_users_csv "${PYTHON_BIN}"
build_events_users_csv "${PYTHON_BIN}"
build_friends_users_csv "${PYTHON_BIN}"
build_messages_clients_csv "${PYTHON_BIN}"
build_events_dimension_csvs "${PYTHON_BIN}"
split_csv_with_header "${PYTHON_BIN}" "${MESSAGES_CLIENTS_FILE}" "${MESSAGES_CLIENTS_CHUNK_DIR}" "${MESSAGE_CLIENT_CHUNK_ROWS}"
split_csv_with_header "${PYTHON_BIN}" "${EVENTS_FILE}" "${EVENTS_CHUNK_DIR}" "${EVENT_CHUNK_ROWS}"
split_csv_with_header "${PYTHON_BIN}" "${MESSAGES_FILE}" "${MESSAGES_CHUNK_DIR}" "${MESSAGE_CHUNK_ROWS}"
split_csv_with_header "${PYTHON_BIN}" "${FRIENDS_FILE}" "${FRIENDS_CHUNK_DIR}" "${FRIEND_CHUNK_ROWS}"

if [ "${LOAD_MODE}" = "docker_import" ]; then
  DOCKER_CAMPAIGNS_NAME="load_data_graph_campaigns.csv"
  DOCKER_CLIENTS_NAME="load_data_graph_client_first_purchase_date.csv"
  DOCKER_EVENTS_NAME="load_data_graph_events.csv"
  DOCKER_MESSAGES_NAME="load_data_graph_messages.csv"
  DOCKER_FRIENDS_NAME="load_data_graph_friends.csv"
  DOCKER_MESSAGES_USERS_NAME="load_data_graph_messages_users.csv"
  DOCKER_FRIENDS_USERS_NAME="load_data_graph_friends_users.csv"
  DOCKER_MESSAGES_CLIENTS_NAME="load_data_graph_messages_clients.csv"
  DOCKER_EVENTS_USERS_NAME="load_data_graph_events_users.csv"
  DOCKER_EVENTS_PRODUCTS_NAME="load_data_graph_events_products.csv"
  DOCKER_EVENTS_CATEGORIES_NAME="load_data_graph_events_categories.csv"
  DOCKER_EVENTS_PRODUCT_CATEGORIES_NAME="load_data_graph_events_product_categories.csv"

  docker cp "${CAMPAIGNS_FILE}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${DOCKER_CAMPAIGNS_NAME}"
  docker cp "${CLIENTS_FILE}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${DOCKER_CLIENTS_NAME}"
  docker cp "${EVENTS_FILE}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${DOCKER_EVENTS_NAME}"
  docker cp "${MESSAGES_FILE}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${DOCKER_MESSAGES_NAME}"
  docker cp "${FRIENDS_FILE}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${DOCKER_FRIENDS_NAME}"
  docker cp "${MESSAGES_USERS_FILE}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${DOCKER_MESSAGES_USERS_NAME}"
  docker cp "${FRIENDS_USERS_FILE}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${DOCKER_FRIENDS_USERS_NAME}"
  docker cp "${MESSAGES_CLIENTS_FILE}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${DOCKER_MESSAGES_CLIENTS_NAME}"
  docker cp "${EVENTS_USERS_FILE}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${DOCKER_EVENTS_USERS_NAME}"
  docker cp "${EVENTS_PRODUCTS_FILE}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${DOCKER_EVENTS_PRODUCTS_NAME}"
  docker cp "${EVENTS_CATEGORIES_FILE}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${DOCKER_EVENTS_CATEGORIES_NAME}"
  docker cp "${EVENTS_PRODUCT_CATEGORIES_FILE}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${DOCKER_EVENTS_PRODUCT_CATEGORIES_NAME}"

  CAMPAIGNS_URI="file:///${DOCKER_CAMPAIGNS_NAME}"
  CLIENTS_URI="file:///${DOCKER_CLIENTS_NAME}"
  EVENTS_URI="file:///${DOCKER_EVENTS_NAME}"
  MESSAGES_URI="file:///${DOCKER_MESSAGES_NAME}"
  FRIENDS_URI="file:///${DOCKER_FRIENDS_NAME}"
  MESSAGES_USERS_URI="file:///${DOCKER_MESSAGES_USERS_NAME}"
  FRIENDS_USERS_URI="file:///${DOCKER_FRIENDS_USERS_NAME}"
  MESSAGES_CLIENTS_URI="file:///${DOCKER_MESSAGES_CLIENTS_NAME}"
  EVENTS_USERS_URI="file:///${DOCKER_EVENTS_USERS_NAME}"
  EVENTS_PRODUCTS_URI="file:///${DOCKER_EVENTS_PRODUCTS_NAME}"
  EVENTS_CATEGORIES_URI="file:///${DOCKER_EVENTS_CATEGORIES_NAME}"
  EVENTS_PRODUCT_CATEGORIES_URI="file:///${DOCKER_EVENTS_PRODUCT_CATEGORIES_NAME}"
elif [ "${LOAD_MODE}" = "host" ]; then
  CAMPAIGNS_URI="$(to_file_uri "${CAMPAIGNS_FILE}")"
  CLIENTS_URI="$(to_file_uri "${CLIENTS_FILE}")"
  EVENTS_URI="$(to_file_uri "${EVENTS_FILE}")"
  MESSAGES_URI="$(to_file_uri "${MESSAGES_FILE}")"
  FRIENDS_URI="$(to_file_uri "${FRIENDS_FILE}")"
  MESSAGES_USERS_URI="$(to_file_uri "${MESSAGES_USERS_FILE}")"
  FRIENDS_USERS_URI="$(to_file_uri "${FRIENDS_USERS_FILE}")"
  MESSAGES_CLIENTS_URI="$(to_file_uri "${MESSAGES_CLIENTS_FILE}")"
  EVENTS_USERS_URI="$(to_file_uri "${EVENTS_USERS_FILE}")"
  EVENTS_PRODUCTS_URI="$(to_file_uri "${EVENTS_PRODUCTS_FILE}")"
  EVENTS_CATEGORIES_URI="$(to_file_uri "${EVENTS_CATEGORIES_FILE}")"
  EVENTS_PRODUCT_CATEGORIES_URI="$(to_file_uri "${EVENTS_PRODUCT_CATEGORIES_FILE}")"
else
  echo "Error: invalid NEO4J_LOAD_MODE '${NEO4J_LOAD_MODE}'. Use: auto, host, docker_import"
  exit 1
fi

echo "Loading Neo4j model and data..."
echo "  URI: ${NEO4J_URI}"
echo "  User: ${NEO4J_USER}"
echo "  Database: ${NEO4J_DATABASE}"
echo "  Batch size: ${BATCH_SIZE}"
echo "  Cypher exec mode: ${CYPHER_EXEC_MODE}"
echo "  Connect retries: ${NEO4J_CONNECT_RETRIES}"
echo "  Connect wait seconds: ${NEO4J_CONNECT_WAIT_SECONDS}"
echo "  Delete relationship batch size: ${DELETE_REL_BATCH_SIZE}"
echo "  Delete batch size: ${DELETE_BATCH_SIZE}"
echo "  Client batch size: ${CLIENT_BATCH_SIZE}"
echo "  Campaign batch size: ${CAMPAIGN_BATCH_SIZE}"
echo "  Event batch size: ${EVENT_BATCH_SIZE}"
echo "  Event chunk rows: ${EVENT_CHUNK_ROWS}"
echo "  Message client batch size: ${MESSAGE_CLIENT_BATCH_SIZE}"
echo "  Message client chunk rows: ${MESSAGE_CLIENT_CHUNK_ROWS}"
echo "  Message batch size: ${MESSAGE_BATCH_SIZE}"
echo "  Message chunk rows: ${MESSAGE_CHUNK_ROWS}"
echo "  Friend batch size: ${FRIEND_BATCH_SIZE}"
echo "  Friend chunk rows: ${FRIEND_CHUNK_ROWS}"
echo "  User event batch size: ${USER_EVENT_BATCH_SIZE}"
echo "  User message batch size: ${USER_MESSAGE_BATCH_SIZE}"
echo "  User friend batch size: ${USER_FRIEND_BATCH_SIZE}"
echo "  Load mode: ${LOAD_MODE}"
if [ "${LOAD_MODE}" = "docker_import" ]; then
  echo "  Docker container: ${NEO4J_CONTAINER}"
  echo "  Docker import dir: ${DOCKER_IMPORT_DIR}"
  if [ "${CYPHER_EXEC_MODE}" = "container" ]; then
    echo "  Container Bolt URI: ${CONTAINER_NEO4J_URI}"
  fi
fi
echo "  campaigns.csv URI: ${CAMPAIGNS_URI}"
echo "  client_first_purchase_date.csv URI: ${CLIENTS_URI}"
echo "  events.csv URI: ${EVENTS_URI}"
echo "  messages.csv URI: ${MESSAGES_URI}"
echo "  friends.csv URI: ${FRIENDS_URI}"
echo "  messages_users.csv URI: ${MESSAGES_USERS_URI}"
echo "  friends_users.csv URI: ${FRIENDS_USERS_URI}"
echo "  messages_clients.csv URI: ${MESSAGES_CLIENTS_URI}"
echo "  events_users.csv URI: ${EVENTS_USERS_URI}"
echo "  events_products.csv URI: ${EVENTS_PRODUCTS_URI}"
echo "  events_categories.csv URI: ${EVENTS_CATEGORIES_URI}"
echo "  events_product_categories.csv URI: ${EVENTS_PRODUCT_CATEGORIES_URI}"

if ! reset_graph_data; then
  echo "Database reset failed. If Neo4j still reports a memory pool error, lower DELETE_REL_BATCH_SIZE and DELETE_BATCH_SIZE."
  exit 1
fi

TMP_CYPHER="${TMP_DIR}/load_data_graph.cypher"
TMP_COUNTS_CYPHER="${TMP_DIR}/load_data_graph_counts_rendered.cypher"
TMP_MESSAGES_CLIENTS_STAGE_CYPHER="${TMP_DIR}/load_data_graph_messages_clients_rendered.cypher"
TMP_EVENTS_STAGE_CYPHER="${TMP_DIR}/load_data_graph_events_rendered.cypher"
TMP_MESSAGES_STAGE_CYPHER="${TMP_DIR}/load_data_graph_messages_rendered.cypher"
TMP_FRIENDS_STAGE_CYPHER="${TMP_DIR}/load_data_graph_friends_rendered.cypher"

sed \
  -e "s|__CAMPAIGNS_URI__|$(escape_sed_replacement "${CAMPAIGNS_URI}")|g" \
  -e "s|__CLIENTS_URI__|$(escape_sed_replacement "${CLIENTS_URI}")|g" \
  -e "s|__EVENTS_URI__|$(escape_sed_replacement "${EVENTS_URI}")|g" \
  -e "s|__MESSAGES_URI__|$(escape_sed_replacement "${MESSAGES_URI}")|g" \
  -e "s|__FRIENDS_URI__|$(escape_sed_replacement "${FRIENDS_URI}")|g" \
  -e "s|__MESSAGES_USERS_URI__|$(escape_sed_replacement "${MESSAGES_USERS_URI}")|g" \
  -e "s|__FRIENDS_USERS_URI__|$(escape_sed_replacement "${FRIENDS_USERS_URI}")|g" \
  -e "s|__MESSAGES_CLIENTS_URI__|$(escape_sed_replacement "${MESSAGES_CLIENTS_URI}")|g" \
  -e "s|__EVENTS_USERS_URI__|$(escape_sed_replacement "${EVENTS_USERS_URI}")|g" \
  -e "s|__EVENTS_PRODUCTS_URI__|$(escape_sed_replacement "${EVENTS_PRODUCTS_URI}")|g" \
  -e "s|__EVENTS_CATEGORIES_URI__|$(escape_sed_replacement "${EVENTS_CATEGORIES_URI}")|g" \
  -e "s|__EVENTS_PRODUCT_CATEGORIES_URI__|$(escape_sed_replacement "${EVENTS_PRODUCT_CATEGORIES_URI}")|g" \
  -e "s|__CLIENT_BATCH_SIZE__|$(escape_sed_replacement "${CLIENT_BATCH_SIZE}")|g" \
  -e "s|__DELETE_REL_BATCH_SIZE__|$(escape_sed_replacement "${DELETE_REL_BATCH_SIZE}")|g" \
  -e "s|__DELETE_BATCH_SIZE__|$(escape_sed_replacement "${DELETE_BATCH_SIZE}")|g" \
  -e "s|__CAMPAIGN_BATCH_SIZE__|$(escape_sed_replacement "${CAMPAIGN_BATCH_SIZE}")|g" \
  -e "s|__EVENT_BATCH_SIZE__|$(escape_sed_replacement "${EVENT_BATCH_SIZE}")|g" \
  -e "s|__MESSAGE_CLIENT_BATCH_SIZE__|$(escape_sed_replacement "${MESSAGE_CLIENT_BATCH_SIZE}")|g" \
  -e "s|__MESSAGE_BATCH_SIZE__|$(escape_sed_replacement "${MESSAGE_BATCH_SIZE}")|g" \
  -e "s|__FRIEND_BATCH_SIZE__|$(escape_sed_replacement "${FRIEND_BATCH_SIZE}")|g" \
  -e "s|__USER_EVENT_BATCH_SIZE__|$(escape_sed_replacement "${USER_EVENT_BATCH_SIZE}")|g" \
  -e "s|__USER_MESSAGE_BATCH_SIZE__|$(escape_sed_replacement "${USER_MESSAGE_BATCH_SIZE}")|g" \
  -e "s|__USER_FRIEND_BATCH_SIZE__|$(escape_sed_replacement "${USER_FRIEND_BATCH_SIZE}")|g" \
  "${CYPHER_TEMPLATE}" > "${TMP_CYPHER}"

cp "${COUNTS_TEMPLATE}" "${TMP_COUNTS_CYPHER}"

echo "Running schema + bootstrap stages..."
if ! run_cypher_file "schema + bootstrap stages" "${TMP_CYPHER}"; then
  echo "Initial stages failed after the database reset completed."
  exit 1
fi

for chunk_path in "${MESSAGES_CLIENTS_CHUNK_DIR}"/*.csv; do
  [ -e "${chunk_path}" ] || continue

  if [ "${LOAD_MODE}" = "docker_import" ]; then
    chunk_name="$(basename "${chunk_path}")"
    docker cp "${chunk_path}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${chunk_name}"
    chunk_uri="file:///${chunk_name}"
  else
    chunk_uri="$(to_file_uri "${chunk_path}")"
  fi

  sed \
    -e "s|__MESSAGES_CLIENTS_URI__|$(escape_sed_replacement "${chunk_uri}")|g" \
    -e "s|__MESSAGE_CLIENT_BATCH_SIZE__|$(escape_sed_replacement "${MESSAGE_CLIENT_BATCH_SIZE}")|g" \
    "${MESSAGE_CLIENTS_STAGE_TEMPLATE}" > "${TMP_MESSAGES_CLIENTS_STAGE_CYPHER}"

  run_cypher_file "messages_clients chunk load" "${TMP_MESSAGES_CLIENTS_STAGE_CYPHER}"
done

for chunk_path in "${EVENTS_CHUNK_DIR}"/*.csv; do
  [ -e "${chunk_path}" ] || continue

  if [ "${LOAD_MODE}" = "docker_import" ]; then
    chunk_name="$(basename "${chunk_path}")"
    docker cp "${chunk_path}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${chunk_name}"
    chunk_uri="file:///${chunk_name}"
  else
    chunk_uri="$(to_file_uri "${chunk_path}")"
  fi

  sed \
    -e "s|__EVENTS_URI__|$(escape_sed_replacement "${chunk_uri}")|g" \
    -e "s|__EVENT_BATCH_SIZE__|$(escape_sed_replacement "${EVENT_BATCH_SIZE}")|g" \
    "${EVENTS_STAGE_TEMPLATE}" > "${TMP_EVENTS_STAGE_CYPHER}"

  run_cypher_file "events chunk load" "${TMP_EVENTS_STAGE_CYPHER}"
done

for chunk_path in "${MESSAGES_CHUNK_DIR}"/*.csv; do
  [ -e "${chunk_path}" ] || continue

  if [ "${LOAD_MODE}" = "docker_import" ]; then
    chunk_name="$(basename "${chunk_path}")"
    docker cp "${chunk_path}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${chunk_name}"
    chunk_uri="file:///${chunk_name}"
  else
    chunk_uri="$(to_file_uri "${chunk_path}")"
  fi

  sed \
    -e "s|__MESSAGES_URI__|$(escape_sed_replacement "${chunk_uri}")|g" \
    -e "s|__MESSAGE_BATCH_SIZE__|$(escape_sed_replacement "${MESSAGE_BATCH_SIZE}")|g" \
    "${MESSAGES_STAGE_TEMPLATE}" > "${TMP_MESSAGES_STAGE_CYPHER}"

  run_cypher_file "messages chunk load" "${TMP_MESSAGES_STAGE_CYPHER}"
done

for chunk_path in "${FRIENDS_CHUNK_DIR}"/*.csv; do
  [ -e "${chunk_path}" ] || continue

  if [ "${LOAD_MODE}" = "docker_import" ]; then
    chunk_name="$(basename "${chunk_path}")"
    docker cp "${chunk_path}" "${NEO4J_CONTAINER}:${DOCKER_IMPORT_DIR}/${chunk_name}"
    chunk_uri="file:///${chunk_name}"
  else
    chunk_uri="$(to_file_uri "${chunk_path}")"
  fi

  sed \
    -e "s|__FRIENDS_URI__|$(escape_sed_replacement "${chunk_uri}")|g" \
    -e "s|__FRIEND_BATCH_SIZE__|$(escape_sed_replacement "${FRIEND_BATCH_SIZE}")|g" \
    "${FRIENDS_STAGE_TEMPLATE}" > "${TMP_FRIENDS_STAGE_CYPHER}"

  run_cypher_file "friends chunk load" "${TMP_FRIENDS_STAGE_CYPHER}"
done

run_cypher_file "final counts" "${TMP_COUNTS_CYPHER}"

echo "Done."
