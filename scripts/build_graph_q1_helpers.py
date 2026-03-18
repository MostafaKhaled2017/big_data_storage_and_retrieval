#!/usr/bin/env python3
"""Build compact helper relationships used by graph q1 analysis."""

from __future__ import annotations

import os
import time
from typing import Any

from neo4j import GraphDatabase, Query


def log(message: str) -> None:
    print(message, flush=True)


def run_query(
    session: Any,
    query_text: str,
    params: dict[str, Any] | None = None,
    query_timeout_ms: int | None = None,
) -> list[dict[str, Any]]:
    query: str | Query = query_text
    if query_timeout_ms and query_timeout_ms > 0:
        query = Query(query_text, timeout=query_timeout_ms / 1000.0)
    return [record.data() for record in session.run(query, params or {})]


def consume_query(
    session: Any,
    query_text: str,
    params: dict[str, Any] | None = None,
    query_timeout_ms: int | None = None,
) -> None:
    query: str | Query = query_text
    if query_timeout_ms and query_timeout_ms > 0:
        query = Query(query_text, timeout=query_timeout_ms / 1000.0)
    session.run(query, params or {}).consume()


def main() -> None:
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "neo4jneo4j")
    database = os.getenv("NEO4J_DATABASE", "neo4j")
    query_timeout_ms = int(os.getenv("NEO4J_QUERY_TIMEOUT_MS", "0"))
    message_id_step = int(os.getenv("Q1_HELPER_MESSAGE_ID_STEP", "5000"))

    bounds_query = """
    MATCH (m:Message)
    RETURN min(id(m)) AS min_message_id, max(id(m)) AS max_message_id, count(m) AS message_count
    """

    chunk_build_query = """
    MATCH (u:User)-[:HAS_CLIENT]->(:Client)-[:RECEIVED]->(m:Message)-[:BELONGS_TO]->(c:Campaign)
    WHERE id(m) >= $start_id AND id(m) < $end_id
    MERGE (u)-[:RECEIVED_CAMPAIGN]->(c)
    FOREACH (_ IN CASE WHEN coalesce(m.is_purchased, false) THEN [1] ELSE [] END |
      MERGE (u)-[:PURCHASED_CAMPAIGN]->(c)
    )
    """

    counts_query = """
    CALL () {
      MATCH ()-[r:RECEIVED_CAMPAIGN]->()
      RETURN count(r) AS received_helper_count
    }
    CALL () {
      MATCH ()-[r:PURCHASED_CAMPAIGN]->()
      RETURN count(r) AS purchased_helper_count
    }
    RETURN received_helper_count, purchased_helper_count
    """

    log("Building q1 graph helpers")
    log(f"DB: {database} @ {uri} as {user}")
    log(f"Message id step: {message_id_step}")
    if query_timeout_ms > 0:
        log(f"Query timeout: {query_timeout_ms} ms")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        log("Checking graph connectivity...")
        driver.verify_connectivity()
        with driver.session(database=database) as session:
            bounds = run_query(session, bounds_query, query_timeout_ms=query_timeout_ms)[0]
            min_message_id = bounds["min_message_id"]
            max_message_id = bounds["max_message_id"]
            message_count = bounds["message_count"]

            if min_message_id is None or max_message_id is None:
                log("No messages found. Nothing to build.")
                return

            total_chunks = ((max_message_id - min_message_id) // message_id_step) + 1
            log(
                f"Messages: {message_count} "
                f"(id range {min_message_id}..{max_message_id}, {total_chunks} chunks)"
            )

            overall_started = time.perf_counter()
            chunk_number = 0
            for start_id in range(min_message_id, max_message_id + 1, message_id_step):
                end_id = start_id + message_id_step
                chunk_number += 1
                chunk_started = time.perf_counter()
                consume_query(
                    session,
                    chunk_build_query,
                    {"start_id": start_id, "end_id": end_id},
                    query_timeout_ms=query_timeout_ms,
                )
                elapsed = time.perf_counter() - chunk_started
                log(
                    f"[chunk {chunk_number}/{total_chunks}] "
                    f"id {start_id}..{end_id - 1} in {elapsed:.4f} sec"
                )

            log(f"Helper build finished in {time.perf_counter() - overall_started:.4f} sec")
            counts = run_query(session, counts_query, query_timeout_ms=query_timeout_ms)[0]
            log(
                "Helper counts: "
                f"RECEIVED_CAMPAIGN={counts['received_helper_count']}, "
                f"PURCHASED_CAMPAIGN={counts['purchased_helper_count']}"
            )
    finally:
        driver.close()


if __name__ == "__main__":
    main()
