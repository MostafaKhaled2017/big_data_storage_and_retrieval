#!/usr/bin/env python3
"""Run assignment data analysis queries on Neo4j/Memgraph."""

from __future__ import annotations

import json
import os
import re
import time
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any

from neo4j import GraphDatabase


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = REPO_ROOT / "output"


def to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [to_jsonable(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def read_query(query_name: str) -> str:
    path = SCRIPT_DIR / f"{query_name}.cypherl"
    return path.read_text(encoding="utf-8")


def run_cypher(session: Any, query_text: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    result = session.run(query_text, params or {})
    return [record.data() for record in result]


def build_keywords_from_q2(q2_rows: list[dict[str, Any]], top_n: int = 3) -> list[str]:
    counter: Counter[str] = Counter()
    for row in q2_rows:
        category_code = (row.get("category_code") or "").lower()
        for token in re.split(r"[^a-z0-9]+", category_code):
            if len(token) > 2:
                counter[token] += 1
    return [token for token, _ in counter.most_common(top_n)]


def run_q3(session: Any, keywords: list[str]) -> list[dict[str, Any]]:
    if not keywords:
        return []

    results: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    fulltext_supported = True
    try:
        session.run(
            "CREATE FULLTEXT INDEX category_code_fulltext IF NOT EXISTS "
            "FOR (c:Category) ON EACH [c.category_code]"
        ).consume()
    except Exception:
        fulltext_supported = False

    if fulltext_supported:
        fulltext_query = (
            "CALL db.index.fulltext.queryNodes('category_code_fulltext', $keyword) "
            "YIELD node, score "
            "MATCH (p:Product)-[:IN_CATEGORY]->(node) "
            "RETURN $keyword AS keyword, p.product_id AS product_id, "
            "node.category_code AS category_code, p.brand AS brand, score AS text_score "
            "ORDER BY text_score DESC, product_id LIMIT 20"
        )
        try:
            for keyword in keywords:
                rows = run_cypher(session, fulltext_query, {"keyword": keyword})
                for row in rows:
                    key = (row["keyword"], str(row["product_id"]))
                    if key in seen:
                        continue
                    seen.add(key)
                    results.append(row)
        except Exception:
            fulltext_supported = False
            results.clear()
            seen.clear()

    if not fulltext_supported:
        fallback_query = (
            "MATCH (p:Product)-[:IN_CATEGORY]->(c:Category) "
            "WHERE toLower(coalesce(c.category_code, '')) CONTAINS $keyword "
            "RETURN $keyword AS keyword, p.product_id AS product_id, "
            "c.category_code AS category_code, p.brand AS brand, 0.0 AS text_score "
            "ORDER BY product_id LIMIT 20"
        )
        for keyword in keywords:
            rows = run_cypher(session, fallback_query, {"keyword": keyword})
            for row in rows:
                key = (row["keyword"], str(row["product_id"]))
                if key in seen:
                    continue
                seen.add(key)
                results.append(row)

    results.sort(key=lambda r: (r["keyword"], -(r.get("text_score") or 0), str(r["product_id"])))
    return results


def time_query(name: str, fn: Any) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    started = time.perf_counter()
    rows = fn()
    elapsed = time.perf_counter() - started
    print(f"\n[{name}]")
    print(f"Execution time: {elapsed:.4f} sec")
    print(f"Rows returned : {len(rows)}")
    preview = rows[:5]
    if preview:
        print("Preview:")
        print(json.dumps(to_jsonable(preview), indent=2, ensure_ascii=True))
    return (
        {
            "execution_time_sec": elapsed,
            "row_count": len(rows),
            "rows": to_jsonable(rows),
        },
        rows,
    )


def main() -> None:
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "neo4jneo4j")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    print("Running Graph analysis")
    print(f"DB: {database} @ {uri} as {user}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session(database=database) as session:
            q1_query = read_query("q1")
            q2_query = read_query("q2")

            q1_result, _ = time_query("q1", lambda: run_cypher(session, q1_query))
            q2_result, q2_rows = time_query("q2", lambda: run_cypher(session, q2_query))

            keywords = build_keywords_from_q2(q2_rows)
            print(f"\n[q3] Keywords from q2 products: {keywords}")
            q3_result, _ = time_query("q3", lambda: run_q3(session, keywords))

        output_payload = {
            "database": "neo4j_or_memgraph",
            "connection": {
                "uri": uri,
                "user": user,
                "database": database,
            },
            "queries": {
                "q1": q1_result,
                "q2": q2_result,
                "q3": q3_result,
            },
        }
        out_path = OUTPUT_DIR / "analysis_graph.json"
        out_path.write_text(json.dumps(output_payload, indent=2, ensure_ascii=True), encoding="utf-8")
        print(f"\nSaved: {out_path}")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
