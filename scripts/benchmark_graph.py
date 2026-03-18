#!/usr/bin/env python3
"""Benchmark assignment analysis queries on Neo4j/Memgraph."""

from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path
from statistics import mean
from typing import Any, Callable

from neo4j import GraphDatabase

from analyze_graph import build_keywords_from_q2, read_query, run_cypher, run_q3


REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "benchmark_results"
RESULTS_FILE = RESULTS_DIR / "graph_results.csv"
DATABASE_LABEL = "graph"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Neo4j/Memgraph analysis queries.")
    parser.add_argument("runs", nargs="?", type=int, default=1, help="Number of runs per query (default: 1)")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("runs must be >= 1")
    return args


def benchmark_query(
    query_name: str,
    runs: int,
    runner: Callable[[], Any],
    database_label: str,
) -> tuple[list[dict[str, Any]], float, Any]:
    rows: list[dict[str, Any]] = []
    timings: list[float] = []
    last_result: Any = None

    for run_number in range(1, runs + 1):
        started = time.perf_counter()
        last_result = runner()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        timings.append(elapsed_ms)
        rows.append(
            {
                "query_name": query_name,
                "run_number": run_number,
                "execution_time_ms": f"{elapsed_ms:.3f}",
                "database": database_label,
            }
        )
        print(f"[{query_name}] run {run_number}/{runs}: {elapsed_ms:.3f} ms")

    avg_ms = mean(timings)
    print(f"[{query_name}] average: {avg_ms:.3f} ms")
    return rows, avg_ms, last_result


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["query_name", "run_number", "execution_time_ms", "database"],
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    runs = args.runs

    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "neo4jneo4j")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    q1_query = read_query("q1")
    q2_query = read_query("q2")

    print(f"Benchmarking Graph DB: {database} @ {uri} as {user}")
    print(f"Runs per query: {runs}")

    all_rows: list[dict[str, Any]] = []
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session(database=database) as session:
            q1_rows, _, _ = benchmark_query(
                "q1",
                runs,
                lambda: run_cypher(session, q1_query),
                DATABASE_LABEL,
            )
            all_rows.extend(q1_rows)

            q2_rows, _, q2_result_rows = benchmark_query(
                "q2",
                runs,
                lambda: run_cypher(session, q2_query),
                DATABASE_LABEL,
            )
            all_rows.extend(q2_rows)

            keywords = build_keywords_from_q2(q2_result_rows or [])
            print(f"[q3] Keywords from latest q2 result: {keywords}")

            q3_rows, _, _ = benchmark_query(
                "q3",
                runs,
                lambda: run_q3(session, keywords),
                DATABASE_LABEL,
            )
            all_rows.extend(q3_rows)
    finally:
        driver.close()

    write_csv(RESULTS_FILE, all_rows)
    print(f"Saved benchmark results: {RESULTS_FILE}")


if __name__ == "__main__":
    main()
