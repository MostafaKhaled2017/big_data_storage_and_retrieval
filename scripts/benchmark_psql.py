#!/usr/bin/env python3
"""Benchmark assignment analysis queries on PostgreSQL."""

from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path
from statistics import mean
from typing import Any, Callable

import psycopg2

from analyze_psql import read_query


REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "benchmark_results"
RESULTS_FILE = RESULTS_DIR / "postgres_results.csv"
DATABASE_LABEL = "postgres"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark PostgreSQL analysis queries.")
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
) -> tuple[list[dict[str, Any]], float]:
    rows: list[dict[str, Any]] = []
    timings: list[float] = []

    for run_number in range(1, runs + 1):
        started = time.perf_counter()
        runner()
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
    return rows, avg_ms


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

    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD")
    dbname = os.getenv("TARGET_DB", "customer_campaign_analytics")

    connect_kwargs: dict[str, Any] = {
        "host": host,
        "port": port,
        "user": user,
        "dbname": dbname,
    }
    if password:
        connect_kwargs["password"] = password

    q1 = read_query("q1")
    q2 = read_query("q2")
    q3 = read_query("q3")

    print(f"Benchmarking PostgreSQL: {dbname} @ {host}:{port} as {user}")
    print(f"Runs per query: {runs}")

    all_rows: list[dict[str, Any]] = []
    with psycopg2.connect(**connect_kwargs) as conn:
        with conn.cursor() as cursor:
            q1_rows, _ = benchmark_query(
                "q1",
                runs,
                lambda: cursor.execute(q1) or cursor.fetchall(),
                DATABASE_LABEL,
            )
            all_rows.extend(q1_rows)

            q2_rows, _ = benchmark_query(
                "q2",
                runs,
                lambda: cursor.execute(q2) or cursor.fetchall(),
                DATABASE_LABEL,
            )
            all_rows.extend(q2_rows)

            q3_rows, _ = benchmark_query(
                "q3",
                runs,
                lambda: cursor.execute(q3) or cursor.fetchall(),
                DATABASE_LABEL,
            )
            all_rows.extend(q3_rows)

    write_csv(RESULTS_FILE, all_rows)
    print(f"Saved benchmark results: {RESULTS_FILE}")


if __name__ == "__main__":
    main()
