#!/usr/bin/env python3
"""Run assignment data analysis queries on PostgreSQL."""

from __future__ import annotations

import json
import os
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor


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
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def read_query(query_name: str) -> str:
    path = SCRIPT_DIR / f"{query_name}.sql"
    return path.read_text(encoding="utf-8")


def run_query(cursor: RealDictCursor, query_name: str, query_text: str) -> dict[str, Any]:
    started = time.perf_counter()
    cursor.execute(query_text)
    rows = cursor.fetchall()
    elapsed = time.perf_counter() - started

    print(f"\n[{query_name}]")
    print(f"Execution time: {elapsed:.4f} sec")
    print(f"Rows returned : {len(rows)}")
    preview = rows[:5]
    if preview:
        print("Preview:")
        print(json.dumps(to_jsonable(preview), indent=2, ensure_ascii=True))

    return {
        "execution_time_sec": elapsed,
        "row_count": len(rows),
        "rows": to_jsonable(rows),
    }


def main() -> None:
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD")
    dbname = os.getenv("TARGET_DB", "customer_campaign_analytics")

    print("Running PostgreSQL analysis")
    print(f"DB: {dbname} @ {host}:{port} as {user}")

    connect_kwargs: dict[str, Any] = {
        "host": host,
        "port": port,
        "user": user,
        "dbname": dbname,
        "cursor_factory": RealDictCursor,
    }
    if password:
        connect_kwargs["password"] = password

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with psycopg2.connect(**connect_kwargs) as conn:
        with conn.cursor() as cursor:
            results = {
                "q1": run_query(cursor, "q1", read_query("q1")),
                "q2": run_query(cursor, "q2", read_query("q2")),
                "q3": run_query(cursor, "q3", read_query("q3")),
            }

    output_payload = {
        "database": "postgresql",
        "connection": {
            "host": host,
            "port": port,
            "user": user,
            "database": dbname,
        },
        "queries": results,
    }
    out_path = OUTPUT_DIR / "analysis_psql.json"
    out_path.write_text(json.dumps(output_payload, indent=2, ensure_ascii=True), encoding="utf-8")
    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
