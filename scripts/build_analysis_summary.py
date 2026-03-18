#!/usr/bin/env python3
"""Build a compact CSV summary from analysis JSON outputs."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "output"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    sources = [
        ("PostgreSQL", OUTPUT_DIR / "analysis_psql.json"),
        ("MongoDB", OUTPUT_DIR / "analysis_mongodb.json"),
        ("Neo4j/Memgraph", OUTPUT_DIR / "analysis_graph.json"),
    ]

    rows: list[list[Any]] = []
    for db_name, path in sources:
        if not path.exists():
            continue
        payload = load_json(path)
        queries = payload.get("queries", {})
        for q_name in ("q1", "q2", "q3"):
            q_data = queries.get(q_name, {})
            rows.append(
                [
                    db_name,
                    q_name,
                    q_data.get("execution_time_sec"),
                    q_data.get("row_count"),
                ]
            )

    out_path = OUTPUT_DIR / "analysis_summary.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["database", "query", "execution_time_sec", "row_count"])
        writer.writerows(rows)

    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
