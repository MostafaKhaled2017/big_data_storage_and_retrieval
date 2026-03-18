#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Iterable

MISSING_TOKENS = {"", "na", "n/a", "null", "none", "nan"}
BOOLEAN_TRUE = {"true", "t", "1", "yes", "y"}
BOOLEAN_FALSE = {"false", "f", "0", "no", "n"}

# Datetime formats found in the provided raw datasets.
DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S %Z",
    "%Y-%m-%d",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean raw CSV files and write cleaned CSV files."
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory containing raw CSV files (default: data/raw).",
    )
    parser.add_argument(
        "--processed-dir",
        type=Path,
        default=Path("data/processed"),
        help="Directory where cleaned CSV files are written (default: data/processed).",
    )
    return parser.parse_args()


def is_bool_column(column: str) -> bool:
    col = column.lower()
    return (
        col.startswith("is_")
        or col.startswith("has_")
        or "_with_" in col
        or col in {"ab_test", "warmup_mode"}
    )


def is_datetime_column(column: str) -> bool:
    col = column.lower()
    return col.endswith("_at") or col.endswith("_time") or "date" in col


def is_integer_like_column(column: str) -> bool:
    col = column.lower()
    return (
        col == "id"
        or col.endswith("_id")
        or col in {"friend1", "friend2", "total_count", "hour_limit", "position"}
    )


def parse_datetime(value: str) -> datetime | None:
    for fmt in DATETIME_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def normalize_boolean(value: str) -> str:
    v = value.strip().lower()
    if v in BOOLEAN_TRUE:
        return "1"
    if v in BOOLEAN_FALSE:
        return "0"
    return value.strip()


def normalize_number(value: str, integer_only: bool) -> str:
    v = value.strip()
    if v == "":
        return ""

    if integer_only:
        try:
            as_float = float(v)
        except ValueError:
            return v
        if as_float.is_integer():
            return str(int(as_float))
        return v

    # Keep decimal data but remove cosmetic trailing zeros (e.g. 123.4500 -> 123.45).
    try:
        as_float = float(v)
    except ValueError:
        return v
    normalized = f"{as_float:.10f}".rstrip("0").rstrip(".")
    return normalized if normalized else "0"


def clean_value(column: str, value: str) -> str:
    value = (value or "").strip()
    if value.lower() in MISSING_TOKENS:
        return ""

    if is_bool_column(column):
        value = normalize_boolean(value)

    if is_datetime_column(column) and value:
        parsed = parse_datetime(value)
        if parsed is not None:
            if len(value) == 10:
                return parsed.strftime("%Y-%m-%d")
            return parsed.strftime("%Y-%m-%d %H:%M:%S")

    if is_integer_like_column(column):
        return normalize_number(value, integer_only=True)

    if column.lower() == "price":
        return normalize_number(value, integer_only=False)

    return value


def process_csv(input_path: Path, output_path: Path) -> tuple[int, int, int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with input_path.open("r", encoding="utf-8", newline="") as src, output_path.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        reader = csv.DictReader(src)
        if reader.fieldnames is None:
            return 0, 0, 0

        fieldnames = [name.strip() for name in reader.fieldnames]
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()

        kept_rows = 0
        skipped_empty = 0
        skipped_malformed = 0

        for row in reader:
            if row is None:
                continue

            # Extra unnamed columns indicate malformed rows.
            if None in row:
                skipped_malformed += 1
                continue

            cleaned_row = {col: clean_value(col, row.get(col, "")) for col in fieldnames}

            if all(value == "" for value in cleaned_row.values()):
                skipped_empty += 1
                continue

            writer.writerow(cleaned_row)
            kept_rows += 1

    return kept_rows, skipped_empty, skipped_malformed


def find_csv_files(raw_dir: Path) -> Iterable[Path]:
    return sorted(p for p in raw_dir.rglob("*.csv") if p.is_file())


def main() -> None:
    args = parse_args()
    raw_dir: Path = args.raw_dir
    processed_dir: Path = args.processed_dir

    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw data directory not found: {raw_dir}")

    csv_files = list(find_csv_files(raw_dir))
    if not csv_files:
        print(f"No CSV files found under: {raw_dir}")
        return

    total_kept = 0
    total_empty = 0
    total_malformed = 0

    for input_path in csv_files:
        relative = input_path.relative_to(raw_dir)
        output_path = processed_dir / relative
        kept, empty, malformed = process_csv(input_path, output_path)
        total_kept += kept
        total_empty += empty
        total_malformed += malformed
        print(
            f"Processed {relative}: kept={kept}, "
            f"skipped_empty={empty}, skipped_malformed={malformed}"
        )

    print(
        f"Done. Files={len(csv_files)}, kept_rows={total_kept}, "
        f"skipped_empty_rows={total_empty}, skipped_malformed_rows={total_malformed}"
    )


if __name__ == "__main__":
    main()
