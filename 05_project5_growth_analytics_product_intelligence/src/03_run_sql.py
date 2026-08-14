"""
03_run_sql.py
─────────────
Generic SQL file runner for DuckDB.
Executes a single .sql file against the warehouse.

Usage:
    python3 src/03_run_sql.py --db_path outputs/warehouse.duckdb --sql_file sql/03_build_dims.sql
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser(description="Execute a SQL file against DuckDB warehouse.")
    ap.add_argument("--db_path",  default="outputs/warehouse.duckdb", help="Path to DuckDB warehouse file")
    ap.add_argument("--sql_file", required=True,                       help="Path to .sql file to execute")
    args = ap.parse_args()

    db_path  = Path(args.db_path).resolve()
    sql_file = Path(args.sql_file).resolve()

    if not db_path.exists():
        raise SystemExit(f"Warehouse not found: {db_path}  — run 01_load_raw.py first.")
    if not sql_file.exists():
        raise SystemExit(f"SQL file not found: {sql_file}")

    sql = sql_file.read_text(encoding="utf-8")

    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=8;")
    con.execute(sql)
    con.close()

    print(f"OK: executed {sql_file.name}")


if __name__ == "__main__":
    main()
