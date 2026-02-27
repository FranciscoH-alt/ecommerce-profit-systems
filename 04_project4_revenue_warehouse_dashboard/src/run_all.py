from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def run(cmd: list[str]) -> None:
    print("\n>", " ".join(cmd))
    subprocess.check_call(cmd, cwd=str(ROOT))

def main() -> None:
    py = sys.executable

    run([py, "src/01_load_raw.py", "--raw_dir", "data/raw", "--db_path", "outputs/warehouse.duckdb"])
    run([py, "src/02_clean.py", "--db_path", "outputs/warehouse.duckdb"])

    # execute SQL files in order using duckdb CLI via python -c to avoid extra deps
    run([py, "-c", "import duckdb; con=duckdb.connect('outputs/warehouse.duckdb'); con.execute(open('sql/03_build_dims.sql','r').read()); con.close()"])
    run([py, "-c", "import duckdb; con=duckdb.connect('outputs/warehouse.duckdb'); con.execute(open('sql/04_build_fact_orders.sql','r').read()); con.close()"])
    run([py, "-c", "import duckdb; con=duckdb.connect('outputs/warehouse.duckdb'); con.execute(open('sql/05_marketing_spend_and_marts.sql','r').read()); con.close()"])

    run([py, "src/06_export.py", "--db_path", "outputs/warehouse.duckdb", "--out_dir", "data/processed"])

    print("\nDONE: warehouse built + parquet exports ready for Tableau")

if __name__ == "__main__":
    main()
