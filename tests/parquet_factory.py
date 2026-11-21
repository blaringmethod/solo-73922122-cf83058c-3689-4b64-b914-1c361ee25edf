"""测试辅助：在独立目录生成多 row group 的 Parquet 样本文件。"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Sequence

import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = Path(__file__).resolve().parent / "_data"


def write_parquet(
    table: pa.Table, row_group_size: int, name: str = "sample.parquet"
) -> str:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / name
    pq.write_table(table, str(path), row_group_size=row_group_size)
    return str(path)


def make_table(columns: dict, schema: pa.Schema | None = None) -> pa.Table:
    return pa.table(columns, schema=schema)


def expected_num_row_groups(n_rows: int, row_group_size: int) -> int:
    return (n_rows + row_group_size - 1) // row_group_size
