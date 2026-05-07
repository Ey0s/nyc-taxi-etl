from __future__ import annotations

from dataclasses import dataclass

import pyarrow.parquet as pq

from .config import BATCH_SIZE_ROWS, DATA_PATH, EXTRACT_COLUMNS


@dataclass(frozen=True)
class ExtractInfo:
    path: str
    total_rows: int
    num_row_groups: int

def get_extract_info(path=DATA_PATH) -> ExtractInfo:
    if not path.exists():
        raise FileNotFoundError(
            f"Parquet file not found at {path}. "
            "Update DATA_PATH in src/config.py or set DATA_PATH in .env."
        )

    parquet = pq.ParquetFile(path)
    return ExtractInfo(
        path=str(path),
        total_rows=parquet.metadata.num_rows,
        num_row_groups=parquet.metadata.num_row_groups,
    )


def extract_batches(
    path=DATA_PATH,
    columns=EXTRACT_COLUMNS,
    batch_size_rows: int = BATCH_SIZE_ROWS,
):
    info = get_extract_info(path)
    print(f"Reading dataset: {info.path}")
    print(f"Rows: {info.total_rows:,} | Row groups: {info.num_row_groups}")

    parquet = pq.ParquetFile(path)
    for record_batch in parquet.iter_batches(batch_size=batch_size_rows, columns=columns):
        yield record_batch.to_pandas()
