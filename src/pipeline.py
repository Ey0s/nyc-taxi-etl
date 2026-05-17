from __future__ import annotations
import argparse
import uuid
from tqdm import tqdm 
from .config import DATA_PATH
from .extract import extract_batches, get_extract_info
from .load import get_engine, init_db, load_batch
from .transform import transform_batch

def run_pipeline(init: bool) -> None:
    print("Starting ETL pipeline (Pandas -> PostgreSQL)")
    engine = get_engine()
    if init:
        print("Initializing database schema...")
        init_db(engine)

    info = get_extract_info(DATA_PATH)
    total_in = 0
    total_out = 0
    total_loaded = 0
    with tqdm(total=info.total_rows, unit="rows") as bar:
        for i, df in enumerate(extract_batches(DATA_PATH), start=1):
            total_in += len(df)
            cleaned = transform_batch(df)
            total_out += len(cleaned)

            batch_id = uuid.uuid4()
            loaded = load_batch(engine, cleaned, batch_id=batch_id)
            total_loaded += loaded

            bar.update(len(df))
            bar.set_postfix(batch=i, kept=len(cleaned), inserted=loaded)

    print(
        "Completed. "
        f"Read={total_in:,} | Kept={total_out:,} | Inserted={total_loaded:,}"
    )

def parse_args():
    p = argparse.ArgumentParser(description="NYC Taxi ETL: Parquet -> PostgreSQL")
    p.add_argument(
        "--init-db",
        action="store_true",
        help="Create schema/tables if missing (runs sql/schema.sql).",
    )
    return p.parse_args()

def main():
    args = parse_args()
    run_pipeline(init=args.init_db)
if __name__ == "__main__":
    main()
