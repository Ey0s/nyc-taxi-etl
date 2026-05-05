from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

_data_path_env = os.getenv("DATA_PATH")
if _data_path_env:
    _candidate = Path(_data_path_env)
    DATA_PATH = _candidate if _candidate.is_absolute() else (PROJECT_ROOT / _candidate)
else:
    DATA_PATH = PROJECT_ROOT / "data" / "raw" / "fhvhv_tripdata_2026-01.parquet"

DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", "taxi_etl"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
}

TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "etl")
FACT_TABLE = os.getenv("FACT_TABLE", "fact_fhvhv_trips")
STAGING_TABLE = os.getenv("STAGING_TABLE", "stg_fhvhv_trips")

BATCH_SIZE_ROWS = int(os.getenv("BATCH_SIZE_ROWS", "250000"))

SCHEMA_SQL_PATH = PROJECT_ROOT / "sql" / "schema.sql"

EXTRACT_COLUMNS = [
    "hvfhs_license_num",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "tips",
    "driver_pay",
]
