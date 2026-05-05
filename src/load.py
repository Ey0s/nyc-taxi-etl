from __future__ import annotations

import io
import uuid

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

from .config import DB_CONFIG, FACT_TABLE, SCHEMA_SQL_PATH, STAGING_TABLE, TARGET_SCHEMA


def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_CONFIG["user"],
        password=DB_CONFIG["password"] or None,
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
    )

    engine = create_engine(url, connect_args={"connect_timeout": 10})

    try:
        with engine.connect():
            pass
    except OperationalError as exc:
        raise OperationalError(
            "Database connection failed. Make sure PostgreSQL is running and that "
            "your .env (PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE) is correct.",
            exc.params,
            exc.orig,
        ) from exc

    return engine


def init_db(engine) -> None:
    sql_text = SCHEMA_SQL_PATH.read_text(encoding="utf-8")
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.exec_driver_sql(stmt)


def load_batch(engine, df: pd.DataFrame, batch_id: uuid.UUID) -> int:
    if df.empty:
        return 0

    df = df.copy()
    df["load_batch_id"] = str(batch_id)

    stg_cols = [
        "trip_key",
        "hvfhs_license_num",
        "pickup_datetime",
        "dropoff_datetime",
        "pu_location_id",
        "do_location_id",
        "trip_miles",
        "tips",
        "driver_pay",
        "trip_duration_minutes",
        "pickup_date",
        "pickup_hour",
        "load_batch_id",
    ]

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N", columns=stg_cols)
    buf.seek(0)

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.copy_expert(
            f"""
COPY {TARGET_SCHEMA}.{STAGING_TABLE} ({",".join(stg_cols)})
FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')
""".strip(),
            buf,
        )
        raw.commit()
    finally:
        raw.close()

    fact_cols = [
        "trip_key",
        "hvfhs_license_num",
        "pickup_datetime",
        "dropoff_datetime",
        "pu_location_id",
        "do_location_id",
        "trip_miles",
        "tips",
        "driver_pay",
        "trip_duration_minutes",
        "pickup_date",
        "pickup_hour",
    ]

    with engine.begin() as conn:
        inserted = conn.exec_driver_sql(
            f"""
INSERT INTO {TARGET_SCHEMA}.{FACT_TABLE} ({",".join(fact_cols)})
SELECT {",".join(fact_cols)}
FROM {TARGET_SCHEMA}.{STAGING_TABLE}
WHERE load_batch_id = %s
ON CONFLICT (trip_key) DO NOTHING
""".strip(),
            (str(batch_id),),
        ).rowcount

        conn.exec_driver_sql(
            f"DELETE FROM {TARGET_SCHEMA}.{STAGING_TABLE} WHERE load_batch_id = %s",
            (str(batch_id),),
        )

    return int(inserted or 0)
