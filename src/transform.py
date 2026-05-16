from __future__ import annotations
import pandas as pd

def transform_batch(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.rename(
        columns={
            "PULocationID": "pu_location_id",
            "DOLocationID": "do_location_id",
        }
    )
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

    df = df.dropna(
        subset=[
            "hvfhs_license_num",
            "pickup_datetime",
            "dropoff_datetime",
            "pu_location_id",
            "do_location_id",
            "trip_miles",
        ]
    )

    df["trip_miles"] = pd.to_numeric(df["trip_miles"], errors="coerce")
    df["tips"] = pd.to_numeric(df.get("tips"), errors="coerce")
    df["driver_pay"] = pd.to_numeric(df.get("driver_pay"), errors="coerce")

    #create trip duration column in minutes
    df["trip_duration_minutes"] = (
        (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60.0
    )
    #extract pickup date and hour for partitioning and analysis
    df["pickup_date"] = df["pickup_datetime"].dt.date
    df["pickup_hour"] = df["pickup_datetime"].dt.hour.astype("int16")

    df = df[
        (df["trip_miles"] > 0)
        & (df["trip_miles"] <= 200)
        & (df["trip_duration_minutes"] > 0)
        & (df["trip_duration_minutes"] <= 24 * 60)
    ]
    df = df[(df["tips"].isna()) | (df["tips"] >= 0)]
    df = df[(df["driver_pay"].isna()) | (df["driver_pay"] >= 0)]
    hash_cols = [
        "hvfhs_license_num",
        "pickup_datetime",
        "dropoff_datetime",
        "pu_location_id",
        "do_location_id",
        "trip_miles",
        "tips",
        "driver_pay",
    ]
    df["trip_key"] = pd.util.hash_pandas_object(df[hash_cols], index=False).values.view(
        "int64"
    )
    df["trip_miles"] = df["trip_miles"].round(2)
    df["tips"] = df["tips"].round(2)
    df["driver_pay"] = df["driver_pay"].round(2)
    df["trip_duration_minutes"] = df["trip_duration_minutes"].round(2)
    
    out_cols = ["trip_key", "hvfhs_license_num", "pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id", "trip_miles", "tips", "driver_pay", "trip_duration_minutes", "pickup_date", "pickup_hour"]
    return df[out_cols].reset_index(drop=True)
