CREATE SCHEMA IF NOT EXISTS etl;

-- Staging table: fast bulk loads via COPY
CREATE TABLE IF NOT EXISTS etl.stg_fhvhv_trips (
    trip_key BIGINT,
    hvfhs_license_num VARCHAR(10),
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    pu_location_id SMALLINT,
    do_location_id SMALLINT,
    trip_miles NUMERIC(8,2),
    tips NUMERIC(10,2),
    driver_pay NUMERIC(10,2),
    trip_duration_minutes NUMERIC(10,2),
    pickup_date DATE,
    pickup_hour SMALLINT,
    load_batch_id UUID NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stg_fhvhv_load_batch_id
    ON etl.stg_fhvhv_trips (load_batch_id);

-- Fact table analytics-ready trips (deduped on trip_key)
CREATE TABLE IF NOT EXISTS etl.fact_fhvhv_trips (
    trip_key BIGINT PRIMARY KEY,
    hvfhs_license_num VARCHAR(10) NOT NULL,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    pu_location_id SMALLINT NOT NULL,
    do_location_id SMALLINT NOT NULL,
    trip_miles NUMERIC(8,2) NOT NULL,
    tips NUMERIC(10,2),
    driver_pay NUMERIC(10,2),
    trip_duration_minutes NUMERIC(10,2) NOT NULL,
    pickup_date DATE NOT NULL,
    pickup_hour SMALLINT NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_fhvhv_pickup_date
    ON etl.fact_fhvhv_trips (pickup_date);

CREATE INDEX IF NOT EXISTS idx_fact_fhvhv_pu_location_id
    ON etl.fact_fhvhv_trips (pu_location_id);

CREATE INDEX IF NOT EXISTS idx_fact_fhvhv_do_location_id
    ON etl.fact_fhvhv_trips (do_location_id);
