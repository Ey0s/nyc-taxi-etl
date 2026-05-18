from __future__ import annotations
from pathlib import Path
import sys
from typing import Iterable
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

PROJECT_ROOT_PATH = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_PATH))

from src.config import DB_CONFIG, FACT_TABLE, PROJECT_ROOT, TARGET_SCHEMA

st.set_page_config(page_title="NYC Taxi ETL Dashboard", layout="wide")

MAX_JS_SAFE_INTEGER = 2**53 - 1
@st.cache_resource
def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_CONFIG["user"],
        password=DB_CONFIG["password"] or None,
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
    )
    return create_engine(url, connect_args={"connect_timeout": 10})


@st.cache_data
def load_parquet_info(path_str: str) -> dict:
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"Raw file not found: {path}")

    pf = pq.ParquetFile(path)
    return {
        "path": str(path),
        "rows": int(pf.metadata.num_rows),
        "row_groups": int(pf.metadata.num_row_groups),
        "columns": pf.schema.names,
    }


@st.cache_data
def load_raw_preview(path_str: str, n_rows: int = 15) -> pd.DataFrame:
    path = Path(path_str)
    table = pq.ParquetFile(path).read_row_group(0)
    return table.to_pandas().head(n_rows)


@st.cache_data
def query_scalar(sql: str) -> int:
    engine = get_engine()
    with engine.connect() as conn:
        val = conn.exec_driver_sql(sql).scalar()
    return int(val or 0)


@st.cache_data
def query_df(sql: str) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


def _quote_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _in_clause(values: Iterable[str]) -> str:
    return ", ".join(_quote_str(v) for v in values)


def format_currency(v: float) -> str:
    return f"${v:,.2f}"


def sanitize_for_streamlit(df: pd.DataFrame) -> pd.DataFrame:
    """Convert columns with integer-like values beyond JS safe range to string."""
    out = df.copy()
    for col in out.columns:
        series = out[col]
        non_null = series.dropna()
        if non_null.empty:
            continue

        # Native/nullable integer dtypes.
        if pd.api.types.is_integer_dtype(series):
            if non_null.abs().max() > MAX_JS_SAFE_INTEGER:
                out[col] = series.astype("string")
            continue

        # Object columns that may contain Python integers (common from DB drivers).
        if pd.api.types.is_object_dtype(series):
            integer_like = non_null.map(lambda v: isinstance(v, int) and not isinstance(v, bool))
            if integer_like.all():
                max_abs = non_null.map(abs).max()
                if max_abs > MAX_JS_SAFE_INTEGER:
                    out[col] = series.astype("string")
    return out


@st.cache_data
def get_date_bounds(table_name: str) -> pd.DataFrame:
    return query_df(
        f"""
        SELECT MIN(pickup_date) AS min_date, MAX(pickup_date) AS max_date
        FROM {table_name}
        """
    )


@st.cache_data
def get_license_values(table_name: str) -> pd.DataFrame:
    return query_df(
        f"""
        SELECT DISTINCT hvfhs_license_num
        FROM {table_name}
        WHERE hvfhs_license_num IS NOT NULL
        ORDER BY hvfhs_license_num
        """
    )


def build_where_clause(
    start_date: str,
    end_date: str,
    hour_range: tuple[int, int],
    licenses: list[str],
) -> str:
    filters = [
        f"pickup_date BETWEEN {_quote_str(start_date)} AND {_quote_str(end_date)}",
        f"pickup_hour BETWEEN {int(hour_range[0])} AND {int(hour_range[1])}",
    ]
    if licenses:
        filters.append(f"hvfhs_license_num IN ({_in_clause(licenses)})")
    return " AND ".join(filters)


@st.cache_data
def get_kpis(table_name: str, where_clause: str) -> pd.DataFrame:
    return query_df(
        f"""
        SELECT
            COUNT(*) AS trips,
            AVG(trip_miles) AS avg_trip_miles,
            AVG(trip_duration_minutes) AS avg_duration_min,
            AVG(driver_pay) AS avg_driver_pay,
            AVG(tips) AS avg_tip,
            SUM(COALESCE(driver_pay, 0)) AS total_driver_pay,
            SUM(COALESCE(tips, 0)) AS total_tips,
            AVG(CASE WHEN COALESCE(tips, 0) > 0 THEN 1 ELSE 0 END) AS tip_rate,
            AVG(driver_pay / NULLIF(trip_miles, 0)) AS avg_pay_per_mile
        FROM {table_name}
        WHERE {where_clause}
        """
    )


@st.cache_data
def get_daily_metrics(table_name: str, where_clause: str) -> pd.DataFrame:
    return query_df(
        f"""
        SELECT
            pickup_date,
            COUNT(*) AS trips,
            AVG(trip_duration_minutes) AS avg_duration_min,
            AVG(driver_pay) AS avg_driver_pay,
            AVG(tips) AS avg_tip
        FROM {table_name}
        WHERE {where_clause}
        GROUP BY pickup_date
        ORDER BY pickup_date
        """
    )


@st.cache_data
def get_hourly_metrics(table_name: str, where_clause: str) -> pd.DataFrame:
    return query_df(
        f"""
        SELECT
            pickup_hour,
            COUNT(*) AS trips,
            AVG(trip_miles) AS avg_trip_miles,
            AVG(trip_duration_minutes) AS avg_duration_min,
            AVG(driver_pay) AS avg_driver_pay,
            AVG(tips) AS avg_tip
        FROM {table_name}
        WHERE {where_clause}
        GROUP BY pickup_hour
        ORDER BY pickup_hour
        """
    )


@st.cache_data
def get_top_pickup_zones(table_name: str, where_clause: str) -> pd.DataFrame:
    return query_df(
        f"""
        SELECT
            pu_location_id,
            COUNT(*) AS trips,
            SUM(COALESCE(driver_pay, 0)) AS total_driver_pay,
            AVG(driver_pay) AS avg_driver_pay
        FROM {table_name}
        WHERE {where_clause}
        GROUP BY pu_location_id
        ORDER BY trips DESC
        LIMIT 15
        """
    )


@st.cache_data
def get_top_routes(table_name: str, where_clause: str) -> pd.DataFrame:
    return query_df(
        f"""
        SELECT
            pu_location_id,
            do_location_id,
            COUNT(*) AS trips,
            AVG(trip_miles) AS avg_trip_miles,
            AVG(trip_duration_minutes) AS avg_duration_min,
            AVG(driver_pay) AS avg_driver_pay
        FROM {table_name}
        WHERE {where_clause}
        GROUP BY pu_location_id, do_location_id
        ORDER BY trips DESC
        LIMIT 20
        """
    )


@st.cache_data
def get_quality_flags(table_name: str, where_clause: str) -> pd.DataFrame:
    return query_df(
        f"""
        SELECT
            COUNT(*) AS trips,
            SUM(CASE WHEN trip_duration_minutes > 90 THEN 1 ELSE 0 END) AS trips_over_90min,
            SUM(CASE WHEN trip_miles > 30 THEN 1 ELSE 0 END) AS trips_over_30mi,
            SUM(CASE WHEN COALESCE(tips, 0) = 0 THEN 1 ELSE 0 END) AS zero_tip_trips,
            SUM(CASE WHEN COALESCE(driver_pay, 0) > 80 THEN 1 ELSE 0 END) AS high_pay_trips
        FROM {table_name}
        WHERE {where_clause}
        """
    )


@st.cache_data
def get_latest_trips(table_name: str) -> pd.DataFrame:
    return query_df(
        f"""
        SELECT *
        FROM {table_name}
        ORDER BY pickup_datetime DESC
        LIMIT 25
        """
    )


def main():
    st.title("NYC Taxi Business Intelligence Dashboard")
    st.caption(
        "Business-ready ETL monitoring and decision support from raw Parquet to PostgreSQL analytics."
    )

    default_raw_path = str(PROJECT_ROOT / "data" / "raw" / "fhvhv_tripdata_2026-01.parquet")

    st.sidebar.header("Data Inputs")
    raw_path = st.sidebar.text_input("Raw parquet file path", value=default_raw_path)
    preview_rows = st.sidebar.slider("Preview rows", min_value=5, max_value=50, value=15, step=5)

    st.subheader("1) Source Data Health")
    try:
        info = load_parquet_info(raw_path)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Source rows", f"{info['rows']:,}")
        c2.metric("Row groups", f"{info['row_groups']:,}")
        c3.metric("Columns", f"{len(info['columns'])}")
        c4.metric("Scale requirement", "PASS" if info["rows"] >= 2_000_000 else "CHECK")
        st.write("Source path:", info["path"])

        with st.expander("Raw file preview"):
            raw_preview = load_raw_preview(raw_path, n_rows=preview_rows)
            st.dataframe(sanitize_for_streamlit(raw_preview), use_container_width=True)

    except Exception as exc:
        st.error(f"Could not read raw parquet file: {exc}")
        st.stop()

    st.subheader("2) ETL Load Status")
    table_name = f"{TARGET_SCHEMA}.{FACT_TABLE}"

    try:
        fact_rows = query_scalar(f"SELECT COUNT(*) FROM {table_name}")
        latest_ingested = query_df(
            f"SELECT MAX(ingested_at) AS latest_ingested_at FROM {table_name}"
        )
        bounds_df = get_date_bounds(table_name)
        min_date = bounds_df.iloc[0]["min_date"]
        max_date = bounds_df.iloc[0]["max_date"]

        licenses_df = get_license_values(table_name)
        available_licenses = licenses_df["hvfhs_license_num"].dropna().astype(str).tolist()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Fact rows", f"{fact_rows:,}")
        c2.metric("Rows filtered out", f"{max(info['rows'] - fact_rows, 0):,}")
        c3.metric("Table", table_name)
        load_rate = (fact_rows / info["rows"] * 100.0) if info["rows"] else 0.0
        c4.metric("Load yield", f"{load_rate:.2f}%")

        st.write("Latest ingested_at:", latest_ingested.iloc[0]["latest_ingested_at"])

        st.sidebar.header("Business Filters")
        date_range = st.sidebar.date_input(
            "Pickup date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = min_date
            end_date = max_date

        hour_range = st.sidebar.slider("Pickup hour range", 0, 23, (0, 23), 1)
        selected_licenses = st.sidebar.multiselect(
            "License filter",
            options=available_licenses,
            default=available_licenses,
        )

        if not selected_licenses and available_licenses:
            st.sidebar.warning("No license selected. Showing all licenses.")
            selected_licenses = available_licenses

        where_clause = build_where_clause(
            start_date.isoformat(),
            end_date.isoformat(),
            hour_range,
            selected_licenses,
        )

    except Exception as exc:
        st.error(f"Could not query PostgreSQL table {table_name}: {exc}")
        st.info("Make sure the ETL has been run and DB settings in .env are correct.")
        st.stop()

    kpis = get_kpis(table_name, where_clause).iloc[0]
    daily_metrics = get_daily_metrics(table_name, where_clause)
    hourly_metrics = get_hourly_metrics(table_name, where_clause)
    top_pickup_zones = get_top_pickup_zones(table_name, where_clause)
    top_routes = get_top_routes(table_name, where_clause)
    quality_flags = get_quality_flags(table_name, where_clause).iloc[0]
    latest_trips = get_latest_trips(table_name)

    st.subheader("3) Business Insights (Post-ETL)")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Trips in filter", f"{int(kpis['trips'] or 0):,}")
    m2.metric("Avg trip distance", f"{float(kpis['avg_trip_miles'] or 0):.2f} mi")
    m3.metric("Avg trip duration", f"{float(kpis['avg_duration_min'] or 0):.2f} min")
    m4.metric("Tip rate", f"{(float(kpis['tip_rate'] or 0) * 100):.2f}%")

    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Total driver pay", format_currency(float(kpis["total_driver_pay"] or 0)))
    r2.metric("Total tips", format_currency(float(kpis["total_tips"] or 0)))
    r3.metric("Avg driver pay", format_currency(float(kpis["avg_driver_pay"] or 0)))
    r4.metric("Avg pay per mile", format_currency(float(kpis["avg_pay_per_mile"] or 0)))

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "Executive Summary",
            "Operations",
            "Revenue",
            "Data Quality",
        ]
    )

    with tab1:
        st.markdown("**Daily trend of trip demand**")
        if not daily_metrics.empty:
            st.line_chart(daily_metrics.set_index("pickup_date")[["trips"]])
            st.dataframe(sanitize_for_streamlit(daily_metrics), use_container_width=True)
        else:
            st.info("No daily data in selected filters.")

    with tab2:
        st.markdown("**Hourly operational pattern**")
        if not hourly_metrics.empty:
            st.bar_chart(hourly_metrics.set_index("pickup_hour")[["trips", "avg_duration_min"]])
            st.dataframe(sanitize_for_streamlit(hourly_metrics), use_container_width=True)
        else:
            st.info("No hourly data in selected filters.")

        st.markdown("**Top pickup zones by demand**")
        st.dataframe(sanitize_for_streamlit(top_pickup_zones), use_container_width=True)

        st.markdown("**Top origin-destination routes**")
        st.dataframe(sanitize_for_streamlit(top_routes), use_container_width=True)

    with tab3:
        st.markdown("**Revenue performance by hour**")
        if not hourly_metrics.empty:
            revenue_hour = hourly_metrics[["pickup_hour", "avg_driver_pay", "avg_tip"]].copy()
            st.bar_chart(revenue_hour.set_index("pickup_hour"))

        st.markdown("**Top zones by total driver pay**")
        if not top_pickup_zones.empty:
            revenue_zones = top_pickup_zones.sort_values("total_driver_pay", ascending=False)
            st.dataframe(sanitize_for_streamlit(revenue_zones), use_container_width=True)
        else:
            st.info("No revenue-zone data in selected filters.")

    with tab4:
        trips_total = int(quality_flags["trips"] or 0)
        q1, q2, q3, q4 = st.columns(4)
        q1.metric("Trips over 90 min", f"{int(quality_flags['trips_over_90min'] or 0):,}")
        q2.metric("Trips over 30 miles", f"{int(quality_flags['trips_over_30mi'] or 0):,}")
        q3.metric("Zero-tip trips", f"{int(quality_flags['zero_tip_trips'] or 0):,}")
        q4.metric("High-pay trips (> $80)", f"{int(quality_flags['high_pay_trips'] or 0):,}")

        if trips_total > 0:
            st.caption(
                "Quality ratios in current filter: "
                f"Long duration={(int(quality_flags['trips_over_90min']) / trips_total) * 100:.2f}%, "
                f"Long distance={(int(quality_flags['trips_over_30mi']) / trips_total) * 100:.2f}%, "
                f"Zero tip={(int(quality_flags['zero_tip_trips']) / trips_total) * 100:.2f}%"
            )

        st.markdown("**Most recently ingested trips**")
        st.dataframe(sanitize_for_streamlit(latest_trips), use_container_width=True)

    st.markdown("---")
    st.subheader("4) Export Insights")

    st.download_button(
        "Download daily metrics (CSV)",
        data=sanitize_for_streamlit(daily_metrics).to_csv(index=False),
        file_name="daily_metrics.csv",
        mime="text/csv",
    )

    st.download_button(
        "Download top routes (CSV)",
        data=sanitize_for_streamlit(top_routes).to_csv(index=False),
        file_name="top_routes.csv",
        mime="text/csv",
    )

    st.markdown("---")
    st.subheader("5) How Businesses Use the Processed Data")
    st.caption(
        "The cleaned fact table turns raw trip events into decision-ready data for operations, revenue, and quality management."
    )

    use1, use2 = st.columns(2)

    with use1:
        st.markdown("**Operations planning**")
        st.write(
            "Use pickup date and hour trends to staff operations teams, anticipate demand peaks, "
            "and identify the busiest zones for driver positioning."
        )
        st.write(
            "Useful metrics: trips, average duration, top pickup zones, top routes."
        )

        st.markdown("**Service quality monitoring**")
        st.write(
            "Use long-trip, high-mileage, and zero-tip signals to spot unusual patterns, "
            "possible bad data, and customer satisfaction issues."
        )
        st.write(
            "Useful metrics: zero-tip trips, trips over 90 minutes, trips over 30 miles."
        )

    with use2:
        st.markdown("**Revenue analysis**")
        st.write(
            "Use driver pay, tips, and pay-per-mile to understand where revenue is strongest "
            "and where incentive programs may be needed."
        )
        st.write(
            "Useful metrics: total driver pay, total tips, average pay per mile, revenue by zone."
        )

        st.markdown("**Business reporting**")
        st.write(
            "Export daily metrics and route tables to share with managers, BI teams, or instructors. "
            "The processed PostgreSQL table is much easier to query than the raw parquet file."
        )
        st.write(
            "Useful outputs: daily trend CSV, top routes CSV, filtered dashboard views."
        )

    with st.expander("Example business questions this dashboard answers"):
        st.write(
            "- When are trip volumes highest by hour and day?"
        )
        st.write(
            "- Which pickup zones generate the most demand and pay?"
        )
        st.write(
            "- Which routes are most common and how long do they take?"
        )
        st.write(
            "- Are there data quality issues that could distort reporting?"
        )
        st.write(
            "- Where should a business focus driver incentives or operational support?"
        )


if __name__ == "__main__":
    main()
