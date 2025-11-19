import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Sensor Dashboard", layout="wide")
st.title("🌡️ Real-Time Sensor Data Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5434/kafka_db"


@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)


engine = get_engine(DATABASE_URL)


def load_sensor_data(
    sensor_filter: str | None = None, limit: int = 200
) -> pd.DataFrame:
    base_query = "SELECT * FROM sensor_data"
    params = {}

    if sensor_filter and sensor_filter != "All":
        base_query += " WHERE sensor_id = :sensor_id"
        params["sensor_id"] = sensor_filter

    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()


def load_sensor_list() -> list:
    """Get list of unique sensor IDs from database."""
    try:
        query = "SELECT DISTINCT sensor_id FROM sensor_data ORDER BY sensor_id"
        df = pd.read_sql_query(text(query), con=engine.connect())
        return ["All"] + df["sensor_id"].tolist()
    except Exception:
        return [
            "All",
            "sensor_001",
            "sensor_002",
            "sensor_003",
            "sensor_004",
            "sensor_005",
        ]


# Sidebar controls
sensor_options = load_sensor_list()
selected_sensor = st.sidebar.selectbox("Filter by Sensor ID", sensor_options)
update_interval = st.sidebar.slider(
    "Update Interval (seconds)", min_value=2, max_value=20, value=5
)
limit_records = st.sidebar.number_input(
    "Number of records to load", min_value=50, max_value=2000, value=200, step=50
)

if st.sidebar.button("Refresh now"):
    st.rerun()

placeholder = st.empty()

while True:
    df_sensors = load_sensor_data(selected_sensor, limit=int(limit_records))

    with placeholder.container():
        if df_sensors.empty:
            st.warning("No sensor data found. Waiting for data...")
            time.sleep(update_interval)
            continue

        if "timestamp" in df_sensors.columns:
            df_sensors["timestamp"] = pd.to_datetime(df_sensors["timestamp"])

        # KPIs
        total_readings = len(df_sensors)
        avg_temperature = df_sensors["temperature"].mean()
        avg_humidity = df_sensors["humidity"].mean()
        min_temp = df_sensors["temperature"].min()
        max_temp = df_sensors["temperature"].max()
        min_humidity = df_sensors["humidity"].min()
        max_humidity = df_sensors["humidity"].max()

        st.subheader(
            f"Displaying {total_readings} readings (Filter: {selected_sensor})"
        )

        # Temperature KPIs
        st.markdown("#### 🌡️ Temperature Metrics")
        temp_cols = st.columns(4)
        temp_cols[0].metric("Average Temp", f"{avg_temperature:.2f}°C")
        temp_cols[1].metric("Min Temp", f"{min_temp:.2f}°C")
        temp_cols[2].metric("Max Temp", f"{max_temp:.2f}°C")
        temp_cols[3].metric("Temp Range", f"{max_temp - min_temp:.2f}°C")

        # Humidity KPIs
        st.markdown("#### 💧 Humidity Metrics")
        humidity_cols = st.columns(4)
        humidity_cols[0].metric("Average Humidity", f"{avg_humidity:.2f}%")
        humidity_cols[1].metric("Min Humidity", f"{min_humidity:.2f}%")
        humidity_cols[2].metric("Max Humidity", f"{max_humidity:.2f}%")
        humidity_cols[3].metric("Humidity Range", f"{max_humidity - min_humidity:.2f}%")

        st.markdown("---")

        # Time-series charts
        if "timestamp" in df_sensors.columns:
            # Sort by timestamp for proper time-series visualization
            df_sorted = df_sensors.sort_values("timestamp")

            # Temperature over time
            fig_temp_time = px.line(
                df_sorted,
                x="timestamp",
                y="temperature",
                color="sensor_id",
                title="Temperature Over Time",
                labels={"temperature": "Temperature (°C)", "timestamp": "Time"},
            )
            fig_temp_time.update_traces(mode="lines+markers")

            # Humidity over time
            fig_humidity_time = px.line(
                df_sorted,
                x="timestamp",
                y="humidity",
                color="sensor_id",
                title="Humidity Over Time",
                labels={"humidity": "Humidity (%)", "timestamp": "Time"},
            )
            fig_humidity_time.update_traces(mode="lines+markers")

            chart_col1, chart_col2 = st.columns(2)
            with chart_col1:
                st.plotly_chart(fig_temp_time, use_container_width=True)
            with chart_col2:
                st.plotly_chart(fig_humidity_time, use_container_width=True)

        # Bar charts by sensor
        grouped_sensor_temp = (
            df_sensors.groupby("sensor_id")["temperature"].mean().reset_index()
        )
        grouped_sensor_humidity = (
            df_sensors.groupby("sensor_id")["humidity"].mean().reset_index()
        )

        fig_sensor_temp = px.bar(
            grouped_sensor_temp,
            x="sensor_id",
            y="temperature",
            title="Average Temperature by Sensor",
            labels={"temperature": "Avg Temperature (°C)", "sensor_id": "Sensor ID"},
        )

        fig_sensor_humidity = px.bar(
            grouped_sensor_humidity,
            x="sensor_id",
            y="humidity",
            title="Average Humidity by Sensor",
            labels={"humidity": "Avg Humidity (%)", "sensor_id": "Sensor ID"},
        )

        bar_col1, bar_col2 = st.columns(2)
        with bar_col1:
            st.plotly_chart(fig_sensor_temp, use_container_width=True)
        with bar_col2:
            st.plotly_chart(fig_sensor_humidity, use_container_width=True)

        # Raw data table
        st.markdown("### 📊 Raw Data (Latest 10 Readings)")
        display_df = df_sensors.head(10)[
            ["sensor_id", "temperature", "humidity", "timestamp"]
        ]
        st.dataframe(display_df, use_container_width=True)

        st.markdown("---")
        st.caption(
            f"Last updated: {datetime.now().isoformat()} • Auto-refresh: {update_interval}s"
        )

    time.sleep(update_interval)
