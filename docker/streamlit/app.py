import streamlit as st
import plotly.express as px
import pandas as pd
import duckdb
import plotly.graph_objects as go
from datetime import datetime

# Constants
DB_PATH = '/usr/app/data/processed/integrated_data.duckdb'
HEIGHT = 800
WIDTH = 1000

# Function: Load data
@st.cache
def load_data():
    try:
        con = duckdb.connect(DB_PATH)
        df = con.execute("SELECT * FROM integrated_data").fetchdf()
        # Ensure the 'date' column is parsed as datetime
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if "Toimumisaeg" in df.columns:
            df["Toimumisaeg"] = pd.to_datetime(df["Toimumisaeg"], errors="coerce")
        con.close()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        df = pd.DataFrame()
    return df

# Function: Create monthly aggregation chart
def create_monthly_aggregation_chart(df):
    if df.empty:
        st.warning("No data available for Monthly Aggregation.")
        return None

    df["TemperatureYearMonth"] = df["date"].dt.to_period("M").dt.to_timestamp()
    df["CrashYearMonth"] = df["Toimumisaeg"].dt.to_period("M").dt.to_timestamp()

    temperature_data = df.groupby("TemperatureYearMonth", as_index=False).agg({
        "temperature_min": "mean",
        "temperature_max": "mean"
    })
    temperature_data["temperature_avg"] = (temperature_data["temperature_min"] + temperature_data["temperature_max"]) / 2

    crash_precipitation_data = df.groupby("CrashYearMonth", as_index=False).agg({
        "Isikuid": "sum",
        "precipitation_sum": "sum"
    })

    combined_data = pd.merge(
        crash_precipitation_data,
        temperature_data,
        left_on="CrashYearMonth",
        right_on="TemperatureYearMonth",
        how="inner"
    )
    combined_data.rename(columns={"CrashYearMonth": "YearMonth"}, inplace=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(x=combined_data["YearMonth"], y=combined_data["Isikuid"], name="Persons", marker_color="blue"))
    fig.add_trace(go.Bar(x=combined_data["YearMonth"], y=combined_data["precipitation_sum"], name="Precipitation (mm)", marker_color="green"))
    fig.add_trace(go.Scatter(
        x=combined_data["YearMonth"],
        y=combined_data["Isikuid"],
        mode="lines+markers",
        name="Casualty Trendline",
        line=dict(color="red", width=2)
    ))

    fig.update_layout(
        title="Monthly Trends of Crashes, Precipitation, and Casualty Trendline",
        xaxis_title="Month",
        yaxis_title="Values",
        barmode="group",
        height=HEIGHT,
        width=WIDTH,
        legend_title="Metrics"
    )
    return fig

# Function: Create severity vs. temperature chart
def create_severity_temperature_chart(df):
    if df.empty:
        st.warning("No data available for Severity vs. Temperature.")
        return None

    bins = [-float("inf"), -10, 0, 10, 20, float("inf")]
    labels = ["< -10°C", "-10°C to 0°C", "0°C to 10°C", "10°C to 20°C", "> 20°C"]
    df["temperature_category"] = pd.cut(df["temperature_min"], bins=bins, labels=labels)
    
    grouped = df.groupby("temperature_category", as_index=False).agg({
        "Vigastatuid": "mean",
        "Hukkunuid": "mean"
    }).rename(columns={"Vigastatuid": "Injuries", "Hukkunuid": "Fatalities"})

    fig = go.Figure()
    fig.add_trace(go.Bar(x=grouped["temperature_category"], y=grouped["Injuries"], name="Injuries", marker_color="blue"))
    fig.add_trace(go.Bar(x=grouped["temperature_category"], y=grouped["Fatalities"], name="Fatalities", marker_color="red"))
    fig.add_trace(go.Scatter(x=grouped["temperature_category"], y=grouped["Injuries"], mode="lines+markers", name="Injury Trend", line=dict(color="blue", dash="dash")))
    fig.add_trace(go.Scatter(x=grouped["temperature_category"], y=grouped["Fatalities"], mode="lines+markers", name="Fatality Trend", line=dict(color="red", dash="dash")))

    fig.update_layout(
        title="Accident Severity vs. Temperature",
        xaxis_title="Temperature Category",
        yaxis_title="Average per Accident",
        barmode="group",
        height=HEIGHT,
        width=WIDTH,
        legend_title="Metrics"
    )
    return fig

# Function: Create precipitation vs. vehicles/people chart
def create_precipitation_chart(df):
    if df.empty:
        st.warning("No data available for Precipitation Analysis.")
        return None

    bins = [0, 10, 20, 30, float("inf")]
    labels = ["0-10 mm", "10-20 mm", "20-30 mm", "> 30 mm"]
    df["Precipitation Category"] = pd.cut(df["precipitation_sum"], bins=bins, labels=labels)

    agg_data = df.groupby("Precipitation Category", as_index=False).agg({
        "Sõidukeid": "sum",
        "Isikuid": "sum"
    }).rename(columns={"Sõidukeid": "Vehicles", "Isikuid": "People"})

    fig = px.bar(
        agg_data,
        x="Precipitation Category",
        y=["Vehicles", "People"],
        barmode="group",
        title="Vehicles and People Involved by Precipitation Levels",
        labels={"value": "Count", "Precipitation Category": "Precipitation (mm)"}
    )
    return fig

# Main Streamlit App
st.title("Car Crashes and Weather Data Dashboard")

# Load and preprocess data
df = load_data()

# Sidebar for Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Monthly Aggregation", "Severity vs Temperature", "Precipitation Analysis"])

# Display Selected Page
if page == "Monthly Aggregation":
    st.subheader("Monthly Aggregation")
    fig_monthly = create_monthly_aggregation_chart(df)
    if fig_monthly:
        st.plotly_chart(fig_monthly, use_container_width=True)

elif page == "Severity vs Temperature":
    st.subheader("Severity vs Temperature")
    fig_severity = create_severity_temperature_chart(df)
    if fig_severity:
        st.plotly_chart(fig_severity, use_container_width=True)

elif page == "Precipitation Analysis":
    st.subheader("Precipitation Analysis")
    fig_precipitation = create_precipitation_chart(df)
    if fig_precipitation:
        st.plotly_chart(fig_precipitation, use_container_width=True)
