import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import duckdb
from pyproj import Transformer
import plotly.graph_objects as go
import redis
from datetime import datetime

# Constants
DB_PATH = '/usr/app/data/processed/integrated_data.duckdb'
MAPBOX_STYLE = "carto-positron"
HEIGHT = 800

# Function: Load data
def load_data():
    try:
        con = duckdb.connect(DB_PATH)
        df = con.execute("SELECT * FROM integrated_data").fetchdf()
        con.close()
    except Exception as e:
        print(f"Error loading data: {e}")
        df = pd.DataFrame()
    return df

# Function: Transform data
def preprocess_data(df):
    transformer = Transformer.from_crs("EPSG:3301", "EPSG:4326", always_xy=True)
    df["Longitude"], df["Latitude"] = transformer.transform(df["Y koordinaat"].values, df["X koordinaat"].values)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["Toimumisaeg"] = pd.to_datetime(df["Toimumisaeg"], errors="coerce")
    return df

# Function: Create severity vs. temperature chart
def create_severity_temperature_chart(df):
    if df.empty:
        return go.Figure().update_layout(title="No Data Available for Severity vs. Temperature")

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
        legend_title="Metrics"
    )
    return fig

# Function: Create monthly aggregation chart
def create_monthly_aggregation_chart(df):
    if df.empty:
        return go.Figure().update_layout(title="No Data Available for Monthly Aggregation")

    # Process monthly data
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

    # Create the chart
    fig = go.Figure()
    fig.add_trace(go.Bar(x=combined_data["YearMonth"], y=combined_data["Isikuid"], name="Persons", marker_color="blue"))
    fig.add_trace(go.Bar(x=combined_data["YearMonth"], y=combined_data["precipitation_sum"], name="Precipitation (mm)", marker_color="green"))
    
    # Add trendline for "Isikuid"
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
        legend_title="Metrics"
    )
    return fig

# Function: Create map visualization
def create_map(df):
    return px.scatter_mapbox(
        df,
        lat="Latitude",
        lon="Longitude",
        color="precipitation_sum",
        size="Vigastatuid",
        mapbox_style=MAPBOX_STYLE,
        title="Crash Locations and Weather Conditions",
        hover_name="date",
        hover_data={
            "Latitude": False,
            "Longitude": False,
            "precipitation_sum": True,
            "Vigastatuid": True,
            "date": True
        },
        labels={
            "Vigastatuid": "Injured",
            "precipitation_sum": "Precipitation (mm)",
            "date": "Date"
        }
    ).update_layout(height=HEIGHT)

# Load and preprocess data
df = load_data()
df = preprocess_data(df)
fig_severity_temp = create_severity_temperature_chart(df)
fig_monthly_aggregation = create_monthly_aggregation_chart(df)
fig_map = create_map(df)

# Connect to Redis
redis_client = redis.Redis(host="redis", port=6379)

# Save example data with a timestamp (only for illustration)
data_to_cache = "Example data to cache"
redis_client.set("example_key", data_to_cache)
redis_client.set("example_key_timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Fetch data and timestamp from Redis
try:
    data_from_redis = redis_client.get("example_key").decode("utf-8")
    timestamp_from_redis = redis_client.get("example_key_timestamp").decode("utf-8")
except (redis.exceptions.ConnectionError, AttributeError):
    data_from_redis = "No data found in Redis."
    timestamp_from_redis = "No timestamp available."


# App layout
app = dash.Dash(__name__)
app.title = "Car Crashes and Weather Analysis"
app.layout = html.Div([
    html.H1("Car Crashes and Weather Data Dashboard"),
    dcc.Tabs([
        dcc.Tab(label="Crash Density Heatmap", children=[
            html.Div([
                dcc.Graph(
                    id="spatial-heatmap",
                    figure=px.density_mapbox(
                        df,
                        lat="Latitude",
                        lon="Longitude",
                        z="Isikuid",
                        radius=10,
                        center=dict(lat=58.5953, lon=25.0136),
                        mapbox_style=MAPBOX_STYLE,
                        title="Crash Density Across Estonia"
                    ).update_layout(height=HEIGHT)
                )
            ])
        ]),
        dcc.Tab(label="Monthly Aggregation", children=[
            html.Div([dcc.Graph(id="monthly-bar-chart-with-trendline", figure=fig_monthly_aggregation)])
        ]),
        dcc.Tab(label="Severity vs Temperature", children=[
            html.Div([dcc.Graph(id="severity-temperature-chart", figure=fig_severity_temp)])
        ]),
        dcc.Tab(label="Map Visualization", children=[
            html.Div([dcc.Graph(id="map", figure=fig_map)])
        ]),
        # Create Redis display tab
        dcc.Tab(label="Redis Data", children=[
            html.Div([
                html.H3("Redis Cached Data"),
                html.P(f"Cached on: {timestamp_from_redis}", style={"fontWeight": "bold"}),
                html.Pre(data_from_redis, style={"backgroundColor": "#f9f9f9", "padding": "15px", "border": "1px solid #ccc"})
            ])
        ])
    ])
])

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050)