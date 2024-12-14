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

# Enhanced function: Log query performance
def log_query_performance(query, execution_time, rows_affected):
    log_entry = {
        "query": query,
        "execution_time_ms": execution_time,
        "rows_affected": rows_affected,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    # Store in Redis as a list of logs
    redis_client.lpush("query_logs", str(log_entry))
    # Keep only the last 100 logs
    redis_client.ltrim("query_logs", 0, 99)

# Enhanced function: Get cached query result
def get_cached_query_result(query):
    cache_key = f"query_cache:{query}"
    cached_result = redis_client.get(cache_key)
    if cached_result:
        # Convert cached JSON back to DataFrame
        return pd.read_json(cached_result.decode("utf-8"))
    return None

# Enhanced function: Execute query with caching and performance logging
def execute_query_with_caching_and_logging(query):
    # Check Redis cache first
    cached_result = get_cached_query_result(query)
    if cached_result is not None:
        print(f"Cache hit for query: {query}")
        return cached_result

    # If not cached, execute query on DuckDB
    try:
        con = duckdb.connect(DB_PATH)
        start_time = datetime.now()
        result = con.execute(query).fetchdf()
        execution_time = (datetime.now() - start_time).total_seconds() * 1000  # Convert to milliseconds
        rows_affected = len(result)
        con.close()

        # Cache the result in Redis
        redis_client.set(f"query_cache:{query}", result.to_json(), ex=3600)  # Cache for 1 hour

        # Log performance in Redis
        log_query_performance(query, execution_time, rows_affected)

        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()

# Enhanced function: Fetch query logs
def fetch_query_logs():
    logs = redis_client.lrange("query_logs", 0, -1)  # Get all logs
    return [eval(log.decode("utf-8")) for log in logs] if logs else []

# Example Usage
query = "SELECT Isikuid FROM integrated_data"

# Execute the query with caching and logging
result = execute_query_with_caching_and_logging(query)

# Fetch and display query logs
query_logs = fetch_query_logs()

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
        dcc.Tab(label="Query Logs", children=[
            html.Div([
                html.H3("Query Performance Logs"),
                html.Pre("\n".join(str(log) for log in query_logs), style={"backgroundColor": "#f9f9f9", "padding": "15px", "border": "1px solid #ccc"})
            ])
        ])
    ])
])

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050)