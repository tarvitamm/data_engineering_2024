import dash
from dash import html, dcc
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Initialize Dash app
app = dash.Dash(__name__)

# Connect to DuckDB and fetch data from views
duckdb_file = '/usr/app/data/processed/integrated_data.duckdb'  # Update with your DuckDB file path
try:
    con = duckdb.connect(duckdb_file)

    # Fetch data from views
    monthly_data = con.execute("SELECT * FROM monthly;").fetchdf()
    time_precip_data = con.execute("SELECT * FROM accident_timeofday;").fetchdf()
    severity_temp_data = con.execute("SELECT * FROM combined_analysis;").fetchdf()
    weather_data = con.execute("SELECT * FROM correlation;").fetchdf()

    con.close()
except Exception as e:
    print(f"Error fetching data from DuckDB: {str(e)}")
    monthly_data = pd.DataFrame()
    time_precip_data = pd.DataFrame()
    severity_temp_data = pd.DataFrame()

# Ensure data is sorted correctly by month
if not monthly_data.empty:
    monthly_data["month"] = monthly_data["month"].astype(int)
    monthly_data = monthly_data.sort_values("month")

    # Map month numbers to names
    month_map = {
        1: "January", 2: "February", 3: "March", 4: "April", 5: "May",
        6: "June", 7: "July", 8: "August", 9: "September", 10: "October",
        11: "November", 12: "December"
    }
    monthly_data["month_name"] = monthly_data["month"].map(month_map)

# Create plots from the fetched data

# Plot 1: Monthly Accident Trends
fig_monthly = px.line(
    monthly_data,
    x="month_name",
    y="avg_accidents",  # Correct column name
    title="Monthly Average Accident Trends",
    labels={"month_name": "Month", "avg_accidents": "Average Accidents"},
    markers=True
)
fig_monthly_precip = px.line(
    monthly_data,
    x="month_name",
    y="avg_temp",
    title="Average Monthly Temperature",
    labels={"month_name": "Month", "avg_temp": "Average Temperature"},
    markers=True
)

fig_weather_pie = px.pie(
    weather_data,
    values="total_accidents",
    names="weather_condition",
    title="Accidents by Weather Conditions",
    color="weather_condition",  # Optional: Assign specific colors
    color_discrete_map={
        "Good": "green",
        "Fair": "yellow",
        "Poor": "orange",
        "Bad": "red"
    }
)
# Dual-Axis Line Chart: Monthly Avg Temp and Avg Accidents
fig_dual_axis = go.Figure()

fig_dual_axis.add_trace(
    go.Scatter(
        x=monthly_data["month_name"],
        y=monthly_data["avg_temp"],
        mode="lines+markers",
        name="Average Temperature (°C)",
        line=dict(color="blue")
    )
)

fig_dual_axis.add_trace(
    go.Scatter(
        x=monthly_data["month_name"],
        y=monthly_data["avg_accidents"],
        mode="lines+markers",
        name="Average Accidents",
        line=dict(color="red"),
        yaxis="y2"
    )
)

fig_dual_axis.update_layout(
    title="Average Temperature and Accident Count by Month",
    xaxis_title="Month",
    yaxis_title="Average Temperature (°C)",
    yaxis2=dict(
        title="Average Accidents",
        overlaying="y",
        side="right"
    ),
    xaxis=dict(tickmode="linear"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    margin=dict(t=50, b=40)
)

# Plot 2: Time of Day vs. Precipitation Category
fig_time_precip = px.bar(
    time_precip_data,
    x="time_of_day",
    y="total_accidents",
    color="precipitation_category",
    title="Accidents by Time of Day and Precipitation Category",
    labels={"time_of_day": "Time of Day", "total_accidents": "Total Accidents"},
    category_orders={"time_of_day": ["Night (00:00-05:59)", "Morning (06:00-11:59)", "Afternoon (12:00-17:59)", "Evening (18:00-23:59)"]},  # Ensure all periods appear in order
    barmode="stack"
)

# Plot 3: Accident Severity vs. Temperature Categories
# Prepare melted data for a grouped bar chart
severity_temp_melted = severity_temp_data.melt(
    id_vars="temperature_category",
    value_vars=["avg_injuries_per_accident", "avg_fatalities_per_accident"],
    var_name="severity_type",
    value_name="value"
)
fig_severity_temp = px.bar(
    severity_temp_melted,
    x="temperature_category",
    y="value",
    color="severity_type",
    title="Accident Severity vs. Temperature Categories",
    labels={"temperature_category": "Temperature Category", "value": "Average per Accident"},
    barmode="group"
)

# Layout of the Dash app
app.layout = html.Div([
    html.H1('Traffic Accident Analysis Dashboard'),
    
    # Monthly Trends Section
    html.Div([
        html.H2("Monthly Accident Trends"),
        dcc.Graph(figure=fig_monthly),
        dcc.Graph(figure=fig_monthly_precip),
        dcc.Graph(figure=fig_dual_axis)  # Adding the dual-axis plot
    ]),
    
    html.Div([
        html.H2("Accidents by Weather Condition"),
        dcc.Graph(figure=fig_weather_pie)
    ]),

    # Time of Day Section
    html.Div([
        html.H2("Accidents by Time of Day and Precipitation"),
        dcc.Graph(figure=fig_time_precip)
    ]),

    # Severity vs. Temperature Section
    html.Div([
        html.H2("Accident Severity vs. Temperature Categories"),
        dcc.Graph(figure=fig_severity_temp)
    ])
])

# Run the app
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050)
