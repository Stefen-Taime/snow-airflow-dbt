"""
Page 5 — Operations
=====================
Operational analytics: hourly demand heatmap, duration distributions, speed trends, vendor KPIs.
Models: met_hourly_demand, met_trip_duration_dist, met_avg_speed_trend, met_vendor_performance
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from utils.snowflake_conn import run_query

st.header("Operations")

# ── Data ─────────────────────────────────────────────────────────
df_hourly = run_query("SELECT * FROM ANALYTICS.OPERATIONS.MET_HOURLY_DEMAND ORDER BY TRIP_DATE, PICKUP_HOUR")
df_dur = run_query("SELECT * FROM ANALYTICS.OPERATIONS.MET_TRIP_DURATION_DIST ORDER BY TRIP_MONTH")
df_speed = run_query("SELECT * FROM ANALYTICS.OPERATIONS.MET_AVG_SPEED_TREND ORDER BY TRIP_DATE")
df_vendor = run_query("SELECT * FROM ANALYTICS.OPERATIONS.MET_VENDOR_PERFORMANCE ORDER BY TRIP_MONTH")

if df_hourly.empty:
    st.warning("No operations data available.")
    st.stop()

# ── KPIs ─────────────────────────────────────────────────────────
total_trips = df_hourly["TRIP_COUNT"].sum()
avg_speed = df_speed["AVG_SPEED_MPH"].mean()
avg_duration = df_hourly["AVG_DURATION_MIN"].mean()
unique_vendors = df_vendor["VENDOR_NAME"].nunique()
peak_hour = df_hourly.groupby("PICKUP_HOUR")["TRIP_COUNT"].sum().idxmax()
avg_distance = df_hourly["AVG_DISTANCE"].mean()

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Total Trips", f"{total_trips:,.0f}")
k2.metric("Avg Speed", f"{avg_speed:.1f} mph")
k3.metric("Avg Duration", f"{avg_duration:.1f} min")
k4.metric("Active Vendors", f"{unique_vendors}")
k5.metric("Peak Hour", f"{int(peak_hour)}:00")
k6.metric("Avg Distance", f"{avg_distance:.1f} mi")

st.divider()

# ── Hourly Demand Heatmap ────────────────────────────────────────
st.subheader("Hourly Demand Heatmap")

# Build pivot: day_of_week (rows) x pickup_hour (columns)
day_names = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
heatmap_data = (
    df_hourly.groupby(["DAY_OF_WEEK", "PICKUP_HOUR"], as_index=False)["TRIP_COUNT"]
    .sum()
    .pivot(index="DAY_OF_WEEK", columns="PICKUP_HOUR", values="TRIP_COUNT")
    .fillna(0)
)

# Ensure all hours 0-23 present
for h in range(24):
    if h not in heatmap_data.columns:
        heatmap_data[h] = 0
heatmap_data = heatmap_data[sorted(heatmap_data.columns)]

fig_heat = go.Figure(
    go.Heatmap(
        z=heatmap_data.values,
        x=[f"{h}:00" for h in heatmap_data.columns],
        y=[day_names.get(d, str(d)) for d in heatmap_data.index],
        colorscale="Inferno",
        colorbar=dict(title="Trips"),
        hovertemplate="Hour: %{x}<br>Day: %{y}<br>Trips: %{z:,.0f}<extra></extra>",
    )
)
fig_heat.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    height=320,
    margin=dict(l=60, r=20, t=10, b=40),
    xaxis_title="Hour of Day",
)
st.plotly_chart(fig_heat, use_container_width=True)

# ── Trip Duration Distribution ───────────────────────────────────
st.subheader("Trip Duration Distribution")

col_left, col_right = st.columns(2)

with col_left:
    # Order duration buckets logically
    bucket_order = ["0-5 min", "5-10 min", "10-20 min", "20-30 min", "30-60 min", "60+ min"]
    df_dur_agg = (
        df_dur.groupby(["DURATION_BUCKET", "TAXI_TYPE"], as_index=False)
        .agg({"TRIP_COUNT": "sum"})
    )
    df_dur_agg["BUCKET_ORDER"] = df_dur_agg["DURATION_BUCKET"].map(
        {b: i for i, b in enumerate(bucket_order)}
    )
    df_dur_agg = df_dur_agg.sort_values("BUCKET_ORDER")

    taxi_colors = {"yellow": "#F5C518", "green": "#00A67E"}

    fig_dur = go.Figure()
    for taxi in df_dur_agg["TAXI_TYPE"].unique():
        sub = df_dur_agg[df_dur_agg["TAXI_TYPE"] == taxi]
        fig_dur.add_trace(
            go.Bar(
                x=sub["DURATION_BUCKET"],
                y=sub["TRIP_COUNT"],
                name=taxi.capitalize(),
                marker=dict(color=taxi_colors.get(taxi, "#999"), opacity=0.85),
            )
        )

    fig_dur.update_layout(
        barmode="group",
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="Trip Count",
        xaxis_title="Duration Bucket",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
        margin=dict(l=60, r=20, t=30, b=40),
    )
    fig_dur.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
    st.plotly_chart(fig_dur, use_container_width=True)

with col_right:
    # Avg fare by duration bucket
    df_dur_fare = (
        df_dur.groupby(["DURATION_BUCKET"], as_index=False)
        .agg({"AVG_FARE": "mean", "AVG_DISTANCE": "mean"})
    )
    df_dur_fare["BUCKET_ORDER"] = df_dur_fare["DURATION_BUCKET"].map(
        {b: i for i, b in enumerate(bucket_order)}
    )
    df_dur_fare = df_dur_fare.sort_values("BUCKET_ORDER")

    fig_fare_dur = make_subplots(specs=[[{"secondary_y": True}]])

    fig_fare_dur.add_trace(
        go.Bar(
            x=df_dur_fare["DURATION_BUCKET"],
            y=df_dur_fare["AVG_FARE"],
            name="Avg Fare ($)",
            marker=dict(color="#FF6B35", opacity=0.8),
        ),
        secondary_y=False,
    )

    fig_fare_dur.add_trace(
        go.Scatter(
            x=df_dur_fare["DURATION_BUCKET"],
            y=df_dur_fare["AVG_DISTANCE"],
            name="Avg Distance (mi)",
            mode="lines+markers",
            line=dict(color="#00D4AA", width=2.5),
            marker=dict(size=8, symbol="diamond"),
        ),
        secondary_y=True,
    )

    fig_fare_dur.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
        margin=dict(l=60, r=60, t=30, b=40),
    )
    fig_fare_dur.update_yaxes(
        title_text="Avg Fare ($)", secondary_y=False, gridcolor="rgba(255,255,255,0.05)"
    )
    fig_fare_dur.update_yaxes(
        title_text="Avg Distance (mi)", secondary_y=True, gridcolor="rgba(255,255,255,0.05)"
    )
    st.plotly_chart(fig_fare_dur, use_container_width=True)

# ── Speed Trend ──────────────────────────────────────────────────
st.subheader("Average Speed Trend (Daily)")

fig_speed_chart = go.Figure()
for taxi in df_speed["TAXI_TYPE"].unique():
    sub = df_speed[df_speed["TAXI_TYPE"] == taxi].sort_values("TRIP_DATE")
    fig_speed_chart.add_trace(
        go.Scatter(
            x=sub["TRIP_DATE"],
            y=sub["AVG_SPEED_MPH"],
            name=taxi.capitalize(),
            mode="lines",
            line=dict(color=taxi_colors.get(taxi, "#999"), width=2),
            fill="tozeroy",
            fillcolor=taxi_colors.get(taxi, "#999").replace(")", ",0.08)") if ")" in taxi_colors.get(taxi, "#999") else None,
            opacity=0.9,
        )
    )

# Add reference line at 12 mph (NYC avg)
fig_speed_chart.add_hline(
    y=12,
    line_dash="dash",
    line_color="rgba(255,255,255,0.3)",
    annotation_text="NYC Avg (12 mph)",
    annotation_position="top right",
    annotation_font=dict(size=10, color="rgba(255,255,255,0.5)"),
)

fig_speed_chart.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    yaxis_title="Avg Speed (mph)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    height=380,
    margin=dict(l=60, r=20, t=30, b=40),
)
fig_speed_chart.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
st.plotly_chart(fig_speed_chart, use_container_width=True)

# ── Vendor Performance ───────────────────────────────────────────
st.subheader("Vendor Performance Comparison")

vendor_agg = (
    df_vendor.groupby(["VENDOR_NAME", "TAXI_TYPE"], as_index=False)
    .agg(
        {
            "TRIP_COUNT": "sum",
            "TOTAL_REVENUE": "sum",
            "AVG_REVENUE_PER_TRIP": "mean",
            "AVG_TRIP_DISTANCE": "mean",
            "AVG_TIP_PCT": "mean",
            "AVG_DURATION_MIN": "mean",
        }
    )
)

# Radar / spider chart per vendor
vendors = vendor_agg["VENDOR_NAME"].unique()
metrics = ["AVG_REVENUE_PER_TRIP", "AVG_TRIP_DISTANCE", "AVG_TIP_PCT", "AVG_DURATION_MIN"]
metric_labels = ["Avg Rev/Trip ($)", "Avg Distance (mi)", "Avg Tip %", "Avg Duration (min)"]

vendor_colors = ["#FF6B35", "#00D4AA", "#FFD166", "#118AB2", "#EF476F"]

fig_radar = go.Figure()
for i, vendor in enumerate(vendors):
    sub = vendor_agg[vendor_agg["VENDOR_NAME"] == vendor][metrics].mean()
    # Normalize each metric 0-1 for radar
    fig_radar.add_trace(
        go.Scatterpolar(
            r=[sub[m] for m in metrics],
            theta=metric_labels,
            fill="toself",
            name=str(vendor),
            line=dict(color=vendor_colors[i % len(vendor_colors)]),
            fillcolor=vendor_colors[i % len(vendor_colors)].replace("#", "rgba(")
            if False
            else None,
            opacity=0.7,
        )
    )

fig_radar.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    polar=dict(
        bgcolor="rgba(0,0,0,0)",
        radialaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
        angularaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
    ),
    legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5),
    height=450,
    margin=dict(l=80, r=80, t=30, b=60),
)
st.plotly_chart(fig_radar, use_container_width=True)

# ── Vendor table ─────────────────────────────────────────────────
st.subheader("Vendor Summary Table")

vendor_table = (
    vendor_agg.groupby("VENDOR_NAME", as_index=False)
    .agg(
        {
            "TRIP_COUNT": "sum",
            "TOTAL_REVENUE": "sum",
            "AVG_REVENUE_PER_TRIP": "mean",
            "AVG_TIP_PCT": "mean",
        }
    )
    .sort_values("TOTAL_REVENUE", ascending=False)
)

vendor_table.columns = ["Vendor", "Trips", "Revenue ($)", "Avg Rev/Trip ($)", "Avg Tip %"]
vendor_table["Revenue ($)"] = vendor_table["Revenue ($)"].apply(lambda v: f"${v:,.0f}")
vendor_table["Trips"] = vendor_table["Trips"].apply(lambda v: f"{v:,.0f}")
vendor_table["Avg Rev/Trip ($)"] = vendor_table["Avg Rev/Trip ($)"].apply(lambda v: f"${v:,.2f}")
vendor_table["Avg Tip %"] = vendor_table["Avg Tip %"].apply(lambda v: f"{v:,.1f}%")

st.dataframe(vendor_table, use_container_width=True, hide_index=True)
