"""
Page 3 — Geographic Intelligence
==================================
Zone-level analytics: pickup rankings, zone-pair routes, borough summary, airport traffic.
Models: met_zone_pickup_ranking, met_zone_pair_analysis, met_borough_summary, met_airport_trips
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from utils.snowflake_conn import run_query

st.header("Geographic Intelligence")

# ── Data ─────────────────────────────────────────────────────────
df_ranking = run_query("SELECT * FROM ANALYTICS.GEOGRAPHIC.MET_ZONE_PICKUP_RANKING ORDER BY TRIP_COUNT DESC")
df_pairs = run_query("SELECT * FROM ANALYTICS.GEOGRAPHIC.MET_ZONE_PAIR_ANALYSIS ORDER BY TRIP_COUNT DESC")
df_borough = run_query("SELECT * FROM ANALYTICS.GEOGRAPHIC.MET_BOROUGH_SUMMARY ORDER BY TRIP_MONTH")
df_airport = run_query("SELECT * FROM ANALYTICS.GEOGRAPHIC.MET_AIRPORT_TRIPS ORDER BY TRIP_MONTH")

if df_ranking.empty:
    st.warning("No geographic data available.")
    st.stop()

# ── KPIs ─────────────────────────────────────────────────────────
unique_zones = df_ranking["PICKUP_ZONE"].nunique()
total_boroughs = df_borough["PICKUP_BOROUGH"].nunique()
top_zone = df_ranking.groupby("PICKUP_ZONE")["TRIP_COUNT"].sum().idxmax()
airport_trips = df_airport["TRIP_COUNT"].sum()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Active Zones", f"{unique_zones}")
k2.metric("Boroughs", f"{total_boroughs}")
k3.metric("Top Zone", top_zone)
k4.metric("Airport Trips", f"{airport_trips:,.0f}")

st.divider()

# ── Top 20 Pickup Zones (horizontal bar) ────────────────────────
st.subheader("Top 20 Pickup Zones")

top20 = (
    df_ranking.groupby(["PICKUP_BOROUGH", "PICKUP_ZONE"], as_index=False)
    .agg({"TRIP_COUNT": "sum", "TOTAL_REVENUE": "sum"})
    .nlargest(20, "TRIP_COUNT")
    .sort_values("TRIP_COUNT", ascending=True)
)

fig_top = go.Figure()
fig_top.add_trace(
    go.Bar(
        y=top20["PICKUP_ZONE"],
        x=top20["TRIP_COUNT"],
        orientation="h",
        marker=dict(
            color=top20["TRIP_COUNT"],
            colorscale="Turbo",
            colorbar=dict(title="Trips"),
            line=dict(width=0),
        ),
        text=top20["TRIP_COUNT"].apply(lambda v: f"{v:,.0f}"),
        textposition="outside",
        textfont=dict(size=10),
    )
)
fig_top.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    xaxis_title="Total Trips",
    height=550,
    margin=dict(l=200, r=80, t=10, b=40),
)
fig_top.update_xaxes(gridcolor="rgba(255,255,255,0.05)")
st.plotly_chart(fig_top, use_container_width=True)

# ── Borough Summary (side by side) ──────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Borough Trip Volume Trend")

    borough_trend = df_borough.groupby(["TRIP_MONTH", "PICKUP_BOROUGH"], as_index=False).agg(
        {"TRIP_COUNT": "sum"}
    )
    borough_colors = {
        "Manhattan": "#FF6B35",
        "Brooklyn": "#00D4AA",
        "Queens": "#FFD166",
        "Bronx": "#EF476F",
        "Staten Island": "#118AB2",
        "EWR": "#8338EC",
        "Unknown": "#666",
    }

    fig_bor = go.Figure()
    for borough in borough_trend["PICKUP_BOROUGH"].unique():
        sub = borough_trend[borough_trend["PICKUP_BOROUGH"] == borough]
        fig_bor.add_trace(
            go.Scatter(
                x=sub["TRIP_MONTH"],
                y=sub["TRIP_COUNT"],
                name=borough,
                mode="lines+markers",
                line=dict(color=borough_colors.get(borough, "#999"), width=2),
                marker=dict(size=6),
            )
        )
    fig_bor.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="Trips",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        height=420,
        margin=dict(l=60, r=20, t=30, b=40),
    )
    fig_bor.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
    st.plotly_chart(fig_bor, use_container_width=True)

with col_right:
    st.subheader("Avg Revenue per Trip by Borough")

    borough_rev = (
        df_borough.groupby("PICKUP_BOROUGH", as_index=False)
        .agg({"AVG_REVENUE_PER_TRIP": "mean", "TRIP_COUNT": "sum"})
        .sort_values("AVG_REVENUE_PER_TRIP", ascending=False)
    )

    fig_bor_rev = go.Figure(
        go.Bar(
            x=borough_rev["PICKUP_BOROUGH"],
            y=borough_rev["AVG_REVENUE_PER_TRIP"],
            marker=dict(
                color=borough_rev["AVG_REVENUE_PER_TRIP"],
                colorscale="Sunset",
                line=dict(width=0),
            ),
            text=borough_rev["AVG_REVENUE_PER_TRIP"].apply(lambda v: f"${v:,.2f}"),
            textposition="outside",
            textfont=dict(size=11),
        )
    )
    fig_bor_rev.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="Avg Rev / Trip ($)",
        height=420,
        margin=dict(l=60, r=20, t=30, b=40),
    )
    fig_bor_rev.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
    st.plotly_chart(fig_bor_rev, use_container_width=True)

# ── Top Zone Pairs (Sankey-style table) ──────────────────────────
st.subheader("Top 15 Route Corridors (Pickup → Dropoff)")

top_pairs = (
    df_pairs.groupby(["PICKUP_ZONE", "DROPOFF_ZONE"], as_index=False)
    .agg({"TRIP_COUNT": "sum", "AVG_FARE": "mean", "AVG_DISTANCE": "mean", "AVG_DURATION_MIN": "mean"})
    .nlargest(15, "TRIP_COUNT")
)
top_pairs["ROUTE"] = top_pairs["PICKUP_ZONE"] + " → " + top_pairs["DROPOFF_ZONE"]

fig_routes = go.Figure(
    go.Bar(
        y=top_pairs.sort_values("TRIP_COUNT", ascending=True)["ROUTE"],
        x=top_pairs.sort_values("TRIP_COUNT", ascending=True)["TRIP_COUNT"],
        orientation="h",
        marker=dict(
            color=top_pairs.sort_values("TRIP_COUNT", ascending=True)["AVG_FARE"],
            colorscale="Viridis",
            colorbar=dict(title="Avg Fare ($)"),
            line=dict(width=0),
        ),
        text=top_pairs.sort_values("TRIP_COUNT", ascending=True)["TRIP_COUNT"].apply(
            lambda v: f"{v:,.0f}"
        ),
        textposition="outside",
        textfont=dict(size=10),
    )
)
fig_routes.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    xaxis_title="Total Trips",
    height=520,
    margin=dict(l=320, r=80, t=10, b=40),
)
fig_routes.update_xaxes(gridcolor="rgba(255,255,255,0.05)")
st.plotly_chart(fig_routes, use_container_width=True)

# ── Airport Traffic ──────────────────────────────────────────────
st.subheader("Airport Trip Analysis")

col_a, col_b = st.columns(2)

with col_a:
    airport_by_dir = (
        df_airport.groupby(["AIRPORT_NAME", "TRIP_DIRECTION"], as_index=False)
        .agg({"TRIP_COUNT": "sum", "TOTAL_REVENUE": "sum"})
    )

    fig_air = px.sunburst(
        airport_by_dir,
        path=["AIRPORT_NAME", "TRIP_DIRECTION"],
        values="TRIP_COUNT",
        color="TOTAL_REVENUE",
        color_continuous_scale="OrRd",
    )
    fig_air.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        height=400,
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig_air, use_container_width=True)

with col_b:
    airport_trend = df_airport.groupby(["TRIP_MONTH", "AIRPORT_NAME"], as_index=False).agg(
        {"TRIP_COUNT": "sum"}
    )
    air_colors = {"JFK Airport": "#FF6B35", "LaGuardia Airport": "#00D4AA"}

    fig_air_trend = go.Figure()
    for airport in airport_trend["AIRPORT_NAME"].unique():
        sub = airport_trend[airport_trend["AIRPORT_NAME"] == airport]
        fig_air_trend.add_trace(
            go.Scatter(
                x=sub["TRIP_MONTH"],
                y=sub["TRIP_COUNT"],
                name=str(airport),
                mode="lines+markers",
                line=dict(color=air_colors.get(str(airport), "#999"), width=2.5),
                marker=dict(size=7),
                fill="tozeroy",
                fillcolor=air_colors.get(str(airport), "#999").replace(")", ",0.1)"),
            )
        )
    fig_air_trend.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="Airport Trips",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        height=400,
        margin=dict(l=60, r=20, t=30, b=40),
    )
    fig_air_trend.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
    st.plotly_chart(fig_air_trend, use_container_width=True)
