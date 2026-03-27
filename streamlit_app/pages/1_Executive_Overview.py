"""
Page 1 — Executive Overview
============================
High-level KPIs and trend lines from met_executive_summary.
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.snowflake_conn import run_query

# ── Page config ──────────────────────────────────────────────────
st.header("Executive Overview")
st.caption("Monthly aggregates from `ANALYTICS.EXECUTIVE.MET_EXECUTIVE_SUMMARY`")

# ── Data ─────────────────────────────────────────────────────────
df = run_query("""
    SELECT *
    FROM ANALYTICS.EXECUTIVE.MET_EXECUTIVE_SUMMARY
    ORDER BY TRIP_MONTH
""")

if df.empty:
    st.warning("No data returned from met_executive_summary.")
    st.stop()

# Totals across all months
total_trips = df["TOTAL_TRIPS"].sum()
total_revenue = df["TOTAL_REVENUE"].sum()
avg_fare = df["AVG_FARE"].mean()
total_cbd = df["TOTAL_CBD_FEES"].sum()
avg_distance = df["AVG_TRIP_DISTANCE"].mean()
avg_passengers = df["AVG_PASSENGER_COUNT"].mean()

# ── KPI row ──────────────────────────────────────────────────────
k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Total Trips", f"{total_trips:,.0f}")
k2.metric("Total Revenue", f"${total_revenue:,.0f}")
k3.metric("Avg Fare", f"${avg_fare:,.2f}")
k4.metric("CBD Fees Collected", f"${total_cbd:,.0f}")
k5.metric("Avg Distance", f"{avg_distance:,.1f} mi")
k6.metric("Avg Passengers", f"{avg_passengers:,.1f}")

st.divider()

# ── Monthly Trips & Revenue (dual-axis) ─────────────────────────
df_agg = df.groupby("TRIP_MONTH", as_index=False).agg(
    {"TOTAL_TRIPS": "sum", "TOTAL_REVENUE": "sum"}
)

fig = make_subplots(specs=[[{"secondary_y": True}]])

fig.add_trace(
    go.Bar(
        x=df_agg["TRIP_MONTH"],
        y=df_agg["TOTAL_TRIPS"],
        name="Trips",
        marker=dict(
            color=df_agg["TOTAL_TRIPS"],
            colorscale="Oranges",
            line=dict(width=0),
        ),
        opacity=0.85,
    ),
    secondary_y=False,
)

fig.add_trace(
    go.Scatter(
        x=df_agg["TRIP_MONTH"],
        y=df_agg["TOTAL_REVENUE"],
        name="Revenue ($)",
        mode="lines+markers",
        line=dict(color="#FF6B35", width=3),
        marker=dict(size=8, symbol="diamond"),
    ),
    secondary_y=True,
)

fig.update_layout(
    title=dict(text="Monthly Trip Volume & Revenue", font=dict(size=18)),
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(l=60, r=60, t=60, b=40),
    height=420,
)
fig.update_yaxes(title_text="Trips", secondary_y=False, gridcolor="rgba(255,255,255,0.05)")
fig.update_yaxes(title_text="Revenue ($)", secondary_y=True, gridcolor="rgba(255,255,255,0.05)")

st.plotly_chart(fig, use_container_width=True)

# ── Yellow vs Green breakdown ────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    df_type = df.groupby("TAXI_TYPE", as_index=False).agg({"TOTAL_TRIPS": "sum"})
    colors = {"yellow": "#F5C518", "green": "#00A67E"}

    fig_pie = go.Figure(
        go.Pie(
            labels=df_type["TAXI_TYPE"],
            values=df_type["TOTAL_TRIPS"],
            hole=0.55,
            marker=dict(colors=[colors.get(t, "#666") for t in df_type["TAXI_TYPE"]]),
            textinfo="percent+label",
            textfont=dict(size=13),
        )
    )
    fig_pie.update_layout(
        title=dict(text="Trip Share by Taxi Type", font=dict(size=16)),
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        height=370,
        margin=dict(l=20, r=20, t=50, b=20),
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with col_right:
    fig_avg = go.Figure()
    for taxi in df["TAXI_TYPE"].unique():
        sub = df[df["TAXI_TYPE"] == taxi].sort_values("TRIP_MONTH")
        fig_avg.add_trace(
            go.Scatter(
                x=sub["TRIP_MONTH"],
                y=sub["AVG_FARE"],
                name=taxi.capitalize(),
                mode="lines+markers",
                line=dict(
                    color=colors.get(taxi, "#666"),
                    width=2.5,
                ),
                marker=dict(size=7),
            )
        )
    fig_avg.update_layout(
        title=dict(text="Avg Fare Trend by Taxi Type", font=dict(size=16)),
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="Avg Fare ($)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=370,
        margin=dict(l=60, r=20, t=50, b=40),
    )
    fig_avg.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
    st.plotly_chart(fig_avg, use_container_width=True)

# ── CBD Adoption ─────────────────────────────────────────────────
st.subheader("CBD Congestion Fee Adoption")

df_cbd = df.groupby("TRIP_MONTH", as_index=False).agg(
    {"CBD_TRIPS": "sum", "TOTAL_TRIPS": "sum", "TOTAL_CBD_FEES": "sum"}
)
df_cbd["CBD_PCT"] = df_cbd["CBD_TRIPS"] / df_cbd["TOTAL_TRIPS"] * 100

fig_cbd = make_subplots(specs=[[{"secondary_y": True}]])

fig_cbd.add_trace(
    go.Bar(
        x=df_cbd["TRIP_MONTH"],
        y=df_cbd["TOTAL_CBD_FEES"],
        name="CBD Fees ($)",
        marker=dict(color="#FF6B35", opacity=0.8),
    ),
    secondary_y=False,
)

fig_cbd.add_trace(
    go.Scatter(
        x=df_cbd["TRIP_MONTH"],
        y=df_cbd["CBD_PCT"],
        name="% CBD Trips",
        mode="lines+markers",
        line=dict(color="#00D4AA", width=2.5, dash="dot"),
        marker=dict(size=8),
    ),
    secondary_y=True,
)

fig_cbd.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    height=380,
    margin=dict(l=60, r=60, t=30, b=40),
)
fig_cbd.update_yaxes(title_text="CBD Fees ($)", secondary_y=False, gridcolor="rgba(255,255,255,0.05)")
fig_cbd.update_yaxes(title_text="% CBD Trips", secondary_y=True, gridcolor="rgba(255,255,255,0.05)")

st.plotly_chart(fig_cbd, use_container_width=True)
