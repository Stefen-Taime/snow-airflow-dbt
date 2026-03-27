"""
Page 4 — Congestion Pricing Impact
====================================
CBD congestion fee analytics: daily impact, CBD vs Non-CBD, peak/off-peak, yellow vs green.
Models: met_cbd_daily_impact, met_cbd_vs_non_cbd, met_cbd_peak_offpeak, met_cbd_yellow_vs_green
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from utils.snowflake_conn import run_query

st.header("Congestion Pricing Impact (CBD)")

# ── Data ─────────────────────────────────────────────────────────
df_cbd = run_query("SELECT * FROM ANALYTICS.PUBLIC_CONGESTION.MET_CBD_DAILY_IMPACT ORDER BY TRIP_DATE")
df_compare = run_query("SELECT * FROM ANALYTICS.PUBLIC_CONGESTION.MET_CBD_VS_NON_CBD ORDER BY TRIP_MONTH")
df_peak = run_query("SELECT * FROM ANALYTICS.PUBLIC_CONGESTION.MET_CBD_PEAK_OFFPEAK ORDER BY TRIP_MONTH")
df_yg = run_query("SELECT * FROM ANALYTICS.PUBLIC_CONGESTION.MET_CBD_YELLOW_VS_GREEN ORDER BY TRIP_MONTH")

if df_cbd.empty:
    st.warning("No congestion data available.")
    st.stop()

# ── KPIs ─────────────────────────────────────────────────────────
total_cbd_fees = df_cbd["TOTAL_CBD_FEES"].sum()
avg_cbd_pct = df_cbd["CBD_TRIP_PCT"].mean()
avg_cbd_fee = df_cbd["AVG_CBD_FEE"].mean()
cbd_zone_speed = (
    df_compare[df_compare["ZONE_CATEGORY"] == "CBD Zone"]["AVG_SPEED_MPH"].mean()
    if "CBD Zone" in df_compare["ZONE_CATEGORY"].values
    else 0
)
total_cbd_trips = df_cbd["CBD_TRIPS"].sum()
rev_per_mile_cbd = df_cbd["REVENUE_PER_MILE"].mean()

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Total CBD Fees", f"${total_cbd_fees:,.0f}")
k2.metric("% Trips in CBD", f"{avg_cbd_pct:.1f}%")
k3.metric("Avg CBD Fee", f"${avg_cbd_fee:.2f}")
k4.metric("CBD Avg Speed", f"{cbd_zone_speed:.1f} mph")
k5.metric("Total CBD Trips", f"{total_cbd_trips:,.0f}")
k6.metric("CBD Rev/Mile", f"${rev_per_mile_cbd:.2f}")

st.divider()

# ── CBD Daily Impact Trend ───────────────────────────────────────
st.subheader("CBD Trip Volume & Fee Collection (Daily)")

fig_impact = make_subplots(specs=[[{"secondary_y": True}]])

# Aggregate across taxi types
df_cbd_agg = df_cbd.groupby("TRIP_DATE", as_index=False).agg(
    {"CBD_TRIPS": "sum", "TOTAL_CBD_FEES": "sum", "CBD_TRIP_PCT": "mean"}
)

fig_impact.add_trace(
    go.Bar(
        x=df_cbd_agg["TRIP_DATE"],
        y=df_cbd_agg["CBD_TRIPS"],
        name="CBD Trips",
        marker=dict(color="#FF6B35", opacity=0.7),
    ),
    secondary_y=False,
)

fig_impact.add_trace(
    go.Scatter(
        x=df_cbd_agg["TRIP_DATE"],
        y=df_cbd_agg["TOTAL_CBD_FEES"],
        name="CBD Fees ($)",
        mode="lines+markers",
        line=dict(color="#00D4AA", width=2.5),
        marker=dict(size=5),
    ),
    secondary_y=True,
)

fig_impact.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    height=400,
    margin=dict(l=60, r=60, t=30, b=40),
)
fig_impact.update_yaxes(title_text="CBD Trips", secondary_y=False, gridcolor="rgba(255,255,255,0.05)")
fig_impact.update_yaxes(title_text="CBD Fees ($)", secondary_y=True, gridcolor="rgba(255,255,255,0.05)")
st.plotly_chart(fig_impact, use_container_width=True)

# ── CBD vs Non-CBD Comparison ────────────────────────────────────
st.subheader("CBD vs Non-CBD Zone Comparison")

col_left, col_right = st.columns(2)

with col_left:
    fig_rev_comp = px.bar(
        df_compare,
        x="TRIP_MONTH",
        y="AVG_REVENUE_PER_TRIP",
        color="ZONE_CATEGORY",
        barmode="group",
        facet_col="TAXI_TYPE",
        color_discrete_map={"CBD Zone": "#FF6B35", "Non-CBD Zone": "#118AB2"},
    )
    fig_rev_comp.update_layout(
        title=dict(text="Avg Revenue / Trip: CBD vs Non-CBD", font=dict(size=14)),
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=380,
        margin=dict(l=60, r=20, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="center", x=0.5),
    )
    fig_rev_comp.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
    st.plotly_chart(fig_rev_comp, use_container_width=True)

with col_right:
    fig_speed = px.bar(
        df_compare,
        x="TRIP_MONTH",
        y="AVG_SPEED_MPH",
        color="ZONE_CATEGORY",
        barmode="group",
        facet_col="TAXI_TYPE",
        color_discrete_map={"CBD Zone": "#EF476F", "Non-CBD Zone": "#00D4AA"},
    )
    fig_speed.update_layout(
        title=dict(text="Avg Speed: CBD vs Non-CBD (mph)", font=dict(size=14)),
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=380,
        margin=dict(l=60, r=20, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="center", x=0.5),
    )
    fig_speed.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
    st.plotly_chart(fig_speed, use_container_width=True)

# ── Peak vs Off-Peak ─────────────────────────────────────────────
st.subheader("CBD Fees: Peak vs Off-Peak")

df_peak_agg = df_peak.groupby(["TRIP_MONTH", "TIME_CATEGORY"], as_index=False).agg(
    {"TOTAL_CBD_FEES": "sum", "CBD_TRIPS": "sum", "AVG_CBD_FEE": "mean"}
)

peak_colors = {
    "Peak (Weekday 5am-9pm)": "#FF6B35",
    "Peak (Weekend 9am-9pm)": "#FFD166",
    "Off-Peak": "#118AB2",
}

fig_peak = go.Figure()
for cat in df_peak_agg["TIME_CATEGORY"].unique():
    sub = df_peak_agg[df_peak_agg["TIME_CATEGORY"] == cat]
    fig_peak.add_trace(
        go.Bar(
            x=sub["TRIP_MONTH"],
            y=sub["TOTAL_CBD_FEES"],
            name=cat,
            marker=dict(color=peak_colors.get(cat, "#999")),
        )
    )

fig_peak.update_layout(
    barmode="stack",
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    yaxis_title="Total CBD Fees ($)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    height=400,
    margin=dict(l=60, r=20, t=30, b=40),
)
fig_peak.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
st.plotly_chart(fig_peak, use_container_width=True)

# ── Yellow vs Green in CBD ───────────────────────────────────────
st.subheader("Yellow vs Green: CBD Penetration")

col_y, col_g = st.columns(2)

taxi_colors = {"yellow": "#F5C518", "green": "#00A67E"}

with col_y:
    fig_yg_pct = go.Figure()
    for taxi in df_yg["TAXI_TYPE"].unique():
        sub = df_yg[df_yg["TAXI_TYPE"] == taxi].sort_values("TRIP_MONTH")
        fig_yg_pct.add_trace(
            go.Scatter(
                x=sub["TRIP_MONTH"],
                y=sub["CBD_TRIP_PCT"],
                name=taxi.capitalize(),
                mode="lines+markers",
                line=dict(color=taxi_colors.get(taxi, "#999"), width=2.5),
                marker=dict(size=7),
            )
        )
    fig_yg_pct.update_layout(
        title=dict(text="% CBD Trips by Taxi Type", font=dict(size=14)),
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="% CBD Trips",
        height=380,
        margin=dict(l=60, r=20, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig_yg_pct.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
    st.plotly_chart(fig_yg_pct, use_container_width=True)

with col_g:
    fig_yg_rev = go.Figure()
    for taxi in df_yg["TAXI_TYPE"].unique():
        sub = df_yg[df_yg["TAXI_TYPE"] == taxi].sort_values("TRIP_MONTH")
        fig_yg_rev.add_trace(
            go.Bar(
                x=sub["TRIP_MONTH"],
                y=sub["TOTAL_CBD_FEES"],
                name=taxi.capitalize(),
                marker=dict(color=taxi_colors.get(taxi, "#999"), opacity=0.85),
            )
        )
    fig_yg_rev.update_layout(
        title=dict(text="CBD Fee Revenue by Taxi Type", font=dict(size=14)),
        barmode="group",
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="CBD Fees ($)",
        height=380,
        margin=dict(l=60, r=20, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig_yg_rev.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
    st.plotly_chart(fig_yg_rev, use_container_width=True)
