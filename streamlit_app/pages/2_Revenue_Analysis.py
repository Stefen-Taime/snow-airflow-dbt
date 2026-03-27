"""
Page 2 — Revenue Analysis
===========================
Daily revenue trends, zone revenue, rate code mix, and payment distribution.
Models: met_daily_revenue, met_revenue_by_zone, met_revenue_by_rate_code, met_payment_distribution
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from utils.snowflake_conn import run_query

st.header("Revenue Analysis")

# ── Data ─────────────────────────────────────────────────────────
df_daily = run_query("SELECT * FROM ANALYTICS.REVENUE.MET_DAILY_REVENUE ORDER BY TRIP_DATE")
df_zone = run_query("SELECT * FROM ANALYTICS.REVENUE.MET_REVENUE_BY_ZONE ORDER BY TOTAL_REVENUE DESC")
df_rate = run_query("SELECT * FROM ANALYTICS.REVENUE.MET_REVENUE_BY_RATE_CODE ORDER BY TRIP_MONTH")
df_pay = run_query("SELECT * FROM ANALYTICS.REVENUE.MET_PAYMENT_DISTRIBUTION ORDER BY TRIP_MONTH")

if df_daily.empty:
    st.warning("No revenue data available.")
    st.stop()

# ── KPIs ─────────────────────────────────────────────────────────
total_rev = df_daily["TOTAL_REVENUE"].sum()
total_trips = df_daily["TOTAL_TRIPS"].sum()
avg_rev_trip = df_daily["AVG_REVENUE_PER_TRIP"].mean()
total_tips = df_daily["TOTAL_TIPS"].sum()
avg_tip_pct = df_daily["AVG_TIP_PCT"].mean()
rev_per_mile = df_daily["REVENUE_PER_MILE"].mean()

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Total Revenue", f"${total_rev:,.0f}")
k2.metric("Total Trips", f"{total_trips:,.0f}")
k3.metric("Avg Rev / Trip", f"${avg_rev_trip:,.2f}")
k4.metric("Total Tips", f"${total_tips:,.0f}")
k5.metric("Avg Tip %", f"{avg_tip_pct:,.1f}%")
k6.metric("Rev / Mile", f"${rev_per_mile:,.2f}")

st.divider()

# ── Daily Revenue Trend (area chart) ────────────────────────────
st.subheader("Daily Revenue Trend")

df_daily_agg = df_daily.groupby("TRIP_DATE", as_index=False).agg(
    {"TOTAL_REVENUE": "sum", "TOTAL_TRIPS": "sum"}
)

fig_daily = make_subplots(specs=[[{"secondary_y": True}]])

fig_daily.add_trace(
    go.Scatter(
        x=df_daily_agg["TRIP_DATE"],
        y=df_daily_agg["TOTAL_REVENUE"],
        name="Revenue",
        fill="tozeroy",
        fillcolor="rgba(255,107,53,0.15)",
        line=dict(color="#FF6B35", width=2),
        mode="lines",
    ),
    secondary_y=False,
)

fig_daily.add_trace(
    go.Scatter(
        x=df_daily_agg["TRIP_DATE"],
        y=df_daily_agg["TOTAL_TRIPS"],
        name="Trips",
        line=dict(color="#00D4AA", width=1.5, dash="dot"),
        mode="lines",
    ),
    secondary_y=True,
)

fig_daily.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    height=400,
    margin=dict(l=60, r=60, t=30, b=40),
)
fig_daily.update_yaxes(title_text="Revenue ($)", secondary_y=False, gridcolor="rgba(255,255,255,0.05)")
fig_daily.update_yaxes(title_text="Trips", secondary_y=True, gridcolor="rgba(255,255,255,0.05)")
st.plotly_chart(fig_daily, use_container_width=True)

# ── Revenue Breakdown (fee components) ───────────────────────────
st.subheader("Revenue Component Breakdown (Daily)")

df_comp = df_daily.groupby("TRIP_DATE", as_index=False).agg(
    {
        "TOTAL_FARES": "sum",
        "TOTAL_TIPS": "sum",
        "TOTAL_TOLLS": "sum",
        "TOTAL_CONGESTION_SURCHARGES": "sum",
        "TOTAL_AIRPORT_FEES": "sum",
        "TOTAL_CBD_FEES": "sum",
    }
)

fig_stack = go.Figure()
components = [
    ("TOTAL_FARES", "Fares", "#FF6B35"),
    ("TOTAL_TIPS", "Tips", "#00D4AA"),
    ("TOTAL_TOLLS", "Tolls", "#FFD166"),
    ("TOTAL_CONGESTION_SURCHARGES", "Congestion Surcharge", "#EF476F"),
    ("TOTAL_AIRPORT_FEES", "Airport Fees", "#118AB2"),
    ("TOTAL_CBD_FEES", "CBD Fees", "#8338EC"),
]

for col, name, color in components:
    fig_stack.add_trace(
        go.Scatter(
            x=df_comp["TRIP_DATE"],
            y=df_comp[col],
            name=name,
            stackgroup="one",
            line=dict(width=0.5, color=color),
            fillcolor=color.replace(")", ",0.6)").replace("rgb", "rgba") if "rgb" in color else color,
        )
    )

fig_stack.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    yaxis_title="Revenue ($)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    height=400,
    margin=dict(l=60, r=20, t=30, b=40),
)
fig_stack.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
st.plotly_chart(fig_stack, use_container_width=True)

# ── Top Zones & Rate Code (side by side) ────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Top 15 Zones by Revenue")
    top_zones = (
        df_zone.groupby(["PICKUP_BOROUGH", "PICKUP_ZONE"], as_index=False)
        .agg({"TOTAL_REVENUE": "sum", "TRIP_COUNT": "sum"})
        .nlargest(15, "TOTAL_REVENUE")
    )

    fig_zones = go.Figure(
        go.Bar(
            y=top_zones["PICKUP_ZONE"],
            x=top_zones["TOTAL_REVENUE"],
            orientation="h",
            marker=dict(
                color=top_zones["TOTAL_REVENUE"],
                colorscale="OrRd",
                line=dict(width=0),
            ),
            text=top_zones["TOTAL_REVENUE"].apply(lambda v: f"${v:,.0f}"),
            textposition="auto",
            textfont=dict(size=10),
        )
    )
    fig_zones.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(autorange="reversed"),
        xaxis_title="Total Revenue ($)",
        height=500,
        margin=dict(l=180, r=20, t=10, b=40),
    )
    fig_zones.update_xaxes(gridcolor="rgba(255,255,255,0.05)")
    st.plotly_chart(fig_zones, use_container_width=True)

with col_right:
    st.subheader("Revenue by Rate Code")
    df_rate_agg = (
        df_rate.groupby("RATE_CODE_NAME", as_index=False)
        .agg({"TOTAL_REVENUE": "sum", "TRIP_COUNT": "sum"})
        .sort_values("TOTAL_REVENUE", ascending=False)
    )

    fig_rate = go.Figure(
        go.Pie(
            labels=df_rate_agg["RATE_CODE_NAME"],
            values=df_rate_agg["TOTAL_REVENUE"],
            hole=0.5,
            marker=dict(
                colors=px.colors.sequential.Sunset,
                line=dict(color="#1A1F2E", width=2),
            ),
            textinfo="percent+label",
            textfont=dict(size=11),
        )
    )
    fig_rate.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        height=500,
        margin=dict(l=20, r=20, t=10, b=20),
    )
    st.plotly_chart(fig_rate, use_container_width=True)

# ── Payment Distribution ────────────────────────────────────────
st.subheader("Payment Method Distribution Over Time")

df_pay_pivot = df_pay.pivot_table(
    index="TRIP_MONTH", columns="PAYMENT_TYPE_NAME", values="TRIP_COUNT", aggfunc="sum"
).fillna(0)

# Normalize to 100%
df_pay_pct = df_pay_pivot.div(df_pay_pivot.sum(axis=1), axis=0) * 100

pay_colors = ["#FF6B35", "#00D4AA", "#FFD166", "#118AB2", "#EF476F", "#8338EC"]
fig_pay = go.Figure()

for i, col in enumerate(df_pay_pct.columns):
    fig_pay.add_trace(
        go.Bar(
            x=df_pay_pct.index,
            y=df_pay_pct[col],
            name=col,
            marker=dict(color=pay_colors[i % len(pay_colors)]),
        )
    )

fig_pay.update_layout(
    barmode="stack",
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    yaxis_title="% of Trips",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    height=380,
    margin=dict(l=60, r=20, t=30, b=40),
)
fig_pay.update_yaxes(gridcolor="rgba(255,255,255,0.05)")
st.plotly_chart(fig_pay, use_container_width=True)
