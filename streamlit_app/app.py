"""
NYC TLC Taxi Analytics Dashboard
=================================
Entry point for the Streamlit multi-page application.
Connects to Snowflake pre-aggregated dbt mart tables.
"""

import streamlit as st

st.set_page_config(
    page_title="NYC TLC Taxi Analytics",
    page_icon="🚖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar branding ─────────────────────────────────────────────
st.sidebar.markdown(
    """
    <div style="text-align:center; padding: 1rem 0 0.5rem 0;">
        <h2 style="margin:0; color:#FF6B35;">NYC TLC</h2>
        <p style="margin:0; font-size:0.85rem; color:#999;">Taxi & Limousine Analytics</p>
    </div>
    <hr style="margin:0.5rem 0; border-color:#333;">
    """,
    unsafe_allow_html=True,
)

# ── Navigation pages ─────────────────────────────────────────────
executive = st.Page("pages/1_Executive_Overview.py", title="Executive Overview", icon="📊")
revenue = st.Page("pages/2_Revenue_Analysis.py", title="Revenue Analysis", icon="💰")
geographic = st.Page("pages/3_Geographic_Intel.py", title="Geographic Intel", icon="🗺️")
congestion = st.Page("pages/4_Congestion_Pricing.py", title="Congestion Pricing", icon="🚦")
operations = st.Page("pages/5_Operations.py", title="Operations", icon="⚙️")

pg = st.navigation(
    {
        "Dashboards": [executive, revenue, geographic, congestion, operations],
    }
)

pg.run()
