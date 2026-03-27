"""
Snowflake connection manager using st.connection (native Streamlit connector).
All queries go through pre-aggregated mart tables — no heavy scans.
"""

import streamlit as st
import pandas as pd


@st.cache_resource
def get_connection():
    """Return a cached Snowflake connection via st.connection."""
    return st.connection("snowflake")


def run_query(sql: str, ttl: int = 600) -> pd.DataFrame:
    """Execute a read-only query with TTL-based caching (default 10 min).

    Parameters
    ----------
    sql : str
        SQL SELECT statement targeting an ANALYTICS mart table.
    ttl : int
        Cache time-to-live in seconds.

    Returns
    -------
    pd.DataFrame
    """
    conn = get_connection()
    return conn.query(sql, ttl=ttl)
