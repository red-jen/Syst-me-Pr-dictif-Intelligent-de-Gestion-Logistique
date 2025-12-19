import os
import pymongo
import streamlit as st
import pandas as pd
import time
from datetime import datetime

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "logistics")
MONGO_COLLECTION_AGG = os.environ.get("MONGO_COLLECTION_AGG", "aggregates")
REFRESH_SEC = int(os.environ.get("DASH_REFRESH_SEC", "5"))

st.set_page_config(page_title="Streaming Late Delivery Risk", layout="wide")
st.title("Streaming Late Delivery Risk Dashboard")

# Auto-refresh logic using native Streamlit components
if 'auto_refresh' not in st.session_state:
    st.session_state.auto_refresh = True

auto_refresh = st.checkbox('Enable Auto-Refresh', value=st.session_state.auto_refresh)

if auto_refresh:
    time.sleep(REFRESH_SEC)
    st.rerun()

@st.cache_resource
def get_collection():
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        return client[MONGO_DB][MONGO_COLLECTION_AGG]
    except Exception as e:
        st.error(f"Mongo connection failed: {e}")
        return None

# Auto refresh (safe for current Streamlit)
# st_autorefresh(interval=REFRESH_SEC * 1000, key="auto_refresh")

col = get_collection()
if col is None:
    st.stop()

try:
    docs = list(col.find().sort("window_start", -1).limit(500))
except Exception as e:
    st.error(f"Query error: {e}")
    st.stop()

if not docs:
    st.info("No aggregate data yet. Waiting for streaming job...")
    st.stop()

df = pd.DataFrame(docs)

for c in ["window_start", "window_end"]:
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce")

st.subheader("Recent Window Aggregates (last 50)")
core_cols = [c for c in ["window_start", "window_end", "Market", "avg_late_prob", "events"] if c in df.columns]
st.dataframe(df[core_cols].head(50))

if {"window_start", "avg_late_prob", "Market"}.issubset(df.columns):
    latest_start = df["window_start"].max()
    latest = df[df["window_start"] == latest_start]
    if not latest.empty:
        st.subheader(f"Average Late Probability by Market (Latest Window: {latest_start})")
        st.bar_chart(latest.set_index("Market")["avg_late_prob"])

if {"window_start", "avg_late_prob"}.issubset(df.columns):
    trend = df.groupby("window_start")["avg_late_prob"].mean().sort_index()
    st.subheader("Trend of Avg Late Probability (All Markets)")
    st.line_chart(trend)

st.caption(f"Source: MongoDB {MONGO_URI} • Documents loaded: {len(df)} • Auto-refresh: {REFRESH_SEC}s")
