import streamlit as st
import pandas as pd
import requests
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NewsPulse")
    .master("local[*]")
    .getOrCreate()
)

st.set_page_config(page_title="News Pulse", layout="wide")
st.title("News Pulse - Live Dashboard")

def safe_sql(query):
    try:
        return spark.sql(query).toPandas()
    except Exception:
        return pd.DataFrame()

def make_summary(keywords):
    if not keywords:
        return "No keywords available yet. The dashboard is waiting for live RSS data."

    prompt = f"""
    Write one paragraph, max 80 words, summarizing the main news themes.
    Mention at least three named storylines if possible.
    Use these keywords: {", ".join(keywords)}
    """

    try:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise Exception("No API key")

        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 120
            },
            timeout=10
        )

        return response.json()["choices"][0]["message"]["content"]

    except Exception:
        return "Current news pulse is driven by keywords such as: " + ", ".join(keywords[:15]) + "."

by_source = safe_sql("SELECT * FROM by_source ORDER BY count DESC")
by_window = safe_sql("""
    SELECT window.start AS hour, count 
    FROM by_window 
    ORDER BY hour
""")
top_words = safe_sql("SELECT * FROM top_words ORDER BY count DESC LIMIT 10")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Headline Count by Source")
    if not by_source.empty:
        st.bar_chart(by_source.set_index("source"))
    else:
        st.info("Waiting for source data...")

with col2:
    st.subheader("Headline Volume by Hour")
    if not by_window.empty:
        st.line_chart(by_window.set_index("hour"))
    else:
        st.info("Waiting for windowed data...")

st.subheader("Top Keywords")
if not top_words.empty:
    st.dataframe(top_words)
else:
    st.info("Waiting for keyword data...")

keywords = top_words["word"].tolist() if not top_words.empty else []

st.subheader("LLM News Summary")
st.write(make_summary(keywords))

st.caption("Refresh the page every few seconds to see updates.")
