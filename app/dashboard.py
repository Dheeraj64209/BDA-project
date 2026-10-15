"""Category-level dashboard for the weekly return rates computed by the MapReduce job."""
import altair as alt
import pandas as pd
import streamlit as st
from hdfs import InsecureClient

from common import HDFS_URL, HDFS_USER, OUTPUT_DIR, THRESHOLD

COLUMNS = ["week", "category", "orders", "returns", "return_rate", "status"]


@st.cache_data(ttl=30)
def load_results():
    """Read the MapReduce output (part-r-* files) from HDFS. Returns None until the job has run."""
    hdfs = InsecureClient(HDFS_URL, user=HDFS_USER)
    if not hdfs.status(f"{OUTPUT_DIR}/_SUCCESS", strict=False):
        return None
    rows = []
    for name in sorted(hdfs.list(OUTPUT_DIR)):
        if name.startswith("part-"):
            with hdfs.read(f"{OUTPUT_DIR}/{name}", encoding="utf-8") as reader:
                rows += [line.split("\t") for line in reader.read().splitlines() if line]
    df = pd.DataFrame(rows, columns=COLUMNS)
    df[["orders", "returns"]] = df[["orders", "returns"]].astype(int)
    df["return_rate"] = df["return_rate"].astype(float)
    return df


def highlight_flagged(row):
    return ["background-color: #f8d7da" if row["status"] == "FLAGGED" else ""] * len(row)


def show_table(df):
    st.dataframe(df.style.apply(highlight_flagged, axis=1).format({"return_rate": "{:.1%}"}),
                 hide_index=True, use_container_width=True)


st.set_page_config(page_title="Return-Rate Dashboard", layout="wide")
st.title("Weekly Return Rate by Product Category")
st.caption(f"Kafka → HDFS → MapReduce. Categories above a {THRESHOLD:.0%} return rate are flagged.")
if st.button("Refresh"):
    st.cache_data.clear()

df = load_results()
if df is None or df.empty:
    st.info("No results yet. Stream the data with the producer, run the MapReduce job, then press Refresh.")
    st.stop()

week = st.selectbox("Week", sorted(df["week"].unique(), reverse=True))   # newest week first
week_df = df[df["week"] == week].sort_values("return_rate", ascending=False)

c1, c2, c3 = st.columns(3)
c1.metric("Categories", len(week_df))
c2.metric(f"Flagged (> {THRESHOLD:.0%})", int((week_df["status"] == "FLAGGED").sum()))
c3.metric("Overall return rate", f"{week_df['returns'].sum() / max(week_df['orders'].sum(), 1):.1%}")

threshold_line = alt.Chart(pd.DataFrame({"threshold": [THRESHOLD]})).mark_rule(
    color="red", strokeDash=[6, 4]).encode(y="threshold:Q")

bars = alt.Chart(week_df).mark_bar().encode(
    x=alt.X("category:N", sort="-y", title="Category"),
    y=alt.Y("return_rate:Q", title="Return rate", axis=alt.Axis(format="%")),
    color=alt.condition(alt.datum.status == "FLAGGED", alt.value("#d62728"), alt.value("#1f77b4")),
    tooltip=["category", "orders", "returns", alt.Tooltip("return_rate:Q", format=".1%"), "status"],
)
st.subheader(f"Return rate by category — {week}")
st.altair_chart(bars + threshold_line, use_container_width=True)
show_table(week_df)

st.subheader("Weekly trend")
trend = alt.Chart(df).mark_line(point=True).encode(
    x=alt.X("week:N", title="Week"),
    y=alt.Y("return_rate:Q", title="Return rate", axis=alt.Axis(format="%")),
    color=alt.Color("category:N", title="Category"),
    tooltip=["week", "category", "orders", "returns", alt.Tooltip("return_rate:Q", format=".1%")],
)
st.altair_chart(trend + threshold_line, use_container_width=True)

st.subheader("All flagged category-weeks")
show_table(df[df["status"] == "FLAGGED"].sort_values(["week", "return_rate"], ascending=[False, False]))
