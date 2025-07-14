import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime

# Set page config
st.set_page_config(page_title="ASEAN Food Prices Dashboard", layout="wide")

# DuckDB connection
con = duckdb.connect()

# Read Parquet data excluding _SUCCESS or CRC files
DATA_PATH = "../data/processed/asean_food.parquet/*/*.parquet"
query = f"SELECT * FROM read_parquet('{DATA_PATH}')"
df = con.execute(query).df()

# Ensure 'month' is datetime
df['month'] = pd.to_datetime(df['month'])

# Prepare dropdown options
all_countries = sorted(df['countryiso3'].unique())

all_commodities = sorted(df['commodity_group'].unique())

essential_commodities = [
    'Rice', 'Wheat', 'Maize', 'Sugar', 'Oil', 'Milk', 'Eggs'
]

# Sidebar controls
st.sidebar.title("Filters")

# Country selection
country_mode = st.sidebar.radio("Country Filter", ["Select Specific Countries", "All ASEAN Countries"])
if country_mode == "All ASEAN Countries":
    selected_countries = all_countries
else:
    selected_countries = st.sidebar.multiselect("Select Countries", all_countries, default=all_countries)

# Commodity selection
commodity_mode = st.sidebar.radio("Commodity Filter", ["All Commodities", "Essential Commodities", "Select Specific Commodities"])
if commodity_mode == "All Commodities":
    selected_commodities = all_commodities
elif commodity_mode == "Essential Commodities":
    selected_commodities = [c for c in all_commodities if any(e.lower() in c.lower() for e in essential_commodities)]
else:
    selected_commodities = st.sidebar.multiselect("Select Commodities", all_commodities, default=all_commodities)

# Date range filter
df_filtered_dates = df[df['month'].notna()]
min_date = df_filtered_dates['month'].min().to_pydatetime()
max_date = df_filtered_dates['month'].max().to_pydatetime()

date_range = st.sidebar.slider(
    "Select Date Range",
    min_value=min_date,
    max_value=max_date,
    value=(min_date, max_date),
    format="YYYY-MM"
)

# Order toggle
order_mode = st.sidebar.radio("Top 10 Chart Order", ["Descending", "Ascending"])

# Filter data
df_filtered = df[
    (df['countryiso3'].isin(selected_countries)) &
    (df['commodity_group'].isin(selected_commodities)) &
    (df['month'] >= date_range[0]) &
    (df['month'] <= date_range[1])
]

# Main Title
st.title("ASEAN Food Prices Dashboard (USD Standardized)")

# Overall trend
st.subheader("Overall Average Price Trend per Country")

fig_trend = px.line(
    df_filtered.groupby(['month', 'countryiso3']).agg({'avg_usd_price': 'mean'}).reset_index(),
    x='month',
    y='avg_usd_price',
    color='countryiso3',
    markers=True,
    labels={'avg_usd_price': 'Average Price (USD)', 'month': 'Date', 'countryiso3': 'Country'},
    title="Average Food Prices Over Time (USD)"
)

st.plotly_chart(fig_trend, use_container_width=True)

# Top 10 commodities chart
st.subheader("Top 10 Commodities by Average Price (USD)")

agg = df_filtered.groupby('commodity_group')['avg_usd_price'].mean().reset_index()
agg = agg.sort_values('avg_usd_price', ascending=(order_mode == "Ascending")).head(10)

fig_top10 = px.bar(
    agg,
    x='commodity_group',
    y='avg_usd_price',
    labels={'commodity_group': 'Commodity', 'avg_usd_price': 'Avg Price (USD)'},
    title=f"Top 10 Commodities by Average Price ({order_mode})",
)

st.plotly_chart(fig_top10, use_container_width=True)

# Optional raw data view
with st.expander("Show Raw Data"):
    st.dataframe(df_filtered)

