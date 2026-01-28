import streamlit as st
import clickhouse_connect
import pandas as pd
import os
import plotly.express as px
from datetime import datetime, timedelta
import math 

# 1. Config
st.set_page_config(page_title="DB Punctuality Tracker", layout="wide")

CH_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASS = os.getenv('CLICKHOUSE_PASSWORD')

@st.cache_resource
def get_clickhouse_client():
    return clickhouse_connect.get_client(host=CH_HOST, port=8123, username=CH_USER, password=CH_PASS)

client = get_clickhouse_client()

# --- Helper Functions ---
def get_filter_data():
    """Получаем списки городов и типов сразу одним легким запросом."""
    try:
        cities = client.query("SELECT DISTINCT city FROM train_delays ORDER BY city").result_rows
        types = client.query("SELECT DISTINCT train_type FROM train_delays ORDER BY train_type").result_rows
        return [c[0] for c in cities], [t[0] for t in types]
    except:
        return [], []

# --- UI Layout ---
st.title("🚆 DB Punctuality Index (Gold Layer Integrated)")
st.write("KPIs are served from aggregated Gold Layer. Charts are drilled down from Raw Layer.")

available_cities, available_types = get_filter_data()

# Sidebar
st.sidebar.header("Filters")
city = st.sidebar.selectbox("Select City", available_cities if available_cities else ["Berlin Hbf"])
selected_types = st.sidebar.multiselect("Train Types", available_types, default=available_types)

if not selected_types:
    st.stop()

# --- 1. KPI BLOCK (Using GOLD Table) ---
st.subheader("📈 Daily Stats (From Gold Layer)")

gold_query = f"""
SELECT 
    sum(total_trains),
    sum(delayed_trains),
    avg(avg_delay),  -- Убрали round из SQL, сделаем в Python
    max(max_delay)
FROM daily_train_stats
WHERE city = '{city}' 
  AND stat_date = toDate(now()) -- Данные за сегодня
  AND train_type IN {tuple(selected_types)}
"""

try:
    result = client.query(gold_query).result_rows
    
    if result and result[0][0] is not None:
        gold_data = result[0]
        total, delayed, avg_del, max_del = gold_data
        
        # --- БЛОК ОЧИСТКИ ДАННЫХ ---
        # 1. Защита от None (если база вернула Null)
        total = total or 0
        delayed = delayed or 0
        max_del = max_del or 0
        
        # 2. Защита от NaN (Not a Number) для среднего значения
        if avg_del is None or math.isnan(avg_del):
            avg_del = 0.0
        # ---------------------------

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Trains", total)
        
        # Считаем процент опозданий безопасно
        delay_rate = (delayed / total * 100) if total > 0 else 0
        k2.metric("Delayed Trains", delayed, delta=f"{delay_rate:.1f}% rate", delta_color="inverse")
        
        k3.metric("Avg Delay", f"{avg_del:.1f} min")
        k4.metric("Max Delay", f"{max_del} min")
        
    else:
        # Если данных за сегодня вообще нет в Gold слое
        st.warning(f"No aggregated stats found for {city} today yet. Check if Airflow DAG ran successfully.")
        
        # Показываем нули, чтобы верстка не ехала
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Trains", 0)
        k2.metric("Delayed Trains", 0)
        k3.metric("Avg Delay", "0.0 min")
        k4.metric("Max Delay", "0 min")

except Exception as e:
    st.error(f"Error loading Gold layer: {e}")

st.divider()

# --- 2. DRILL DOWN CHARTS (Using RAW Table) ---
# Графики строим по сырым данным, так как нам нужна каждая точка
st.subheader(f"📊 Live Timeline (From Raw Layer)")

raw_query = f"""
SELECT
    toString(actual_departure),
    train_type,
    delay_in_min,
    train_id,
    origin,
    destination
FROM train_delays
WHERE city = '{city}'
  AND actual_departure >= now() - INTERVAL 24 HOUR
  AND train_type IN {tuple(selected_types)}
ORDER BY actual_departure ASC
LIMIT 1 BY train_id, planned_departure 
"""

df_raw = client.query_df(raw_query)

if not df_raw.empty:
    # Конвертация даты в Python
    df_raw['actual_departure'] = pd.to_datetime(df_raw['toString(actual_departure)'])
    
    # Фильтруем только опоздавшие для графика
    df_chart = df_raw[df_raw['delay_in_min'] > 0]

    fig = px.scatter(
        df_chart,
        x="actual_departure",
        y="delay_in_min",
        color="train_type",
        title="Delays Timeline",
        hover_data=["train_id", "origin", "destination"]
    )
    
    # Добавляем линию "Сейчас"
    fig.add_vline(x=pd.Timestamp.now().value, line_color="red", annotation_text="Now")

    st.plotly_chart(fig, use_container_width=True)
    
    with st.expander("Detailed Logs"):
        st.dataframe(df_chart.sort_values(by='actual_departure', ascending=False).head(50))
else:
    st.info("No raw data available for charts.")