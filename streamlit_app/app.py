import streamlit as st
import clickhouse_connect
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import math 
import numpy as np

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
        cities = client.query("SELECT DISTINCT city FROM train_delays FINAL ORDER BY city").result_rows
        types = client.query("SELECT DISTINCT train_type FROM train_delays FINAL ORDER BY train_type").result_rows
        return [c[0] for c in cities], [t[0] for t in types]
    except:
        return [], []

# --- UI Layout ---
st.title("🚆 DB Punctuality Tracker")

# === ВАЖНО: Вкладки ===
tab_station, tab_global = st.tabs(["🏙 Single Station Analysis", "🌍 Global Network Overview"])

available_cities, available_types = get_filter_data()

# --- SIDEBAR (Общий для всех вкладок) ---
st.sidebar.header("Filters")

# Выбор города нужен только для первой вкладки, но держим в сайдбаре
city = st.sidebar.selectbox("Select City (for Station Tab)", available_cities if available_cities else ["Berlin Hbf"])

# Мультиселект типов работает на обе вкладки
selected_types = st.sidebar.multiselect("Train Types", available_types, default=available_types)

if not selected_types:
    st.stop()

types_tuple = tuple(selected_types) if len(selected_types) > 1 else f"('{selected_types[0]}')"

# === DISCLAIMER (Добавлен по просьбе) ===
st.sidebar.divider()
st.sidebar.caption("ℹ️ **Project Disclaimer**")
st.sidebar.warning(
    """
    **⚠️ Disclaimer: Educational Project**
    
    This dashboard is a personal Data Engineering portfolio project and is **not** affiliated with Deutsche Bahn. 

    The data presented here is for demonstration purposes only and may not reflect real-time operations. 
    Do not rely on this application for travel planning. The creator assumes no liability for decisions made based on this data.
    """
)

# ==============================================================================
# TAB 1: SINGLE STATION ANALYSIS (Твой существующий код)
# ==============================================================================
with tab_station:
    st.subheader(f"📍 Station Analysis: {city}")
    
    # --- 1. KPI BLOCK (Using GOLD Table) ---
    st.caption("Daily Stats (Aggregated from Gold Layer)")

    gold_query = f"""
    SELECT 
        sum(total_trains),
        sum(delayed_trains),
        avgIf(avg_delay, avg_delay > 0),
        max(max_delay)
    FROM daily_train_stats FINAL
    WHERE city = '{city}' 
      AND stat_date = toDate(now())
      AND train_type IN {types_tuple}
    """

    try:
        result = client.query(gold_query).result_rows
        
        if result and result[0][0] is not None:
            gold_data = result[0]
            total, delayed, avg_del, max_del = gold_data
            
            # Защита от None
            total = total or 0
            delayed = delayed or 0
            max_del = max_del or 0
            
            if avg_del is None:
                avg_del_display = "N/A"
            elif isinstance(avg_del, float) and math.isnan(avg_del):
                avg_del_display = "N/A"
            else:
                avg_del_display = f"{avg_del:.1f} min"

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Trains", total)
            
            delay_rate = (delayed / total * 100) if total > 0 else 0
            k2.metric("Delayed Trains", delayed, delta=f"{delay_rate:.1f}% rate", delta_color="inverse")
            
            k3.metric("Avg Delay (>0min)", avg_del_display)
            k4.metric("Max Delay", f"{max_del} min" if max_del > 0 else "N/A")
            
        else:
            st.warning(f"No aggregated stats found for {city} today yet.")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Trains", 0)
            k2.metric("Delayed Trains", 0)
            k3.metric("Avg Delay", "N/A")
            k4.metric("Max Delay", "N/A")

    except Exception as e:
        st.error(f"Error loading Gold layer: {e}")

    # --- Debug Raw Gold ---
    with st.expander("🔍 Debug: Raw Gold Layer Data"):
        debug_query = f"""
        SELECT * FROM daily_train_stats FINAL
        WHERE city = '{city}' AND stat_date = toDate(now())
        ORDER BY train_type
        """
        try:
            st.dataframe(client.query_df(debug_query), use_container_width=True)
        except: pass

    st.divider()

    # --- 2. DRILL DOWN CHARTS (Using RAW Table) ---
    st.subheader(f"📊 Planned vs Actual Departures")

    # Use calendar day
    raw_query = f"""
    SELECT
        planned_departure,
        actual_departure,
        train_type,
        delay_in_min,
        train_id,
        origin,
        destination,
        is_cancelled
    FROM train_delays FINAL
    WHERE city = '{city}'
      AND toDate(planned_departure) = toDate(now())
      AND train_type IN {types_tuple}
    ORDER BY planned_departure ASC
    LIMIT 1 BY train_id, planned_departure -- Дедупликация для конкретной станции
    """

    df_raw = client.query_df(raw_query)

    if not df_raw.empty:
        df_raw['planned_departure'] = pd.to_datetime(df_raw['planned_departure'])
        df_raw['actual_departure'] = pd.to_datetime(df_raw['actual_departure'])
        
        def get_status(row):
            if row['is_cancelled'] == 1: return 'Cancelled'
            elif row['delay_in_min'] > 5: return 'Delayed'
            elif row['delay_in_min'] < -1: return 'Early'
            else: return 'On Time'
        
        df_raw['status'] = df_raw.apply(get_status, axis=1)
        
        # --- Chart 1: Planned vs Actual ---
        fig = go.Figure()
        color_map = {'On Time': '#2ecc71', 'Delayed': '#e74c3c', 'Early': '#3498db', 'Cancelled': '#95a5a6'}
        
        for status in ['On Time', 'Early', 'Delayed', 'Cancelled']:
            df_status = df_raw[df_raw['status'] == status]
            if not df_status.empty:
                fig.add_trace(go.Scatter(
                    x=df_status['planned_departure'],
                    y=df_status['actual_departure'],
                    mode='markers',
                    name=status,
                    marker=dict(color=color_map[status], size=8, opacity=0.7),
                    customdata=df_status[['train_id', 'delay_in_min', 'origin', 'destination']].values,
                    hovertemplate='<b>%{customdata[0]}</b><br>Delay: %{customdata[1]} min<extra></extra>'
                ))
        
        # Helper lines
        min_time, max_time = df_raw['planned_departure'].min(), df_raw['planned_departure'].max()
        fig.add_trace(go.Scatter(x=[min_time, max_time], y=[min_time, max_time], mode='lines', name='Perfect', line=dict(color='gray', dash='dash')))
        
        now = datetime.now()
        fig.add_shape(type="line", x0=now, x1=now, y0=0, y1=1, yref="paper", line=dict(color="red", dash="dot"))
        fig.update_layout(title="Departure Schedule", height=500, xaxis_title="Planned", yaxis_title="Actual")
        
        st.plotly_chart(fig, use_container_width=True)
        
        # --- Chart 2: Timeline ---
        st.subheader("⏱ Delays Timeline")
        df_delayed = df_raw[df_raw['delay_in_min'] != 0]
        if not df_delayed.empty:
            fig2 = px.scatter(
                df_delayed, x="planned_departure", y="delay_in_min", color="train_type",
                title="Only Delayed/Early Trains", hover_data=["train_id"], labels={"delay_in_min": "Diff (min)"}
            )
            fig2.add_hline(y=0, line_dash="dash", line_color="green")
            st.plotly_chart(fig2, use_container_width=True)

        # --- Detailed Log ---
        with st.expander("📋 Detailed Train Log"):
            st.dataframe(df_raw.sort_values(by='planned_departure', ascending=False).head(100), use_container_width=True)

        st.divider()
        
        # --- 3. HISTOGRAM (Scaled) ---
        st.subheader("📊 Delay Distribution")
        fig_hist = px.histogram(
            df_raw, x="delay_in_min", nbins=50, color="status", color_discrete_map=color_map,
            title="Distribution of Delays"
        )
        
        # Auto-scaling logic using numpy
        delayed_subset = df_raw[df_raw['delay_in_min'] >= 4]
        if not delayed_subset.empty:
            counts, _ = np.histogram(delayed_subset['delay_in_min'], bins=50)
            fig_hist.update_yaxes(range=[0, counts.max() * 1.2]) # Scale to delayed peak
        
        fig_hist.add_vline(x=0, line_dash="dash", line_color="green")
        st.plotly_chart(fig_hist, use_container_width=True)
        
        # --- 4. BOXPLOT ---
        st.subheader("📦 Delays by Train Type")
        fig_box = px.box(df_raw, x="train_type", y="delay_in_min", color="train_type", points="outliers")
        fig_box.add_hline(y=0, line_dash="dash", line_color="green")
        st.plotly_chart(fig_box, use_container_width=True)

    else:
        st.info("No data available for the selected filters.")

# ==============================================================================
# TAB 2: GLOBAL NETWORK OVERVIEW (Новая вкладка)
# ==============================================================================
with tab_global:
    st.header(f"🌍 Global Overview ({len(available_cities)} Stations)")
    st.write("Aggregated statistics across all monitored stations for today.")
    
    # 1. GLOBAL KPIs
    global_kpi_query = f"""
    SELECT 
        sum(total_trains),
        sum(delayed_trains),
        avgIf(avg_delay, avg_delay > 0),
        max(max_delay),
        count(DISTINCT city) -- Сколько станций прислали данные
    FROM daily_train_stats FINAL
    WHERE stat_date = toDate(now())
      AND train_type IN {types_tuple}
    """
    
    try:
        g_res = client.query(global_kpi_query).result_rows
        if g_res and g_res[0][0] is not None:
            g_total, g_delayed, g_avg, g_max, g_cities_count = g_res[0]
            
            g_total = g_total or 0
            g_delayed = g_delayed or 0
            g_avg = g_avg if (g_avg and not math.isnan(g_avg)) else 0.0
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Traffic (All Stations)", g_total, help=f"Data received from {g_cities_count} stations")
            
            g_rate = (g_delayed / g_total * 100) if g_total > 0 else 0
            c2.metric("Global Delay Rate", f"{g_rate:.1f}%", delta="vs average" if g_rate > 20 else None, delta_color="inverse")
            c3.metric("Avg Network Delay", f"{g_avg:.1f} min")
            c4.metric("Network Max Delay", f"{g_max} min")
        else:
            st.warning("No global data for today.")
            
    except Exception as e:
        st.error(f"Global KPI Error: {e}")
        
    st.divider()
    
    col_lead_1, col_lead_2 = st.columns(2)
    
    # 2. LEADERBOARD: WORST STATIONS (Top 10 by Delay Rate)
    with col_lead_1:
        st.subheader("📉 Worst Performing Stations")
        worst_stations_query = f"""
        SELECT 
            city,
            sum(total_trains) as traffic,
            sum(delayed_trains) as delayed,
            round(delayed / traffic * 100, 1) as delay_rate
        FROM daily_train_stats FINAL
        WHERE stat_date = toDate(now())
          AND train_type IN {types_tuple}
        GROUP BY city
        HAVING traffic > 10 -- Исключаем станции с малым трафиком
        ORDER BY delay_rate DESC
        LIMIT 10
        """
        
        df_worst = client.query_df(worst_stations_query)
        if not df_worst.empty:
            st.dataframe(
                df_worst, 
                use_container_width=True,
                column_config={
                    "city": "Station",
                    "traffic": "Trains",
                    "delay_rate": st.column_config.ProgressColumn("Delay %", format="%.1f%%", min_value=0, max_value=100)
                },
                hide_index=True
            )
        else:
            st.info("Not enough data to calculate worst stations.")

    # 3. LEADERBOARD: TRAFFIC VOLUME
    with col_lead_2:
        st.subheader("🚆 Busiest Stations (by Volume)")
        busiest_query = f"""
        SELECT 
            city,
            sum(total_trains) as traffic,
            sum(delayed_trains) as delayed
        FROM daily_train_stats FINAL
        WHERE stat_date = toDate(now())
          AND train_type IN {types_tuple}
        GROUP BY city
        ORDER BY traffic DESC
        LIMIT 10
        """
        
        df_busiest = client.query_df(busiest_query)
        if not df_busiest.empty:
            st.dataframe(
                df_busiest, 
                use_container_width=True,
                column_config={
                    "city": "Station",
                    "traffic": st.column_config.NumberColumn("Total Trains"),
                    "delayed": "Delayed"
                },
                hide_index=True
            )
        else:
            st.info("No data.")

    # 4. GLOBAL TRAIN TYPE STATS
    st.subheader("📋 Network-wide Stats by Train Type")
    
    global_types_query = f"""
    SELECT 
        train_type,
        sum(total_trains) as total,
        sum(delayed_trains) as delayed,
        avgIf(avg_delay, avg_delay > 0) as avg_delay,
        max(max_delay) as max_delay
    FROM daily_train_stats FINAL
    WHERE stat_date = toDate(now())
      AND train_type IN {types_tuple}
    GROUP BY train_type
    ORDER BY total DESC
    """
    
    df_g_types = client.query_df(global_types_query)
    
    if not df_g_types.empty:
        df_g_types['delay_rate'] = (df_g_types['delayed'] / df_g_types['total'] * 100).fillna(0)
        
        st.dataframe(
            df_g_types,
            use_container_width=True,
            column_order=["train_type", "total", "delayed", "delay_rate", "avg_delay", "max_delay"],
            column_config={
                "train_type": "Type",
                "total": "Total",
                "delay_rate": st.column_config.ProgressColumn("Rate", format="%.1f%%", min_value=0, max_value=100),
                "avg_delay": st.column_config.NumberColumn("Avg Delay", format="%.1f"),
            },
            hide_index=True
        )