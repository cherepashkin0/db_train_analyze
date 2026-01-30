import streamlit as st
import clickhouse_connect
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import math 
import numpy as np

# Timezone for Berlin
BERLIN_TZ = ZoneInfo("Europe/Berlin")

def now_berlin():
    """Get current time in Berlin timezone."""
    return datetime.now(BERLIN_TZ)

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

# === Вкладки ===
tab_station, tab_global, tab_debug = st.tabs(["🏙 Single Station", "🌍 Global Network", "🔧 Debug"])

available_cities, available_types = get_filter_data()

# --- SIDEBAR ---
st.sidebar.header("Filters")
city = st.sidebar.selectbox("Select City", available_cities if available_cities else ["Berlin Hbf"])
selected_types = st.sidebar.multiselect("Train Types", available_types, default=available_types)

if not selected_types:
    st.stop()

types_tuple = tuple(selected_types) if len(selected_types) > 1 else f"('{selected_types[0]}')"

# === DISCLAIMER ===
st.sidebar.divider()
st.sidebar.caption("ℹ️ **Project Disclaimer**")
st.sidebar.warning(
    """
    **⚠️ Educational Project**
    
    This dashboard is a personal Data Engineering portfolio project and is **not** affiliated with Deutsche Bahn. 
    Do not rely on this for travel planning.
    """
)

# ==============================================================================
# TAB 1: SINGLE STATION ANALYSIS
# ==============================================================================
with tab_station:
    st.subheader(f"📍 Station Analysis: {city}")
    
    # --- KPI BLOCK (Gold Layer) ---
    st.caption("Daily Stats — Only departed trains (past)")

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
            total, delayed, avg_del, max_del = result[0]
            
            total = total or 0
            delayed = delayed or 0
            max_del = max_del or 0
            
            if avg_del is None or (isinstance(avg_del, float) and math.isnan(avg_del)):
                avg_del_display = "N/A"
            else:
                avg_del_display = f"{avg_del:.1f} min"

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Trains", total)
            
            delay_rate = (delayed / total * 100) if total > 0 else 0
            k2.metric("Delayed (>5min)", delayed, delta=f"{delay_rate:.1f}%", delta_color="inverse")
            
            k3.metric("Avg Delay", avg_del_display)
            k4.metric("Max Delay", f"{max_del} min" if max_del > 0 else "N/A")
            
        else:
            st.warning(f"No stats for {city} today yet.")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Trains", 0)
            k2.metric("Delayed", 0)
            k3.metric("Avg Delay", "N/A")
            k4.metric("Max Delay", "N/A")

    except Exception as e:
        st.error(f"Error: {e}")

    st.divider()

    # --- CHARTS (Silver Layer) ---
    st.subheader(f"📊 Departed Trains Today")

    # ВАЖНО: planned_departure <= now() — только состоявшиеся поезда
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
      AND planned_departure <= now()  -- Только состоявшиеся!
      AND train_type IN {types_tuple}
    ORDER BY planned_departure ASC
    """

    df_raw = client.query_df(raw_query)

    if not df_raw.empty:
        # --- 1. ИСПРАВЛЕНИЕ ЧАСОВЫХ ПОЯСОВ (В САМОМ НАЧАЛЕ) ---
        # Конвертируем ОБЕ колонки в Berlin TimeZone
        for col in ['planned_departure', 'actual_departure']:
            if df_raw[col].dt.tz is None:
                # Если таймзона не указана -> считаем UTC -> переводим в Берлин
                df_raw[col] = df_raw[col].dt.tz_localize('UTC').dt.tz_convert(BERLIN_TZ)
            else:
                # Если таймзона есть -> просто конвертируем в Берлин
                df_raw[col] = df_raw[col].dt.tz_convert(BERLIN_TZ)
        # -----------------------------------------------------

        # Теперь вычисляем статус и рисуем (когда данные уже правильные)
        def get_status(row):
            if row['is_cancelled'] == 1: return 'Cancelled'
            elif row['delay_in_min'] > 5: return 'Delayed'
            elif row['delay_in_min'] < -1: return 'Early'
            else: return 'On Time'
        
        df_raw['status'] = df_raw.apply(get_status, axis=1)
        
        # --- Chart: Planned vs Actual ---
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
        
        # --- ЛИНИЯ PERFECT ---
        # Вычисляем min/max ТОЛЬКО СЕЙЧАС, когда df_raw уже сконвертирован
        min_time = df_raw['planned_departure'].min()
        max_time = df_raw['planned_departure'].max()
        
        # Добавляем небольшой запас (буфер) +- 30 минут, чтобы линия была красивой
        buffer = timedelta(minutes=30)
        start_line = min_time - buffer
        end_line = max_time + buffer

        fig.add_trace(go.Scatter(
            x=[start_line, end_line], 
            y=[start_line, end_line], 
            mode='lines', 
            name='Perfect', 
            line=dict(color='gray', dash='dash')
        ))
        
        # --- ЛИНИЯ NOW ---
        # Берем текущее время тоже в Берлинской зоне
        now = now_berlin()
        
        fig.add_shape(type="line", x0=now, x1=now, y0=0, y1=1, yref="paper", line=dict(color="red", dash="dot"))
        fig.add_annotation(x=now, y=1.02, yref="paper", text=f"Now ({now.strftime('%H:%M')})", showarrow=False)
        
        fig.update_layout(
            title="Planned vs Actual Departure", 
            height=500, 
            xaxis_title="Planned", 
            yaxis_title="Actual",
            # Ограничиваем зум, чтобы буфер линии не растягивал график слишком сильно
            xaxis_range=[min_time - timedelta(minutes=10), max_time + timedelta(minutes=10)],
            yaxis_range=[min_time - timedelta(minutes=10), max_time + timedelta(minutes=10)]
        )
        
        st.plotly_chart(fig, use_container_width=True)
                
        # --- Timeline ---
        st.subheader("⏱ Delays Timeline")
        
        # Берем только поезда с отклонением от графика (опоздание или раннее прибытие)
        df_delayed = df_raw[df_raw['delay_in_min'] != 0]
        
        if not df_delayed.empty:
            fig2 = px.scatter(
                df_delayed, 
                x="planned_departure", 
                y="delay_in_min", 
                color="train_type",  # Разные цвета
                symbol="train_type", # <--- ДОБАВЛЕНО: Разные формы значков (круг, квадрат, ромб...)
                hover_data=["train_id", "origin", "destination"], 
                labels={"delay_in_min": "Delay (min)", "planned_departure": "Time"},
                title="Delay Distribution by Train Type"
            )
            
            # Увеличиваем размер точек, чтобы формы были различимы
            fig2.update_traces(marker_size=10, opacity=0.8)
            
            # Линия "Вовремя"
            fig2.add_hline(y=0, line_dash="dash", line_color="green", annotation_text="On time")
            
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No delays to show on timeline.")

        # --- Log ---
        with st.expander("📋 Detailed Train Log"):
            st.dataframe(df_raw.sort_values(by='planned_departure', ascending=False).head(100), use_container_width=True)

        st.divider()
        
        # --- Histogram ---
        st.subheader("📊 Delay Distribution")
        fig_hist = px.histogram(df_raw, x="delay_in_min", nbins=50, color="status", color_discrete_map=color_map)
        
        delayed_subset = df_raw[df_raw['delay_in_min'] >= 4]
        if not delayed_subset.empty:
            counts, _ = np.histogram(delayed_subset['delay_in_min'], bins=50)
            fig_hist.update_yaxes(range=[0, counts.max() * 1.2])
        
        fig_hist.add_vline(x=0, line_dash="dash", line_color="green")
        fig_hist.add_vline(x=5, line_dash="dot", line_color="orange")
        st.plotly_chart(fig_hist, use_container_width=True)
        
        # --- Boxplot ---
        st.subheader("📦 Delays by Train Type")
        fig_box = px.box(df_raw, x="train_type", y="delay_in_min", color="train_type", points="outliers")
        fig_box.add_hline(y=0, line_dash="dash", line_color="green")
        st.plotly_chart(fig_box, use_container_width=True)

    else:
        st.info("No departed trains yet today for selected filters.")

# ==============================================================================
# TAB 2: GLOBAL NETWORK OVERVIEW
# ==============================================================================
with tab_global:
    st.header(f"🌍 Global Overview ({len(available_cities)} Stations)")
    
    # Global KPIs
    global_kpi_query = f"""
    SELECT 
        sum(total_trains),
        sum(delayed_trains),
        avgIf(avg_delay, avg_delay > 0),
        max(max_delay),
        count(DISTINCT city)
    FROM daily_train_stats FINAL
    WHERE stat_date = toDate(now())
      AND train_type IN {types_tuple}
    """
    
    try:
        g_res = client.query(global_kpi_query).result_rows
        if g_res and g_res[0][0] is not None:
            g_total, g_delayed, g_avg, g_max, g_cities = g_res[0]
            
            g_total = g_total or 0
            g_delayed = g_delayed or 0
            g_avg = g_avg if (g_avg and not math.isnan(g_avg)) else 0.0
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Trains", f"{g_total:,}", help=f"From {g_cities} stations")
            
            g_rate = (g_delayed / g_total * 100) if g_total > 0 else 0
            c2.metric("Delayed", f"{g_delayed:,}", delta=f"{g_rate:.1f}%", delta_color="inverse")
            c3.metric("Avg Delay", f"{g_avg:.1f} min")
            c4.metric("Max Delay", f"{g_max} min")
        else:
            st.warning("No global data today.")
            
    except Exception as e:
        st.error(f"Error: {e}")
        
    st.divider()
    
    col1, col2 = st.columns(2)
    
    # Worst stations
    with col1:
        st.subheader("📉 Worst Stations (by Delay Rate)")
        worst_query = f"""
        SELECT 
            city,
            sum(total_trains) as traffic,
            sum(delayed_trains) as delayed,
            round(delayed / traffic * 100, 1) as delay_rate
        FROM daily_train_stats FINAL
        WHERE stat_date = toDate(now()) AND train_type IN {types_tuple}
        GROUP BY city
        HAVING traffic > 10
        ORDER BY delay_rate DESC
        LIMIT 10
        """
        
        df_worst = client.query_df(worst_query)
        if not df_worst.empty:
            st.dataframe(df_worst, use_container_width=True, hide_index=True,
                column_config={"delay_rate": st.column_config.ProgressColumn("Delay %", format="%.1f%%", min_value=0, max_value=100)})

    # Busiest stations
    with col2:
        st.subheader("🚆 Busiest Stations (by Volume)")
        busiest_query = f"""
        SELECT city, sum(total_trains) as traffic, sum(delayed_trains) as delayed
        FROM daily_train_stats FINAL
        WHERE stat_date = toDate(now()) AND train_type IN {types_tuple}
        GROUP BY city
        ORDER BY traffic DESC
        LIMIT 10
        """
        
        df_busiest = client.query_df(busiest_query)
        if not df_busiest.empty:
            st.dataframe(df_busiest, use_container_width=True, hide_index=True)

    # Train types
    st.subheader("📋 Stats by Train Type")
    
    types_query = f"""
    SELECT 
        train_type,
        sum(total_trains) as total,
        sum(delayed_trains) as delayed,
        avgIf(avg_delay, avg_delay > 0) as avg_delay,
        max(max_delay) as max_delay
    FROM daily_train_stats FINAL
    WHERE stat_date = toDate(now()) AND train_type IN {types_tuple}
    GROUP BY train_type
    ORDER BY total DESC
    """
    
    df_types = client.query_df(types_query)
    if not df_types.empty:
        df_types['delay_rate'] = (df_types['delayed'] / df_types['total'] * 100).fillna(0)
        st.dataframe(df_types, use_container_width=True, hide_index=True,
            column_config={"delay_rate": st.column_config.ProgressColumn("Rate", format="%.1f%%", min_value=0, max_value=100)})

# ==============================================================================
# TAB 3: DEBUG
# ==============================================================================
with tab_debug:
    st.subheader("🔧 Debug Information")
    
    st.markdown("### Gold Layer by Date")
    debug_gold = """
    SELECT stat_date, count() as rows, sum(total_trains) as trains, sum(delayed_trains) as delayed
    FROM daily_train_stats FINAL
    GROUP BY stat_date ORDER BY stat_date DESC LIMIT 7
    """
    st.dataframe(client.query_df(debug_gold), use_container_width=True)
    
    st.markdown("### Silver Layer by Date")
    debug_silver = """
    SELECT 
        toDate(planned_departure) as date,
        count() as records,
        countIf(delay_in_min = 0) as zero_delay,
        countIf(delay_in_min > 5) as delayed,
        countIf(planned_departure <= now()) as past_trains,
        countIf(planned_departure > now()) as future_trains
    FROM train_delays FINAL
    GROUP BY date ORDER BY date DESC LIMIT 7
    """
    st.dataframe(client.query_df(debug_silver), use_container_width=True)
    
    st.markdown("### Train Types Today")
    debug_types = """
    SELECT train_type, count() as records, countIf(delay_in_min > 5) as delayed
    FROM train_delays FINAL
    WHERE toDate(planned_departure) = toDate(now()) AND planned_departure <= now()
    GROUP BY train_type ORDER BY records DESC
    """
    st.dataframe(client.query_df(debug_types), use_container_width=True)