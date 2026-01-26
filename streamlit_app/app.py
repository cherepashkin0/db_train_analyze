import streamlit as st
import clickhouse_connect
import pandas as pd
import os
import plotly.express as px
from datetime import datetime, timedelta

# 1. Настройка страницы
st.set_page_config(page_title="DB Punctuality Tracker", layout="wide")

# 2. Настройки подключения
CH_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASS = os.getenv('CLICKHOUSE_PASSWORD')

@st.cache_resource
def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=8123, username=CH_USER, password=CH_PASS
    )

client = get_clickhouse_client()

# --- Вспомогательные функции ---
def get_available_cities():
    try:
        df = client.query_df("SELECT DISTINCT city FROM train_delays ORDER BY city")
        if not df.empty:
            return df['city'].tolist()
    except Exception as e:
        print(f"Error: {e}")
    return ["Berlin Hbf", "Köln Hbf", "München Hbf"]

def get_available_train_types(city_name):
    try:
        query = f"SELECT DISTINCT train_type FROM train_delays WHERE city = '{city_name}' ORDER BY train_type"
        return client.query_df(query)['train_type'].tolist()
    except:
        return []

# --- UI ---
st.title("🚆 DB Punctuality Index")
st.write("Данные загружаются напрямую из ClickHouse. Показаны только поезда с задержкой > 0 мин.")

# --- Сайдбар ---
st.sidebar.header("Фильтры")
available_cities = get_available_cities()
city = st.sidebar.selectbox("Выберите город", available_cities)

# Получаем все типы
train_types_list = get_available_train_types(city)

# Выбираем ВСЕ типы по умолчанию
selected_types = st.sidebar.multiselect(
    "Типы поездов", 
    train_types_list, 
    default=train_types_list
)

if not selected_types:
    st.warning("Выберите хотя бы один тип поезда.")
    st.stop()

# 4. Основной запрос данных
# Исправлен синтаксис для ClickHouse: subtractHours() или INTERVAL
query_analytics = f"""
SELECT
    actual_departure,
    train_type,
    delay_in_min,
    train_id,
    origin,
    destination
FROM train_delays
WHERE city = '{city}' 
  AND actual_departure >= now() - INTERVAL 24 HOUR
  AND train_type IN {tuple(selected_types) if len(selected_types) > 1 else f"('{selected_types[0]}')"}
ORDER BY actual_departure ASC
"""

# === ЗАГРУЗКА И ОТОБРАЖЕНИЕ ===
try:
    df_raw = client.query_df(query_analytics)

    if not df_raw.empty:
        # Удаляем дубликаты
        df_raw = df_raw.drop_duplicates(subset=['train_id', 'actual_departure'], keep='first')
        
        # --- ФИЛЬТРАЦИЯ ---
        df_analytics = df_raw[df_raw['delay_in_min'] > 0].copy()

        if df_analytics.empty:
            st.success(f"В городе {city} за последние 24 часа не найдено задержек (среди выбранных типов).")
            st.stop()
        
        # --- 1. БЛОК KPI (Метрики) ---
        st.subheader("📈 Статистика по опозданиям (24ч)")
        
        total_delayed_trains = len(df_analytics)
        avg_delay = df_analytics['delay_in_min'].mean()
        median_delay = df_analytics['delay_in_min'].median()
        max_delay = df_analytics['delay_in_min'].max()
        
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("Опоздавших поездов", total_delayed_trains)
        kpi2.metric("Среднее опоздание", f"{avg_delay:.1f} мин")
        kpi3.metric("Медианное опоздание", f"{median_delay:.1f} мин")
        kpi4.metric("Максимальное опоздание", f"{max_delay:.0f} мин")
        
        st.divider() 

        # --- 2. График разброса (Точки) ---
        st.subheader(f"📊 Хронология задержек в {city}")
        
        # Конвертируем datetime в pandas datetime для корректной работы с Plotly
        df_analytics['actual_departure'] = pd.to_datetime(df_analytics['actual_departure'])
        
        fig_scatter = px.scatter(
            df_analytics, 
            x="actual_departure", 
            y="delay_in_min", 
            color="train_type",
            title="Каждая точка — один опоздавший поезд",
            labels={"actual_departure": "Время отправления", "delay_in_min": "Задержка (мин)"},
            hover_data=["train_id", "origin", "destination"]
        )
        
        # Линии времени - конвертируем в timestamp
        now = pd.Timestamp.now()
        midnight = now.normalize()  # Полночь текущего дня
        
        fig_scatter.add_vline(x=now.value, line_color="red", line_dash="solid", annotation_text="Сейчас")
        
        if df_analytics['actual_departure'].min() < midnight:
            fig_scatter.add_vline(x=midnight.value, line_color="gray", line_dash="dash", annotation_text="00:00")
        
        st.plotly_chart(fig_scatter, use_container_width=True)

        # --- 3. Статистические графики ---
        st.subheader("📉 Анализ распределения")
        col_hist, col_box = st.columns(2)

        with col_hist:
            fig_hist = px.histogram(
                df_analytics, 
                x="delay_in_min", 
                nbins=30,
                title="Гистограмма задержек",
                labels={"delay_in_min": "Минут задержки"},
                color_discrete_sequence=['#EF553B']
            )
            fig_hist.update_layout(yaxis_title="Количество поездов")
            st.plotly_chart(fig_hist, use_container_width=True)

        with col_box:
            fig_box = px.box(
                df_analytics, 
                x="train_type", 
                y="delay_in_min", 
                color="train_type",
                title="Boxplot задержек по типам",
                labels={"train_type": "Тип", "delay_in_min": "Задержка (мин)"}
            )
            st.plotly_chart(fig_box, use_container_width=True)

        # --- 4. Детальная таблица ---
        with st.expander("🔎 Детальные данные (последние 50 записей)"):
            detailed_query = f"""
                SELECT 
                    train_id, 
                    origin,
                    destination,
                    planned_departure, 
                    actual_departure, 
                    delay_in_min,
                    is_cancelled
                FROM train_delays
                WHERE city = '{city}'
                  AND delay_in_min > 0
                  AND train_type IN {tuple(selected_types) if len(selected_types) > 1 else f"('{selected_types[0]}')"}
                ORDER BY actual_departure DESC
                LIMIT 50
            """
            st.dataframe(client.query_df(detailed_query))
            
    else:
        st.info(f"Данных по городу {city} за последние 24 часа нет.")

except Exception as e:
    st.error(f"Ошибка в приложении: {e}")
    import traceback
    st.code(traceback.format_exc())