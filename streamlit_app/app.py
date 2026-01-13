import streamlit as st
import clickhouse_connect
import pandas as pd
import os  # Добавляем импорт

st.set_page_config(page_title="DB Punctuality Tracker", layout="wide")

# Получаем настройки из переменных окружения
CH_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASS = os.getenv('CLICKHOUSE_PASSWORD') # Тот самый пароль из .env

st.set_page_config(page_title="DB Punctuality Tracker", layout="wide")

# Функция подключения (кешируем, чтобы не переподключаться при каждом клике)
@st.cache_resource
def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,  # <-- Имя сервиса из docker-compose
        port=8123, 
        username=CH_USER, 
        password=CH_PASS # Пароль, который ты задал
    )

client = get_clickhouse_client()

st.title("🚆 DB Punctuality Index: Real-time DB Connection")
st.write("Данные загружаются напрямую из ClickHouse.")

# Боковая панель
st.sidebar.header("Фильтры")
city = st.sidebar.selectbox("Выберите город", ["Berlin", "Köln", "München"])

# 2. Запрос к ClickHouse
# Берем данные за последние 30 минут для выбранного города
query = f"""
SELECT 
    timestamp, 
    train_type, 
    delay_in_min 
FROM train_delays 
WHERE city = '{city}' 
ORDER BY timestamp DESC 
LIMIT 100
"""

try:
    df = client.query_df(query)

    if not df.empty:
        st.subheader(f"Последние задержки в: {city}")
        
        # Переделываем данные для графика (Pivot)
        # Индекс - время, колонки - типы поездов, значения - задержки
        chart_df = df.pivot_table(index='timestamp', columns='train_type', values='delay_in_min', aggfunc='mean')
        
        st.line_chart(chart_df)
        
        # Показываем сырые данные под графиком
        with st.expander("Посмотреть сырые данные из ClickHouse"):
            st.write(df)
    else:
        st.warning(f"В базе пока нет данных для города {city}. Запустите генератор!")

except Exception as e:
    st.error(f"Ошибка подключения к ClickHouse: {e}")

# Кнопка ручного обновления
if st.button('Обновить данные'):
    st.rerun()

st.info("Это приложение подключено к ClickHouse и отображает данные мгновенно.")