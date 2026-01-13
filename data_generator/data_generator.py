import clickhouse_connect
import pandas as pd
import numpy as np
from datetime import datetime
import time
import random
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')

# Подключение к ClickHouse (внутри Docker сети хост обычно 'clickhouse')
# Если запускаешь вне Docker, используй '127.0.0.1'
client = clickhouse_connect.get_client(host='127.0.0.1', port=8123, username='default', password=CLICKHOUSE_PASSWORD)

# 1. Создаем таблицу с движком ReplacingMergeTree (как советовала менторка)
client.command("""
CREATE TABLE IF NOT EXISTS train_delays (
    timestamp DateTime,
    city String,
    train_type String,
    delay_in_min Int32
) ENGINE = ReplacingMergeTree()
ORDER BY (timestamp, city, train_type)
""")

cities = ["Berlin", "Köln", "München"]
train_types = ["ICE", "RE", "S-Bahn"]

print("🚀 Генератор запущен. Начинаю вставку данных в ClickHouse...")

try:
    while True:
        # Генерируем пачку данных (например, для каждого города и типа поезда)
        data = []
        now = datetime.now()
        
        for city in cities:
            for t_type in train_types:
                delay = random.randint(0, 15) # Случайная задержка
                data.append([now, city, t_type, delay])
        
        # Вставка в ClickHouse
        client.insert('train_delays', data, column_names=['timestamp', 'city', 'train_type', 'delay_in_min'])
        
        print(f"✅ Вставлено {len(data)} записей в {now.strftime('%H:%M:%S')}")
        time.sleep(10) # Пауза 10 секунд

except KeyboardInterrupt:
    print("🛑 Генератор остановлен.")