# train_generator.py

import clickhouse_connect
import random
from datetime import datetime
import os 

CH_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASS = os.getenv('CLICKHOUSE_PASSWORD') # Тот самый пароль из .env

def run_simulation():
    # ВНУТРИ сети Docker используем имя сервиса 'clickhouse'
    client = clickhouse_connect.get_client(
        host=CH_HOST, 
        port=8123, 
        username=CH_USER, 
        password=CH_PASS
    )

    cities = ["Berlin", "Köln", "München"]
    train_types = ["ICE", "RE", "S-Bahn"]
    
    data = []
    now = datetime.now()
    
    for city in cities:
        for t_type in train_types:
            delay = random.randint(0, 15)
            data.append([now, city, t_type, delay])
    
    client.insert('train_delays', data, column_names=['timestamp', 'city', 'train_type', 'delay_in_min'])
    print(f"✅ Успешно вставлено {len(data)} строк в ClickHouse")

if __name__ == "__main__":
    run_simulation()