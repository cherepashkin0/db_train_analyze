# db_real_ingestion.py

import asyncio
import os
import json
import clickhouse_connect
from api_client import fetch_and_save
from iris_parser import parse_db_xml
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- ФУНКЦИЯ ЗАГРУЗКИ КОНФИГА ---
def load_config():
    # Мы знаем, что в Docker Airflow папка dags всегда тут:
    base_dir = "/opt/airflow/dags"
    config_path = os.path.join(base_dir, "config", "railway_config.json")
    
    print(f"🔍 Ищу конфиг здесь: {config_path}")

    # === ОТЛАДКА (DEBUG) ===
    # Выводим список файлов, чтобы понять, что видит Docker
    try:
        config_dir = os.path.join(base_dir, "config")
        if os.path.exists(config_dir):
            print(f"📂 Содержимое папки {config_dir}: {os.listdir(config_dir)}")
        else:
            print(f"❌ Папка {config_dir} не существует!")
            print(f"📂 Содержимое корня {base_dir}: {os.listdir(base_dir)}")
    except Exception as e:
        print(f"⚠ Ошибка при отладке путей: {e}")
    # =======================

    if os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                print("✅ Конфиг успешно открыт.")
                return json.load(f)
        except Exception as e:
            print(f"❌ Файл есть, но ошибка чтения JSON: {e}")
    else:
        print("❌ Файл конфига физически отсутствует по этому пути.")

    print("⚠ Использую дефолтные значения (Berlin Hbf).")
    return {
        "stations": {"8011160": "Berlin Hbf"}, 
        "monitored_types": []
    }

# --- ЛОГИРОВАНИЕ В POSTGRES ---
def log_ingestion_status(context, status, records_count, error_message=None):
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        create_sql = """
        CREATE TABLE IF NOT EXISTS api_ingestion_log (
            run_id SERIAL PRIMARY KEY,
            dag_id VARCHAR(50),
            execution_date VARCHAR(50),
            status VARCHAR(20),
            records_count INT,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        pg_hook.run(create_sql)

        insert_sql = """
            INSERT INTO api_ingestion_log (dag_id, execution_date, status, records_count, error_message)
            VALUES (%s, %s, %s, %s, %s);
        """
        
        dag_id = str(context['dag'].dag_id)
        execution_date = str(context['execution_date'])
        
        pg_hook.run(insert_sql, parameters=(dag_id, execution_date, status, records_count, error_message))
        print(f"📝 Статус '{status}' записан в Postgres.")
    except Exception as e:
        print(f"❌ Ошибка записи лога в Postgres: {e}")

# --- ОСНОВНАЯ ЛОГИКА ---
async def run_real_ingestion(context):
    config = load_config()
    stations = config.get("stations", {})
    # Убираем пустые типы и приводим к set
    target_types = set(filter(None, config.get("monitored_types", [])))
    
    # Если конфиг не загрузился или пустой, stations будет дефолтным
    queries = [
        {"url": f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/{eva}"}
        for eva in stations.keys()
    ]
    
    print(f"🌍 Загружаю данные для {len(stations)} станций...")

    output_path = "/opt/airflow/data/raw_api_data"
    
    df = await fetch_and_save(
        queries=queries,
        output_path=output_path,
        max_concurrent=3,
        rate_limit=60
    )

    # === ПРОВЕРКА НА ОШИБКИ ===
    failed_requests = df['error'].notna().sum()
    total_requests = len(queries)
    
    print(f"📊 Статистика API: {total_requests - failed_requests}/{total_requests} успешных.")

    if failed_requests == total_requests and total_requests > 0:
        error_msg = f"CRITICAL: All {total_requests} API requests failed."
        log_ingestion_status(context, 'FAILED', 0, error_msg)
        raise Exception(error_msg)

    # === ЗАГРУЗКА В CLICKHOUSE ===
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD')
    )

    all_parsed_data = []
    
    for _, row in df.iterrows():
        if row['error']: continue

        eva_id = row['url'].split('/')[-1]
        city = stations.get(eva_id, "Unknown")
        
        if row['response_data']:
            parsed_rows = parse_db_xml(row['response_data'], city)
            
            if target_types:
                parsed_rows = [r for r in parsed_rows if r[2] in target_types]
                
            all_parsed_data.extend(parsed_rows)

    count = len(all_parsed_data)

    if all_parsed_data:
        client.insert('train_delays', all_parsed_data, 
                        column_names=[
                            'timestamp', 'city', 'train_type', 'train_id', 
                            'planned_departure', 'actual_departure', 
                            'delay_in_min', 'is_cancelled',
                            'origin', 'destination'
                        ])
        print(f"✅ Успешно загружено {count} строк в ClickHouse.")
    else:
        print("⚠ API ответил успешно, но данных нет (или отфильтрованы).")

    log_ingestion_status(context, 'SUCCESS', count)

def main(**kwargs):
    asyncio.run(run_real_ingestion(kwargs))

