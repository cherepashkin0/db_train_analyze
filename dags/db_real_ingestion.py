"""
Deutsche Bahn Train Delays Pipeline
====================================
Medallion Architecture: Bronze -> Silver -> Gold
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import asyncio
import os
import json
import clickhouse_connect
# Предполагаем, что parser теперь возвращает unique_id последним элементом
from api_client import generate_plan_queries, generate_fchg_queries, fetch_and_save
from iris_parser import parse_plan_xml, parse_fchg_xml 

# =============================================================================
# CONFIGURATION
# =============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DAG_ID = 'db_train_medallion_pipeline'
BRONZE_PATH = "/opt/airflow/data/bronze/train_api"
CONFIG_PATH = "/opt/airflow/dags/config/railway_config.json"


def load_config():
    """Load station configuration."""
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"stations": {"8011160": "Berlin Hbf"}, "monitored_types": [], "hours_back": 12, "hours_forward": 12}


def get_ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD')
    )


def log_pipeline_stage(dag_id: str, stage: str, status: str, records_processed: int = 0, error_message: str = None):
    """Log pipeline stage to Postgres metadata table."""
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        pg_hook.run("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id SERIAL PRIMARY KEY,
                dag_id VARCHAR(100),
                stage VARCHAR(50),
                status VARCHAR(20),
                records_processed INTEGER,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        pg_hook.run(
            """INSERT INTO pipeline_runs (dag_id, stage, status, records_processed, error_message) 
               VALUES (%s, %s, %s, %s, %s)""",
            parameters=(dag_id, stage, status, records_processed, error_message)
        )
        print(f"📝 Logged: {stage} -> {status} ({records_processed} records)")
    except Exception as e:
        print(f"⚠ Failed to log to Postgres: {e}")


# =============================================================================
# BRONZE LAYER: Raw Data Extraction
# =============================================================================

def bronze_extract(**context):
    print("=" * 60)
    print("🥉 BRONZE LAYER: Starting raw data extraction")
    print("=" * 60)
    
    config = load_config()
    stations = config.get("stations", {})
    # Качаем данные за полные сутки (-12...+12), чтобы видеть полную картину
    hours_back = config.get("hours_back", 12)
    hours_forward = config.get("hours_forward", 12)
    
    plan_queries = generate_plan_queries(stations, hours_back, hours_forward)
    fchg_queries = generate_fchg_queries(stations)
    all_queries = plan_queries + fchg_queries
    
    print(f"📡 API Queries: {len(all_queries)} total")
    
    async def run_fetch():
        return await fetch_and_save(
            queries=all_queries,
            output_path=BRONZE_PATH,
            max_concurrent=10,
            rate_limit=60,
        )
    
    df = asyncio.run(run_fetch())
    
    total = len(df)
    failed = df['error'].notna().sum()
    success = total - failed
    
    if total == 0:
        raise Exception("CRITICAL: No queries generated")
    
    if failed == total:
        raise Exception("CRITICAL: All API requests failed")
    
    log_pipeline_stage(DAG_ID, 'bronze', 'SUCCESS', success)
    
    # Передаем путь к данным следующему таску
    context['ti'].xcom_push(key='bronze_path', value=BRONZE_PATH)
    return total


# =============================================================================
# SILVER LAYER: Parsing, Cleaning, Validation
# =============================================================================

def silver_transform(**context):
    print("=" * 60)
    print("🥈 SILVER LAYER: Starting transformation")
    print("=" * 60)
    
    import pandas as pd
    from pathlib import Path
    
    config = load_config()
    target_types = set(config.get("monitored_types", []))
    
    # Обновляем схему таблицы (добавляем unique_id)
    ensure_silver_tables()
    
    bronze_path = context['ti'].xcom_pull(key='bronze_path', task_ids='bronze_extract')
    bronze_path = Path(bronze_path or BRONZE_PATH)
    
    parquet_files = list(bronze_path.glob("**/*.parquet"))
    if not parquet_files:
        raise Exception("No bronze data found")
    
    # Читаем все файлы и объединяем
    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)
    
    # Фильтруем старые файлы (оставляем последние 2 дня)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    cutoff = datetime.now() - timedelta(days=2)
    df = df[df['timestamp'] >= cutoff]
    
    print(f"📊 Bronze records loaded: {len(df)}")
    
    # --- PARSING & DEDUPLICATION ---
    print("\n🔄 Parsing XML responses...")
    
    # Ключ словаря теперь: (unique_train_id, planned_departure, city)
    # Это позволяет различать поезда с одинаковым ID, проходящие станцию в разное время
    trains_dict = {} 
    
    parse_errors = 0
    
    for _, row in df.iterrows():
        if row['error'] or not row['response_data']: continue
        
        station_name = row.get('station_name', 'Unknown')
        query_type = row.get('query_type', 'plan')
        
        try:
            # Парсим XML
            if query_type == 'fchg':
                rows = parse_fchg_xml(row['response_data'], station_name)
            else:
                rows = parse_plan_xml(row['response_data'], station_name)
            
            # Фильтр по типам
            if target_types:
                rows = [r for r in rows if r[2] in target_types]
            
            # --- ЛОГИКА ДЕДУПЛИКАЦИИ ---
            for row_data in rows:
                # ВАЖНО: Мы ожидаем, что parser теперь возвращает unique_id в конце кортежа!
                # Структура row_data: 
                # [0:timestamp, 1:city, 2:type, 3:human_id, 4:planned, 5:actual, 6:delay, 7:canc, 8:orig, 9:dest, 10:UNIQUE_ID]
                
                # Если парсер еще не обновлен и не возвращает unique_id, используем human_id как fallback (временная мера)
                if len(row_data) > 10:
                    unique_id = row_data[10]
                else:
                    unique_id = row_data[3] # Fallback на "ICE 101"
                
                planned_dep = row_data[4]
                city = row_data[1]
                
                # Собираем ключ. Если поезд проходит станцию 2 раза (в 12:00 и 16:00),
                # planned_dep будет разным, и записи не перезапишут друг друга.
                key = (unique_id, planned_dep, city)
                
                # Приоритет данных: FCHG (изменения) важнее PLAN (расписания)
                if query_type == 'fchg':
                    trains_dict[key] = row_data
                elif key not in trains_dict:
                    trains_dict[key] = row_data
                    
        except Exception as e:
            parse_errors += 1
            if parse_errors <= 1: print(f"⚠ Parse error: {e}")

    print(f"   - Unique trains after merge: {len(trains_dict)}")
    
    if not trains_dict:
        raise Exception("No records after parsing")
    
    all_records = list(trains_dict.values())
    
    # --- DATA QUALITY & LOAD ---
    dq_results = run_silver_dq_checks(all_records)
    
    if dq_results['critical_failures']:
        raise Exception(f"Data Quality Failed: {dq_results['critical_failures']}")
    
    clean_records = dq_results['clean_records']
    
    print("\n💾 Loading to ClickHouse Silver layer...")
    client = get_ch_client()
    
    # Вставляем данные. ВАЖНО: добавить unique_train_id в список колонок
    client.insert('train_delays', clean_records,
                  column_names=['timestamp', 'city', 'train_type', 'train_id',
                                'planned_departure', 'actual_departure',
                                'delay_in_min', 'is_cancelled', 'origin', 'destination', 
                                'unique_train_id']) # <--- НОВОЕ ПОЛЕ
    
    # Форсируем слияние в ClickHouse для удаления дублей на уровне диска
    client.command("OPTIMIZE TABLE train_delays FINAL")
    
    log_pipeline_stage(DAG_ID, 'silver', 'SUCCESS', len(clean_records))
    return len(clean_records)


def ensure_silver_tables():
    """Create ClickHouse tables with UNIQUE ID support."""
    client = get_ch_client()
    
    # Если схема изменилась (добавили unique_id), лучше пересоздать таблицу (для пет-проекта)
    # В проде мы бы делали ALTER TABLE ADD COLUMN
    try:
        # Проверяем, есть ли колонка unique_train_id
        schema = client.query("DESCRIBE TABLE train_delays").result_rows
        columns = [row[0] for row in schema]
        if 'unique_train_id' not in columns:
            print("⚠ Schema changed: Recreating table train_delays...")
            client.command("DROP TABLE IF EXISTS train_delays")
    except:
        pass
    
    # --- SILVER TABLE (ReplacingMergeTree) ---
    # ORDER BY теперь включает unique_train_id для правильной дедупликации
    client.command("""
        CREATE TABLE IF NOT EXISTS train_delays (
            timestamp DateTime,
            city String,
            train_type String,
            train_id String,              -- Human readable "ICE 101"
            planned_departure DateTime,
            actual_departure DateTime,
            delay_in_min Int32,
            is_cancelled UInt8,
            origin String,
            destination String,
            unique_train_id String        -- Internal DB ID "-2348234..."
        ) ENGINE = ReplacingMergeTree(timestamp)
        ORDER BY (city, unique_train_id, planned_departure)
        PARTITION BY toYYYYMM(planned_departure)
    """)
    
    # --- GOLD TABLE ---
    client.command("""
        CREATE TABLE IF NOT EXISTS daily_train_stats (
            stat_date Date,
            city String,
            train_type String,
            total_trains UInt32,
            delayed_trains UInt32,
            avg_delay Float32,
            max_delay Int32,
            created_at DateTime
        ) ENGINE = ReplacingMergeTree(created_at)
        ORDER BY (stat_date, city, train_type)
        PARTITION BY toYYYYMM(stat_date)
    """)
    
    print("✅ ClickHouse tables ready (with unique_train_id)")


def run_silver_dq_checks(records: list) -> dict:
    results = {'critical_failures': [], 'clean_records': []}
    clean = []
    
    # Обновленный индекс, так как теперь 11 полей
    # r = [0:ts, 1:city, 2:type, 3:human_id, 4:planned, 5:actual, 6:delay, 7:canc, 8:orig, 9:dest, 10:UNIQUE_ID]
    
    for r in records:
        # Разворачиваем с учетом, что элементов может быть 10 (старый парсер) или 11 (новый)
        if len(r) == 11:
            timestamp, city, train_type, train_id, planned_dep, actual_dep, delay, cancelled, origin, dest, unique_id = r
        else:
             # Fallback
            timestamp, city, train_type, train_id, planned_dep, actual_dep, delay, cancelled, origin, dest = r
            # Если unique_id нет, временно добавляем пустой или копию train_id, чтобы не падала вставка
            r = list(r)
            r.append(train_id) 
            unique_id = train_id
            
        # DQ Checks
        if not train_id: continue
        if not planned_dep: continue
        if delay > 1000: continue 
        
        clean.append(r) # Добавляем запись (возможно модифицированную)
        
    results['clean_records'] = clean
    return results


# =============================================================================
# GOLD LAYER: Aggregation
# =============================================================================

def gold_aggregate(**context):
    print("=" * 60)
    print("🥇 GOLD LAYER: Starting aggregation")
    print("=" * 60)
    
    client = get_ch_client()
    
    # Обновляем Gold слой (за последние сутки)
    client.command("""
        ALTER TABLE daily_train_stats DELETE 
        WHERE stat_date >= toDate(now() - INTERVAL 1 DAY)
    """)
    
    # Агрегация из Silver в Gold
    # Благодаря правильному ORDER BY в Silver, дубликатов быть не должно (после FINAL)
    query = """
    INSERT INTO daily_train_stats
    SELECT
        toDate(planned_departure) as stat_date,
        city,
        train_type,
        count() as total_trains,
        countIf(delay_in_min > 5) as delayed_trains,
        avgIf(delay_in_min, delay_in_min > 5) as avg_delay,
        maxIf(delay_in_min, delay_in_min > 5) as max_delay,
        now() as created_at
    FROM train_delays FINAL
    WHERE planned_departure >= toStartOfDay(now() - INTERVAL 1 DAY)
    AND planned_departure <= now()  -- Только состоявшиеся поезда!
    GROUP BY stat_date, city, train_type
    HAVING total_trains > 0
    """
    
    client.command(query)
    
    # Статистика для лога
    count = client.query("SELECT count() FROM daily_train_stats WHERE stat_date = toDate(now())").result_rows[0][0]
    log_pipeline_stage(DAG_ID, 'gold', 'SUCCESS', count)


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Deutsche Bahn train delays: Bronze -> Silver -> Gold pipeline',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['deutsche-bahn', 'medallion', 'trains'],
) as dag:
    
    bronze = PythonOperator(task_id='bronze_extract', python_callable=bronze_extract, provide_context=True)
    silver = PythonOperator(task_id='silver_transform', python_callable=silver_transform, provide_context=True)
    gold = PythonOperator(task_id='gold_aggregate', python_callable=gold_aggregate, provide_context=True)
    
    bronze >> silver >> gold