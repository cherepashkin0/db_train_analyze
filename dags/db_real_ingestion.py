import asyncio
import os
import json
import clickhouse_connect
from datetime import datetime, timedelta
from api_client import fetch_and_save
from iris_parser import parse_db_xml
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- КОНФИГУРАЦИЯ ---
def load_config():
    """Загружает конфигурацию станций."""
    base_dir = "/opt/airflow/dags"
    config_path = os.path.join(base_dir, "config", "railway_config.json")
    
    print(f"🔍 Ищу конфиг здесь: {config_path}")
    
    # Отладка путей
    try:
        config_dir = os.path.join(base_dir, "config")
        if os.path.exists(config_dir):
            print(f"📂 Содержимое папки {config_dir}: {os.listdir(config_dir)}")
    except: pass

    if os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                print(f"✅ Конфиг успешно загружен: {len(config.get('stations', {}))} станций")
                return config
        except Exception as e:
            print(f"❌ Ошибка чтения JSON: {e}")
    
    return {
        "stations": {"8011160": "Berlin Hbf"}, 
        "monitored_types": []
    }

# --- HELPER: CLICKHOUSE CLIENT ---
def get_ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD')
    )

# --- ЛОГИРОВАНИЕ ---
def log_status(context, stage, status, msg=""):
    """Пишет статус этапа в консоль и в Postgres."""
    print(f"[{stage}] {status}: {msg}")
    
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = """
            INSERT INTO api_ingestion_log (dag_id, execution_date, status, error_message)
            VALUES (%s, %s, %s, %s)
        """
        dag_id = str(context['dag'].dag_id)
        execution_date = str(context.get('execution_date', datetime.now()))
        
        pg_hook.run("""
            CREATE TABLE IF NOT EXISTS api_ingestion_log (
                run_id SERIAL PRIMARY KEY,
                dag_id VARCHAR(50),
                execution_date VARCHAR(50),
                status VARCHAR(20),
                error_message TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        
        pg_hook.run(sql, parameters=(dag_id, execution_date, status, f"{stage}: {msg}"))
    except Exception as e:
        print(f"⚠ Ошибка записи лога в Postgres: {e}")

# ==========================================
# 1. EXTRACT DATA (API -> Parquet/Bronze)
# ==========================================
async def extract_data(config):
    stations = config.get("stations", {})
    queries = []
    for eva_id in stations.keys():
        queries.append({"url": f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/{eva_id}"})
    
    print(f"🌍 TASK 1: EXTRACT. Загрузка данных для {len(stations)} станций...")
    return await fetch_and_save(
        queries=queries,
        output_path="/opt/airflow/data/raw_api_data",
        max_concurrent=5,
        rate_limit=60
    )

# ==========================================
# 2. LOAD TO SILVER (Parquet -> ClickHouse Raw)
# ==========================================
def load_to_silver(df, config):
    print("📥 TASK 2: LOAD TO SILVER...")
    stations = config.get("stations", {})
    target_types = set(config.get("monitored_types", []))
    
    all_parsed = []
    for _, row in df.iterrows():
        if row['error']: continue
        eva_id = row['url'].split('/')[-1]
        city = stations.get(eva_id, "Unknown")
        
        if row['response_data']:
            rows = parse_db_xml(row['response_data'], city)
            if target_types:
                rows = [r for r in rows if r[2] in target_types]
            all_parsed.extend(rows)
            
    if not all_parsed:
        print("⚠ LOAD: Нет данных для вставки.")
        return 0

    client = get_ch_client()
    
    client.insert('train_delays', all_parsed, 
                  column_names=['timestamp', 'city', 'train_type', 'train_id', 
                                'planned_departure', 'actual_departure', 
                                'delay_in_min', 'is_cancelled', 'origin', 'destination'])
    print(f"✅ LOAD: Вставлено {len(all_parsed)} строк в Silver слой (train_delays).")
    return len(all_parsed)

# ==========================================
# 3. DATA QUALITY CHECK (Validation)
# ==========================================
def data_quality_check():
    print("🧐 TASK 3: DATA QUALITY CHECK...")
    client = get_ch_client()
    
    # === НОВЫЕ ТЕСТЫ ===
    checks = [
        # 1. Validate for Nulls (Пустые ID поездов или городов)
        ("Null Check: Train IDs", 
         "SELECT count() FROM train_delays WHERE train_id = '' AND actual_departure > now() - INTERVAL 1 HOUR"),
         
        ("Null Check: Cities", 
         "SELECT count() FROM train_delays WHERE city = '' AND actual_departure > now() - INTERVAL 1 HOUR"),

        # 2. Test Range Constraints (Задержка должна быть адекватной)
        ("Range Check: Negative Delays", 
         "SELECT count() FROM train_delays WHERE delay_in_min < 0"),
         
        ("Range Check: Extreme Delays (>1000 min)", 
         "SELECT count() FROM train_delays WHERE delay_in_min > 1000 AND actual_departure > now() - INTERVAL 1 HOUR"),
         
        ("Range Check: Future Data (>2 Days)", 
         "SELECT count() FROM train_delays WHERE actual_departure > now() + INTERVAL 2 DAY"),

        # 3. Verify Referential Integrity (Проверяем, что город известен)
        ("Ref Integrity: Unknown Stations", 
         "SELECT count() FROM train_delays WHERE city = 'Unknown' AND actual_departure > now() - INTERVAL 1 HOUR")
    ]
    
    failed_checks = []
    
    for check_name, sql in checks:
        try:
            # Получаем результат (число строк, нарушающих правило)
            result = client.query(sql).result_rows[0][0]
            
            if result > 0:
                msg = f"❌ DQ FAIL: {check_name} -> найдено {result} плохих записей"
                print(msg)
                # Для некоторых проверок можно не ронять пайплайн, а просто алертить
                # Но для строгих требований спринта - добавляем в список ошибок
                failed_checks.append(msg)
            else:
                print(f"✅ DQ PASS: {check_name}")
                
        except Exception as e:
            print(f"⚠ Ошибка при выполнении проверки {check_name}: {e}")
            failed_checks.append(f"SQL Error in {check_name}: {e}")
            
    if failed_checks:
        # Роняем пайплайн, если есть ошибки качества
        raise Exception(f"Data Quality Checks Failed:\n" + "\n".join(failed_checks))

# ==========================================
# 4. TRANSFORM GOLD (Silver -> Aggregated)
# ==========================================
def transform_gold():
    print("🔨 TASK 4: TRANSFORM GOLD...")
    client = get_ch_client()
    
    query = """
    INSERT INTO daily_train_stats
    SELECT
        toDate(actual_departure) as stat_date,
        city,
        train_type,
        count() as total_trains,
        countIf(delay_in_min > 0) as delayed_trains,
        avgIf(delay_in_min, delay_in_min > 0) as avg_delay,
        max(delay_in_min) as max_delay,
        now() as created_at
    FROM train_delays
    WHERE actual_departure >= toStartOfDay(now())
    GROUP BY stat_date, city, train_type
    """
    
    client.command("ALTER TABLE daily_train_stats DELETE WHERE stat_date = toDate(now())")
    client.command(query)
    print("✅ TRANSFORM: Gold слой (daily_train_stats) обновлен.")

# --- ORCHESTRATOR (Внутри скрипта) ---
# Примечание: Чтобы видеть эти шаги графически в Airflow, 
# нужно вызывать extract_data, load_to_silver и т.д. 
# отдельными PythonOperator в файле DAG, а не здесь.
# Но пока оставляем так для работоспособности текущего кода.

async def run_pipeline(context):
    config = load_config()
    
    # 1. EXTRACT
    try:
        df = await extract_data(config)
        
        # Tech Check: API health
        failed_count = df['error'].notna().sum()
        if failed_count == len(df) and len(df) > 0:
            raise Exception("CRITICAL: Все запросы к API упали.")
        if failed_count > 0:
            print(f"⚠ WARNING: {failed_count}/{len(df)} запросов с ошибкой.")
    except Exception as e:
        log_status(context, "EXTRACT", "FAILED", str(e))
        raise

    # 2. LOAD
    try:
        count = load_to_silver(df, config)
    except Exception as e:
        log_status(context, "LOAD", "FAILED", str(e))
        raise

    # 3. DQ CHECK
    if count > 0:
        try:
            data_quality_check()
        except Exception as e:
            log_status(context, "DQ_CHECK", "FAILED", str(e))
            raise

        # 4. TRANSFORM
        try:
            transform_gold()
        except Exception as e:
            log_status(context, "TRANSFORM", "FAILED", str(e))
            raise
            
    log_status(context, "PIPELINE", "SUCCESS", f"Processed {count} records")

def main(**kwargs):
    asyncio.run(run_pipeline(kwargs))