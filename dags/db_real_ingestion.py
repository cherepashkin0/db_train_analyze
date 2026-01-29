"""
Deutsche Bahn Train Delays Pipeline
====================================
Medallion Architecture: Bronze -> Silver -> Gold

Bronze: Raw API data extraction (no transformation)
Silver: Parsing, cleaning, validation, business rules
Gold: Aggregated metrics for dashboards
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import asyncio
import os
import json
import clickhouse_connect
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
    """Get ClickHouse client."""
    return clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD')
    )


def log_pipeline_stage(dag_id: str, stage: str, status: str, records_processed: int = 0, error_message: str = None):
    """Log pipeline stage to Postgres metadata table."""
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Ensure table exists
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
            CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_stage ON pipeline_runs(dag_id, stage, created_at);
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
    """
    BRONZE LAYER: Extract raw data from Deutsche Bahn API.
    
    - No parsing, no transformation
    - Just fetch and save raw XML responses to Parquet
    - Fast failure if API is down
    """
    print("=" * 60)
    print("🥉 BRONZE LAYER: Starting raw data extraction")
    print("=" * 60)
    
    config = load_config()
    stations = config.get("stations", {})
    hours_back = config.get("hours_back", 12)
    hours_forward = config.get("hours_forward", 12)
    
    # Generate queries
    plan_queries = generate_plan_queries(stations, hours_back, hours_forward)
    fchg_queries = generate_fchg_queries(stations)
    all_queries = plan_queries + fchg_queries
    
    print(f"📡 API Queries:")
    print(f"   - Stations: {len(stations)}")
    print(f"   - Plan queries: {len(plan_queries)}")
    print(f"   - Fchg queries: {len(fchg_queries)}")
    print(f"   - Total: {len(all_queries)}")
    
    # Fetch data
    async def run_fetch():
        return await fetch_and_save(
            queries=all_queries,
            output_path=BRONZE_PATH,
            max_concurrent=10,
            rate_limit=60,
        )
    
    df = asyncio.run(run_fetch())
    
    # Check API health
    total = len(df)
    failed = df['error'].notna().sum()
    success = total - failed
    
    if total == 0:
        log_pipeline_stage(DAG_ID, 'bronze', 'FAILED', 0, "No queries generated")
        raise Exception("CRITICAL: No queries generated")
    
    if failed == total:
        log_pipeline_stage(DAG_ID, 'bronze', 'FAILED', 0, "All API requests failed")
        raise Exception("CRITICAL: All API requests failed")
    
    success_rate = success / total * 100
    print(f"✅ Bronze complete: {success}/{total} requests successful ({success_rate:.1f}%)")
    
    if failed > 0:
        print(f"⚠ WARNING: {failed} requests failed")
    
    # Log success
    log_pipeline_stage(DAG_ID, 'bronze', 'SUCCESS', success)
    
    # Pass data path to next task via XCom
    context['ti'].xcom_push(key='bronze_path', value=BRONZE_PATH)
    context['ti'].xcom_push(key='bronze_records', value=total)
    
    return total


# =============================================================================
# SILVER LAYER: Parsing, Cleaning, Validation
# =============================================================================

def silver_transform(**context):
    """
    SILVER LAYER: Parse, clean, validate and apply business rules.
    
    - Parse XML responses
    - Clean and normalize data
    - Apply business rules (merge plan + fchg)
    - Data quality checks
    - Load to ClickHouse
    """
    print("=" * 60)
    print("🥈 SILVER LAYER: Starting transformation")
    print("=" * 60)
    
    import pandas as pd
    import pyarrow.parquet as pq
    from pathlib import Path
    
    config = load_config()
    target_types = set(config.get("monitored_types", []))
    
    # Ensure ClickHouse tables exist
    ensure_silver_tables()
    
    # Read bronze data
    bronze_path = context['ti'].xcom_pull(key='bronze_path', task_ids='bronze_extract')
    bronze_path = Path(bronze_path or BRONZE_PATH)
    
    print(f"📂 Reading bronze data from: {bronze_path}")
    
    # Find all parquet files
    parquet_files = list(bronze_path.glob("**/*.parquet"))
    if not parquet_files:
        log_pipeline_stage(DAG_ID, 'silver', 'FAILED', 0, "No bronze data found")
        raise Exception("No bronze data found")
    
    # Read and combine
    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)
    
    # Filter to recent data only (last 2 days)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    cutoff = datetime.now() - timedelta(days=2)
    df = df[df['timestamp'] >= cutoff]
    
    print(f"📊 Bronze records loaded: {len(df)}")
    
    # --- PARSING ---
    print("\n🔄 Parsing XML responses...")
    
    trains_dict = {}  # Key: (train_id, planned_departure, city) -> row
    
    plan_count = 0
    fchg_count = 0
    parse_errors = 0
    
    for _, row in df.iterrows():
        if row['error'] or not row['response_data']:
            continue
        
        station_name = row.get('station_name', 'Unknown')
        query_type = row.get('query_type', 'plan')
        
        try:
            if query_type == 'fchg':
                rows = parse_fchg_xml(row['response_data'], station_name)
                fchg_count += len(rows)
            else:
                rows = parse_plan_xml(row['response_data'], station_name)
                plan_count += len(rows)
            
            # Filter by train type if configured
            if target_types:
                rows = [r for r in rows if r[2] in target_types]
            
            # Merge: fchg overwrites plan
            for row_data in rows:
                key = (row_data[3], row_data[4], row_data[1])  # (train_id, planned_departure, city)
                if query_type == 'fchg':
                    trains_dict[key] = row_data
                elif key not in trains_dict:
                    trains_dict[key] = row_data
                    
        except Exception as e:
            parse_errors += 1
            if parse_errors <= 5:
                print(f"⚠ Parse error for {station_name}: {e}")
    
    print(f"\n📈 Parsing results:")
    print(f"   - Plan records: {plan_count}")
    print(f"   - Fchg records: {fchg_count}")
    print(f"   - Parse errors: {parse_errors}")
    print(f"   - Unique trains after merge: {len(trains_dict)}")
    
    if not trains_dict:
        log_pipeline_stage(DAG_ID, 'silver', 'FAILED', 0, "No records after parsing")
        raise Exception("No records after parsing")
    
    all_records = list(trains_dict.values())
    
    # --- DATA QUALITY CHECKS ---
    print("\n🧪 Running data quality checks...")
    
    dq_results = run_silver_dq_checks(all_records)
    
    if dq_results['critical_failures']:
        error_msg = "; ".join(dq_results['critical_failures'])
        log_pipeline_stage(DAG_ID, 'silver', 'FAILED', 0, f"DQ Failed: {error_msg}")
        raise Exception(f"Data Quality Failed: {error_msg}")
    
    # Filter out bad records
    clean_records = dq_results['clean_records']
    
    print(f"\n✅ DQ passed: {len(clean_records)} clean records (removed {len(all_records) - len(clean_records)} bad)")
    
    # --- LOAD TO CLICKHOUSE ---
    print("\n💾 Loading to ClickHouse Silver layer...")
    
    client = get_ch_client()
    
    client.insert('train_delays', clean_records,
                  column_names=['timestamp', 'city', 'train_type', 'train_id',
                                'planned_departure', 'actual_departure',
                                'delay_in_min', 'is_cancelled', 'origin', 'destination'])
    
    # Deduplicate
    client.command("OPTIMIZE TABLE train_delays FINAL")
    
    # Statistics
    delayed_count = sum(1 for r in clean_records if r[6] > 5)
    cancelled_count = sum(1 for r in clean_records if r[7] == 1)
    
    print(f"\n📊 Silver layer statistics:")
    print(f"   - Total records: {len(clean_records)}")
    print(f"   - Delayed (>5min): {delayed_count}")
    print(f"   - Cancelled: {cancelled_count}")
    
    log_pipeline_stage(DAG_ID, 'silver', 'SUCCESS', len(clean_records))
    
    context['ti'].xcom_push(key='silver_records', value=len(clean_records))
    context['ti'].xcom_push(key='silver_delayed', value=delayed_count)
    
    return len(clean_records)


def ensure_silver_tables():
    """Create ClickHouse tables if not exist."""
    client = get_ch_client()
    
    # Check if migration needed
    try:
        result = client.query("SELECT engine FROM system.tables WHERE name = 'train_delays' AND database = 'default'")
        if result.result_rows:
            engine = result.result_rows[0][0]
            if 'ReplacingMergeTree' not in engine:
                print(f"⚠ Migrating train_delays from {engine} to ReplacingMergeTree")
                client.command("DROP TABLE IF EXISTS train_delays")
    except:
        pass
    
    client.command("""
        CREATE TABLE IF NOT EXISTS train_delays (
            timestamp DateTime,
            city String,
            train_type String,
            train_id String,
            planned_departure DateTime,
            actual_departure DateTime,
            delay_in_min Int32,
            is_cancelled UInt8,
            origin String,
            destination String
        ) ENGINE = ReplacingMergeTree(timestamp)
        ORDER BY (city, train_id, planned_departure)
        PARTITION BY toYYYYMM(planned_departure)
    """)
    
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
    
    print("✅ ClickHouse tables ready")


def run_silver_dq_checks(records: list) -> dict:
    """
    Run data quality checks on parsed records.
    
    Returns dict with:
    - critical_failures: list of critical errors (pipeline should fail)
    - warnings: list of warnings (logged but pipeline continues)
    - clean_records: filtered records that passed all checks
    """
    results = {
        'critical_failures': [],
        'warnings': [],
        'clean_records': [],
        'stats': {}
    }
    
    clean = []
    
    # Counters for DQ metrics
    null_train_id = 0
    null_city = 0
    null_time = 0
    extreme_delay = 0
    future_data = 0
    negative_extreme = 0
    
    now = datetime.now()
    max_future = now + timedelta(days=7)
    
    for r in records:
        # r = (timestamp, city, train_type, train_id, planned_departure, 
        #      actual_departure, delay_in_min, is_cancelled, origin, destination)
        
        timestamp, city, train_type, train_id, planned_dep, actual_dep, delay, cancelled, origin, dest = r
        
        # Check 1: Null train_id
        if not train_id or train_id.strip() == '':
            null_train_id += 1
            continue
        
        # Check 2: Null city
        if not city or city.strip() == '' or city == 'Unknown':
            null_city += 1
            continue
        
        # Check 3: Null planned_departure
        if not planned_dep:
            null_time += 1
            continue
        
        # Check 4: Extreme positive delay (>1000 min = 16+ hours)
        if delay > 1000:
            extreme_delay += 1
            continue
        
        # Check 5: Extreme negative delay (<-60 min)
        if delay < -60:
            negative_extreme += 1
            continue
        
        # Check 6: Future data (>7 days)
        if planned_dep > max_future:
            future_data += 1
            continue
        
        # Record passed all checks
        clean.append(r)
    
    # Log stats
    total = len(records)
    removed = total - len(clean)
    
    print(f"   ├─ Null train_id: {null_train_id}")
    print(f"   ├─ Null/Unknown city: {null_city}")
    print(f"   ├─ Null planned_time: {null_time}")
    print(f"   ├─ Extreme delay (>1000min): {extreme_delay}")
    print(f"   ├─ Extreme negative (<-60min): {negative_extreme}")
    print(f"   ├─ Future data (>7days): {future_data}")
    print(f"   └─ Total removed: {removed} ({removed/total*100:.1f}%)")
    
    # Critical failure if too many bad records
    bad_rate = removed / total if total > 0 else 0
    if bad_rate > 0.5:
        results['critical_failures'].append(f"Too many bad records: {bad_rate*100:.1f}%")
    
    # Critical failure if no clean records
    if len(clean) == 0:
        results['critical_failures'].append("No clean records after DQ")
    
    results['clean_records'] = clean
    results['stats'] = {
        'total': total,
        'clean': len(clean),
        'removed': removed,
        'null_train_id': null_train_id,
        'null_city': null_city,
        'extreme_delay': extreme_delay,
    }
    
    return results


# =============================================================================
# GOLD LAYER: Aggregation for Analytics
# =============================================================================

def gold_aggregate(**context):
    """
    GOLD LAYER: Create aggregated metrics for dashboards.
    
    - Daily statistics per city and train type
    - Pre-computed KPIs
    - Optimized for BI queries
    """
    print("=" * 60)
    print("🥇 GOLD LAYER: Starting aggregation")
    print("=" * 60)
    
    client = get_ch_client()
    
    # Delete existing data for today and yesterday (will be replaced)
    print("🗑 Clearing old Gold data...")
    client.command("""
        ALTER TABLE daily_train_stats DELETE 
        WHERE stat_date >= toDate(now() - INTERVAL 1 DAY)
    """)
    
    # Aggregate from Silver to Gold
    # DB Standard: delay > 5 minutes = officially late
    print("📊 Aggregating Silver -> Gold...")
    
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
      AND planned_departure < toStartOfDay(now() + INTERVAL 1 DAY)
    GROUP BY stat_date, city, train_type
    HAVING total_trains > 0
    """
    
    client.command(query)
    
    # Get stats for logging
    stats = client.query("""
        SELECT 
            count() as rows,
            sum(total_trains) as trains,
            sum(delayed_trains) as delayed
        FROM daily_train_stats FINAL
        WHERE stat_date = toDate(now())
    """).result_rows[0]
    
    rows, trains, delayed = stats
    delay_rate = (delayed / trains * 100) if trains > 0 else 0
    
    print(f"\n📈 Gold layer statistics (today):")
    print(f"   - Aggregation rows: {rows}")
    print(f"   - Total trains: {trains}")
    print(f"   - Delayed trains: {delayed} ({delay_rate:.1f}%)")
    
    log_pipeline_stage(DAG_ID, 'gold', 'SUCCESS', int(trains or 0))
    
    return int(trains or 0)


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Deutsche Bahn train delays: Bronze -> Silver -> Gold pipeline',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['deutsche-bahn', 'medallion', 'trains'],
) as dag:
    
    # Task 1: Bronze - Extract raw data
    bronze_task = PythonOperator(
        task_id='bronze_extract',
        python_callable=bronze_extract,
        provide_context=True,
    )
    
    # Task 2: Silver - Transform and validate
    silver_task = PythonOperator(
        task_id='silver_transform',
        python_callable=silver_transform,
        provide_context=True,
    )
    
    # Task 3: Gold - Aggregate for analytics
    gold_task = PythonOperator(
        task_id='gold_aggregate',
        python_callable=gold_aggregate,
        provide_context=True,
    )
    
    # Define dependencies: Bronze -> Silver -> Gold
    bronze_task >> silver_task >> gold_task