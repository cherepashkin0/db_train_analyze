"""
Diagnostic script to find where train data is being lost.
Run this in Airflow worker or locally with access to Bronze parquet files.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
import sys

# Add path to iris_parser
sys.path.insert(0, '/opt/airflow/dags')

from iris_parser import parse_plan_xml, parse_fchg_xml

BRONZE_PATH = "/opt/airflow/data/bronze/train_api"


def diagnose():
    print("=" * 70)
    print("🔍 DIAGNOSTIC: Finding where train data is lost")
    print("=" * 70)
    
    bronze_path = Path(BRONZE_PATH)
    
    # 1. Read all parquet files
    parquet_files = list(bronze_path.glob("**/*.parquet"))
    print(f"\n📂 Found {len(parquet_files)} parquet files")
    
    if not parquet_files:
        print("❌ No parquet files found!")
        return
    
    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)
    
    # Filter recent
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    cutoff = datetime.now() - timedelta(days=2)
    df = df[df['timestamp'] >= cutoff]
    
    print(f"\n📊 Bronze layer stats:")
    print(f"   Total rows: {len(df)}")
    print(f"   With errors: {df['error'].notna().sum()}")
    print(f"   With response_data: {df['response_data'].notna().sum()}")
    
    # 2. Check by query_type
    print(f"\n📊 By query_type:")
    print(df['query_type'].value_counts().to_string())
    
    # 3. Check by station
    print(f"\n📊 By station (top 10):")
    print(df['station_name'].value_counts().head(10).to_string())
    
    # 4. Parse and count BEFORE deduplication
    print("\n" + "=" * 70)
    print("🔬 Parsing XML responses (BEFORE deduplication)")
    print("=" * 70)
    
    all_records_plan = []
    all_records_fchg = []
    
    for _, row in df.iterrows():
        if row['error'] or not row['response_data']:
            continue
        
        station_name = row.get('station_name', 'Unknown')
        query_type = row.get('query_type', 'plan')
        
        try:
            if query_type == 'fchg':
                rows = parse_fchg_xml(row['response_data'], station_name)
                all_records_fchg.extend(rows)
            else:
                rows = parse_plan_xml(row['response_data'], station_name)
                all_records_plan.extend(rows)
        except Exception as e:
            pass
    
    print(f"\n📈 Parsed records (BEFORE dedup):")
    print(f"   Plan records: {len(all_records_plan)}")
    print(f"   Fchg records: {len(all_records_fchg)}")
    print(f"   Total: {len(all_records_plan) + len(all_records_fchg)}")
    
    # 5. Check for Berlin specifically
    berlin_plan = [r for r in all_records_plan if 'Berlin' in r[1]]
    berlin_fchg = [r for r in all_records_fchg if 'Berlin' in r[1]]
    
    print(f"\n🏙 Berlin Hbf specifically:")
    print(f"   Plan records: {len(berlin_plan)}")
    print(f"   Fchg records: {len(berlin_fchg)}")
    
    # 6. Check unique train_ids for Berlin
    berlin_train_ids = set(r[3] for r in berlin_plan)
    print(f"   Unique train_ids in plan: {len(berlin_train_ids)}")
    
    # 7. Sample of train_ids
    print(f"\n   Sample train_ids (first 20):")
    for tid in list(berlin_train_ids)[:20]:
        print(f"      - {tid}")
    
    # 8. Check planned_departure distribution for Berlin
    print(f"\n📅 Berlin planned_departure distribution (plan data):")
    berlin_times = [r[4] for r in berlin_plan if r[4]]  # planned_departure
    if berlin_times:
        from collections import Counter
        hour_counts = Counter(t.hour for t in berlin_times)
        for hour in sorted(hour_counts.keys()):
            print(f"   Hour {hour:02d}: {hour_counts[hour]} trains")
    
    # 9. NOW apply deduplication and see what happens
    print("\n" + "=" * 70)
    print("🔄 Applying deduplication (AFTER)")
    print("=" * 70)
    
    trains_dict = {}
    
    # First add plan records
    for row_data in all_records_plan:
        key = (row_data[3], row_data[4], row_data[1])  # (train_id, planned_departure, city)
        if key not in trains_dict:
            trains_dict[key] = row_data
    
    print(f"   After adding plan: {len(trains_dict)} unique records")
    
    # Then add fchg (overwrites)
    overwrites = 0
    for row_data in all_records_fchg:
        key = (row_data[3], row_data[4], row_data[1])
        if key in trains_dict:
            overwrites += 1
        trains_dict[key] = row_data
    
    print(f"   After adding fchg: {len(trains_dict)} unique records")
    print(f"   Fchg overwrites: {overwrites}")
    
    # 10. Check Berlin after dedup
    berlin_after = [r for r in trains_dict.values() if 'Berlin' in r[1]]
    print(f"\n🏙 Berlin after dedup: {len(berlin_after)} records")
    
    # 11. Check for duplicate keys issue
    print("\n" + "=" * 70)
    print("🔍 Checking for key collision issues")
    print("=" * 70)
    
    # Count how many times each key appears in plan data
    key_counts = defaultdict(int)
    for row_data in all_records_plan:
        key = (row_data[3], row_data[4], row_data[1])
        key_counts[key] += 1
    
    duplicates = {k: v for k, v in key_counts.items() if v > 1}
    print(f"   Keys appearing multiple times: {len(duplicates)}")
    
    if duplicates:
        print(f"\n   Sample duplicate keys (showing 10):")
        for key, count in list(duplicates.items())[:10]:
            train_id, planned_dep, city = key
            print(f"      {train_id} @ {planned_dep} in {city}: {count} times")
    
    # 12. Check if planned_departure has time component
    print("\n" + "=" * 70)
    print("🕐 Checking planned_departure precision")
    print("=" * 70)
    
    sample_times = [r[4] for r in all_records_plan[:100] if r[4]]
    print(f"   Sample planned_departure values:")
    for t in sample_times[:10]:
        print(f"      {t} (type: {type(t).__name__})")
    
    # 13. Final summary
    print("\n" + "=" * 70)
    print("📋 SUMMARY")
    print("=" * 70)
    
    print(f"""
    Bronze API responses: {len(df)}
    Parsed plan records: {len(all_records_plan)}
    Parsed fchg records: {len(all_records_fchg)}
    After deduplication: {len(trains_dict)}
    
    Loss analysis:
    - Plan to dedup loss: {len(all_records_plan) - len([r for r in trains_dict.values()])} records
    - This could be due to:
      1. Same train appearing in multiple hourly API calls
      2. Key collision (train_id format issue)
      3. Timezone issues with planned_departure
    """)


if __name__ == "__main__":
    diagnose()