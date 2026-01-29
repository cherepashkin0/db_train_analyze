"""
Quick check: how many unique dates do we have?
"""
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter
import sys

sys.path.insert(0, '/opt/airflow/dags')
from iris_parser import parse_plan_xml

BRONZE_PATH = "/opt/airflow/data/bronze/train_api"

def check_dates():
    bronze_path = Path(BRONZE_PATH)
    parquet_files = list(bronze_path.glob("**/*.parquet"))
    
    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)
    
    # Filter recent
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    cutoff = datetime.now() - timedelta(days=7)
    df = df[df['timestamp'] >= cutoff]
    
    # Parse and collect dates
    all_dates = []
    berlin_dates = []
    
    for _, row in df.iterrows():
        if row['error'] or not row['response_data'] or row['query_type'] != 'plan':
            continue
        
        station_name = row.get('station_name', 'Unknown')
        
        try:
            rows = parse_plan_xml(row['response_data'], station_name)
            for r in rows:
                planned_dep = r[4]  # planned_departure
                if planned_dep:
                    all_dates.append(planned_dep.date())
                    if 'Berlin' in r[1]:
                        berlin_dates.append(planned_dep.date())
        except:
            pass
    
    print("=" * 60)
    print("📅 DATE DISTRIBUTION")
    print("=" * 60)
    
    print("\n📊 All stations - dates:")
    date_counts = Counter(all_dates)
    for date, count in sorted(date_counts.items()):
        print(f"   {date}: {count} trains")
    
    print(f"\n   Total unique dates: {len(date_counts)}")
    print(f"   Total train records: {len(all_dates)}")
    
    print("\n🏙 Berlin Hbf - dates:")
    berlin_counts = Counter(berlin_dates)
    for date, count in sorted(berlin_counts.items()):
        print(f"   {date}: {count} trains")
    
    print(f"\n   Berlin unique dates: {len(berlin_counts)}")
    print(f"   Berlin total records: {len(berlin_dates)}")
    
    # Check today specifically
    today = datetime.now().date()
    print(f"\n📌 TODAY ({today}):")
    print(f"   All stations: {date_counts.get(today, 0)} trains")
    print(f"   Berlin: {berlin_counts.get(today, 0)} trains")


if __name__ == "__main__":
    check_dates()