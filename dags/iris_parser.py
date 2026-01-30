# iris_parser.py
"""
Парсер для Deutsche Bahn Timetables API (IRIS).

Поддерживает:
- /plan/{evaNo}/{YYMMDD}/{HH} - запланированное расписание
- /fchg/{evaNo} - изменения (для будущего использования)

Формат времени в API: YYMMddHHmm (например, 2501281430 = 28.01.2025 14:30)
"""

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional


def parse_iris_timestamp(ts: str) -> Optional[datetime]:
    """
    Парсит timestamp в формате IRIS API: YYMMddHHmm
    Например: 2501281430 -> 2025-01-28 14:30:00
    """
    if not ts or len(ts) != 10:
        return None
    try:
        return datetime.strptime(ts, "%y%m%d%H%M")
    except ValueError:
        return None


def parse_plan_xml(xml_data: str, city: str) -> list[tuple]:
    """
    Парсит XML ответ от /plan endpoint.
    
    Возвращает список кортежей для вставки в ClickHouse:
    (timestamp, city, train_type, train_id, planned_departure, actual_departure, 
     delay_in_min, is_cancelled, origin, destination, unique_id)
    """
    results = []
    now = datetime.now()
    
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        print(f"XML Parse Error: {e}")
        return results
    
    station_name = root.get('station', city)
    
    for stop in root.findall('.//s'):
        try:
            # Внутренний уникальный ID (например: "-1071076418553484331-2601281431-1")
            internal_id = stop.get('id', '')
            
            tl = stop.find('tl')
            if tl is not None:
                train_type = tl.get('c', '')
                train_number = tl.get('n', '')
                if train_type and train_number:
                    train_id = f"{train_type} {train_number}"
                else:
                    train_id = internal_id
            else:
                train_type = ''
                train_id = internal_id
            
            # Arrival
            ar = stop.find('ar')
            arrival_time = None
            origin = ''
            if ar is not None:
                pt = ar.get('pt', '')
                arrival_time = parse_iris_timestamp(pt)
                ppth = ar.get('ppth', '')
                if ppth:
                    origin = ppth.split('|')[0]
            
            # Departure
            dp = stop.find('dp')
            departure_time = None
            destination = ''
            if dp is not None:
                pt = dp.get('pt', '')
                departure_time = parse_iris_timestamp(pt)
                ppth = dp.get('ppth', '')
                if ppth:
                    destination = ppth.split('|')[-1]
            
            if not origin and destination: origin = station_name
            if not destination and origin: destination = station_name
            
            planned_time = departure_time or arrival_time
            
            if not planned_time:
                continue
            
            actual_time = planned_time
            delay_in_min = 0
            is_cancelled = 0
            
            results.append((
                now,            # 0
                station_name,   # 1
                train_type,     # 2
                train_id,       # 3 (Human Readable)
                planned_time,   # 4
                actual_time,    # 5
                delay_in_min,   # 6
                is_cancelled,   # 7
                origin,         # 8
                destination,    # 9
                internal_id     # 10 <--- НОВОЕ ПОЛЕ (Unique ID)
            ))
            
        except Exception as e:
            print(f"Error parsing stop: {e}")
            continue
    
    return results


def parse_fchg_xml(xml_data: str, city: str) -> list[tuple]:
    """
    Парсит XML ответ от /fchg endpoint (full changes).
    """
    results = []
    now = datetime.now()
    
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        print(f"XML Parse Error: {e}")
        return results
    
    station_name = root.get('station', city)
    
    for stop in root.findall('.//s'):
        try:
            # Внутренний уникальный ID
            internal_id = stop.get('id', '')
            
            tl = stop.find('tl')
            if tl is not None:
                train_type = tl.get('c', '')
                train_number = tl.get('n', '')
                if train_type and train_number:
                    train_id = f"{train_type} {train_number}"
                else:
                    train_id = internal_id
            else:
                train_type = ''
                train_id = internal_id
            
            # Arrival
            ar = stop.find('ar')
            arrival_planned = None
            arrival_changed = None
            origin = ''
            ar_cancelled = False
            
            if ar is not None:
                pt = ar.get('pt', '')
                ct = ar.get('ct', '')
                cs = ar.get('cs', '')
                
                arrival_planned = parse_iris_timestamp(pt)
                arrival_changed = parse_iris_timestamp(ct) if ct else arrival_planned
                ar_cancelled = (cs == 'c')
                
                ppth = ar.get('ppth', '')
                if ppth:
                    origin = ppth.split('|')[0]
            
            # Departure
            dp = stop.find('dp')
            departure_planned = None
            departure_changed = None
            destination = ''
            dp_cancelled = False
            
            if dp is not None:
                pt = dp.get('pt', '')
                ct = dp.get('ct', '')
                cs = dp.get('cs', '')
                
                departure_planned = parse_iris_timestamp(pt)
                departure_changed = parse_iris_timestamp(ct) if ct else departure_planned
                dp_cancelled = (cs == 'c')
                
                ppth = dp.get('ppth', '')
                if ppth:
                    destination = ppth.split('|')[-1]
            
            if not origin and destination: origin = station_name
            if not destination and origin: destination = station_name
            
            planned_time = departure_planned or arrival_planned
            actual_time = departure_changed or arrival_changed
            
            if not planned_time:
                continue
            
            if actual_time and planned_time:
                delay_in_min = int((actual_time - planned_time).total_seconds() / 60)
            else:
                delay_in_min = 0
            
            is_cancelled = 1 if (ar_cancelled or dp_cancelled) else 0
            
            results.append((
                now,
                station_name,
                train_type,
                train_id,
                planned_time,
                actual_time or planned_time,
                delay_in_min,
                is_cancelled,
                origin,
                destination,
                internal_id     # 10 <--- НОВОЕ ПОЛЕ (Unique ID)
            ))
            
        except Exception as e:
            print(f"Error parsing stop: {e}")
            continue
    
    return results


def parse_db_xml(xml_data: str, city: str) -> list[tuple]:
    """
    Обёртка для обратной совместимости.
    """
    if 'ct="' in xml_data or 'cs="' in xml_data:
        return parse_fchg_xml(xml_data, city)
    else:
        return parse_plan_xml(xml_data, city)