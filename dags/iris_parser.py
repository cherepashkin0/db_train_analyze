# iris_parser.py

import xml.etree.ElementTree as ET
from datetime import datetime
from zoneinfo import ZoneInfo 

def parse_db_xml(xml_string, city_name):
    if not xml_string:
        return []
    
    try:
        root = ET.fromstring(xml_string)
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return []

    # Опредяляем часовые пояса
    berlin_tz = ZoneInfo("Europe/Berlin")
    utc_tz = ZoneInfo("UTC")

    records = []
    
    for s in root.findall(".//s"):
        raw_id = s.attrib.get('id', 'unknown')
        tl = s.find("tl")
        train_type = tl.attrib.get('c', 'Train') if tl is not None else "Train"
        train_number = tl.attrib.get('n', '') if tl is not None else ""
        
        full_train_id = f"{train_type} {train_number}".strip() or raw_id

        ar = s.find("ar")
        dp = s.find("dp")
        
        # --- Маршрут ---
        origin = city_name
        destination = city_name

        if ar is not None:
            ppth_ar = ar.attrib.get('ppth', '')
            if ppth_ar:
                origin = ppth_ar.split('|')[0]

        if dp is not None:
            ppth_dp = dp.attrib.get('ppth', '')
            if ppth_dp:
                destination = ppth_dp.split('|')[-1]
        # ----------------

        target_node = dp if dp is not None else ar
        is_cancelled = 0
        
        if target_node is not None:
            if target_node.attrib.get('cs') == 'c' or target_node.attrib.get('cp') == 'c':
                is_cancelled = 1
            if s.attrib.get('bs') == 'c':
                 is_cancelled = 1

            pt_str = target_node.attrib.get('pt')
            ct_str = target_node.attrib.get('ct')
            
            if not ct_str:
                ct_str = pt_str

            if pt_str and ct_str:
                try:
                    fmt = "%y%m%d%H%M"
                    # 1. Парсим "наивную" дату (как и раньше)
                    dt_planned_naive = datetime.strptime(pt_str, fmt)
                    dt_actual_naive = datetime.strptime(ct_str, fmt)
                    
                    # 2. Присваиваем ей Берлинский часовой пояс
                    dt_planned_berlin = dt_planned_naive.replace(tzinfo=berlin_tz)
                    dt_actual_berlin = dt_actual_naive.replace(tzinfo=berlin_tz)
                    
                    # 3. Конвертируем в UTC (так принято хранить в базах данных) 
                    # ClickHouse Connect сам разберется, если передать объект с timezone, 
                    # но явная конвертация надежнее.
                    dt_planned_utc = dt_planned_berlin.astimezone(utc_tz)
                    dt_actual_utc = dt_actual_berlin.astimezone(utc_tz)
                    
                    # Рассчитываем задержку
                    delay = int((dt_actual_berlin - dt_planned_berlin).total_seconds() / 60)
                    
                    if is_cancelled:
                        delay = 0

                    records.append([
                        datetime.now(utc_tz), # Текущее время тоже в UTC
                        city_name,
                        train_type,
                        full_train_id,
                        dt_planned_utc,
                        dt_actual_utc, 
                        max(0, delay),
                        is_cancelled,
                        origin,
                        destination
                    ])
                except (ValueError, TypeError) as e:
                    continue
    return records