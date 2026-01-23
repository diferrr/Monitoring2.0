
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

import lxml.etree
import pymysql
import pyodbc
import requests
from django.core.cache import cache
from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)

# Один пул потоков на процесс (не создаём заново на каждый запрос!)
SCADA_EXECUTOR = ThreadPoolExecutor(max_workers=20)

# Один Session = keep-alive
SCADA_SESSION = requests.Session()

ALL_TEMPS_CACHE_KEY = "temps:all:v1"
ALL_TEMPS_CACHE_TTL = 30  # секунд

SQLSERVER_CACHE_KEY = "temps:sqlserver:v1"
SQLSERVER_CACHE_TTL = 30


# ============ КОНФИГ ============
MYSQL_CONFIG = {
    'host': '10.1.1.174',
    'user': 'victor',
    'password': 'VictorSurs@2',
    'database': 'access_user',
    'charset': 'utf8mb4',
    'cursorclass': DictCursor
}

SQLSERVER_CONFIG = {
    'driver': '{SQL Server}',
    'server': '10.1.1.124',
    'database': 'TERMOCOM5',
    'uid': 'disp',
    'pwd': 'disp123'
}

IPS = {
    1: "sql",
    2: "10.1.1.214",
    3: "10.1.1.173",
    4: "10.1.1.242",
    5: "10.2.1.153",
    6: "10.2.1.154",
    7: "10.3.1.139"
}

# ============ CONNECTORS ============
def connect_mysql():
    return pymysql.connect(**MYSQL_CONFIG)

def connect_sql_server():
    conn_str = f"DRIVER={SQLSERVER_CONFIG['driver']};"                f"SERVER={SQLSERVER_CONFIG['server']};"                f"DATABASE={SQLSERVER_CONFIG['database']};"                f"UID={SQLSERVER_CONFIG['uid']};PWD={SQLSERVER_CONFIG['pwd']}"
    return pyodbc.connect(conn_str)

# ============ HELPERS ============
def get_scada_value(ip, param_id):
    try:
        url = f"http://{ip}/cgi-bin/xml/getcv.pl?params={param_id}"
        xml = SCADA_SESSION.get(url, timeout=5).content
        root = lxml.etree.fromstring(xml)
        for val in root.findall("value"):
            val_text = val.text.strip()
            if val_text.lower() == "undefined":
                return None
            return round(float(val_text), 2)
    except Exception as e:
        logger.warning("[SCADA ERROR] %s param=%s → %s", ip, param_id, e)
        return None



def get_sqlserver_temperatures():
    cached = cache.get(SQLSERVER_CACHE_KEY)
    if cached is not None:
        return cached

    data = {}
    conn = connect_sql_server()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            u.UNIT_NAME as name, 
            ROUND(mc.MC_T1_VALUE_INSTANT, 2) as T1, 
            ROUND(mc.MC_T2_VALUE_INSTANT, 2) as T2
        FROM UNITS u
        LEFT JOIN MULTICAL_CURRENT_DATA mc ON u.UNIT_ID = mc.UNIT_ID
    """)
    for row in cursor.fetchall():
        name = row[0].replace("PT_", "").lower()
        data[name] = {"T1": row[1], "T2": row[2]}
    conn.close()

    cache.set(SQLSERVER_CACHE_KEY, data, SQLSERVER_CACHE_TTL)
    return data


# ============ ПОЛУЧЕНИЕ ОДНОГО ОБЪЕКТА ============
def get_live_temperature(param_name):
    name = param_name.lower().replace("pt_", "")
    sql_fallback = get_sqlserver_temperatures()

    with connect_mysql() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    REPLACE(m.param_name, 'PT_', '') as name,
                    m.id_T1, m.id_T2,
                    m.datasource_id as ips,
                    d.address as ip
                FROM map_markers m
                JOIN datasources_http d ON m.datasource_id = d.id
                WHERE LOWER(REPLACE(m.param_name, 'PT_', '')) = %s
            """, (name,))
            row = cursor.fetchone()

    if not row:
        print(f"[DB] Объект '{param_name}' не найден.")
        return None, None

    ip_id = row['ips']
    param_T1 = row['id_T1']
    param_T2 = row['id_T2']
    ip = IPS.get(ip_id)

    if ip_id == 1:
        temps = sql_fallback.get(name, {})
        return temps.get('T1'), temps.get('T2')

    results = {}
    def fetch_value(label, param):
        if ip and param:
            results[label] = get_scada_value(ip, param)
        else:
            results[label] = None

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(fetch_value, 'T1', param_T1)
        executor.submit(fetch_value, 'T2', param_T2)

    return results.get('T1'), results.get('T2')

def get_live_temperature_boiler(param_name):
    # Для объектов из map_markers_cazan (котельные)
    name = param_name.lower().replace("pt_", "")
    sql_fallback = get_sqlserver_temperatures()

    with connect_mysql() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    REPLACE(m.param_name, 'PT_', '') as name,
                    m.id_T1, m.id_T2,
                    m.datasource_id as ips,
                    d.address as ip
                FROM map_markers_cazan m
                JOIN datasources_http d ON m.datasource_id = d.id
                WHERE LOWER(REPLACE(m.param_name, 'PT_', '')) = %s
            """, (name,))
            row = cursor.fetchone()

    if not row:
        print(f"[DB] Объект '{param_name}' не найден в map_markers_cazan.")
        return None, None

    ip_id = row['ips']
    param_T1 = row['id_T1']
    param_T2 = row['id_T2']
    ip = IPS.get(ip_id)

    if ip_id == 1:
        temps = sql_fallback.get(name, {})
        return temps.get('T1'), temps.get('T2')

    results = {}
    def fetch_value(label, param):
        if ip and param:
            results[label] = get_scada_value(ip, param)
        else:
            results[label] = None

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(fetch_value, 'T1', param_T1)
        executor.submit(fetch_value, 'T2', param_T2)

    return results.get('T1'), results.get('T2')




# ============ ВСЕ ОБЪЕКТЫ ============
def fetch_temperature(row, sql_fallback):
    name = row['name'].lower()
    ip_id = row['ips']
    param_T1 = row['id_T1']
    param_T2 = row['id_T2']

    t1 = t2 = None

    if ip_id == 1:
        temps = sql_fallback.get(name, {})
        t1 = temps.get('T1')
        t2 = temps.get('T2')
    else:
        ip = IPS.get(ip_id)
        if ip and param_T1:
            t1 = get_scada_value(ip, param_T1)
        if ip and param_T2:
            t2 = get_scada_value(ip, param_T2)

    return {
        'name': name,
        'ip_id': ip_id,
        'T1': t1,
        'T2': t2
    }

def get_all_temperatures():
    # 1) Сначала отдаём из кэша
    cached = cache.get(ALL_TEMPS_CACHE_KEY)
    if cached is not None:
        return cached

    sql_fallback = get_sqlserver_temperatures()

    with connect_mysql() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    REPLACE(m.param_name, 'PT_', '') as name,
                    m.id_T1, m.id_T2,
                    m.datasource_id as ips,
                    d.address as ip
                FROM map_markers m
                JOIN datasources_http d ON m.datasource_id = d.id
            """)
            rows = cursor.fetchall()

    if not rows:
        logger.warning("❌ В MySQL не получены строки для температур.")
        return []

    results = []
    futures = [SCADA_EXECUTOR.submit(fetch_temperature, row, sql_fallback) for row in rows]

    for future in as_completed(futures):
        try:
            result = future.result()
            if result:
                results.append(result)
        except Exception as e:
            logger.warning("[FUTURE ERROR] %s", e)

    # 2) Кладём в кэш
    cache.set(ALL_TEMPS_CACHE_KEY, results, ALL_TEMPS_CACHE_TTL)
    return results

