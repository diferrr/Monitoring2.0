from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import hashlib
import lxml.etree
import pyodbc
import requests
from django.core.cache import cache

from .geo_store import get_points, get_index_by_param, split_pumps_boilers



logger = logging.getLogger(__name__)

# Один пул потоков на процесс (не создаём заново на каждый запрос!)
SCADA_EXECUTOR = ThreadPoolExecutor(max_workers=20)

# Один Session = keep-alive
SCADA_SESSION = requests.Session()

ALL_TEMPS_CACHE_KEY = "temps:all:v3"  # bumped (чтобы не ловить старый кэш)
ALL_TEMPS_CACHE_TTL = 30  # секунд

SQLSERVER_CACHE_KEY = "temps:sqlserver:v1"
SQLSERVER_CACHE_TTL = 30

ASUTP_CET2_CACHE_TTL = 10  # секунд




SQLSERVER_CONFIG = {
    "driver": "{SQL Server}",  # если есть проблемы с драйвером — можно заменить на ODBC Driver 17/18
    "server": "10.1.1.124",
    "database": "TERMOCOM5",
    "uid": "disp",
    "pwd": "disp123",
}

# ✅ ASUTP CET2 (datasource_id == "sql_asutp_cet2")
ASUTP_CET2_CONFIG = {
    "driver": "{ODBC Driver 17 for SQL Server}",
    "server": "192.168.0.26",
    "database": "asutp_cet2",
    "uid": "sursa1",
    "pwd": "sursa1",
}


# ============ CONNECTORS ============
def connect_sql_server():
    conn_str = (
        f"DRIVER={SQLSERVER_CONFIG['driver']};"
        f"SERVER={SQLSERVER_CONFIG['server']};"
        f"DATABASE={SQLSERVER_CONFIG['database']};"
        f"UID={SQLSERVER_CONFIG['uid']};"
        f"PWD={SQLSERVER_CONFIG['pwd']}"
    )
    return pyodbc.connect(conn_str)


def connect_asutp_cet2():
    conn_str = (
        f"DRIVER={ASUTP_CET2_CONFIG['driver']};"
        f"SERVER={ASUTP_CET2_CONFIG['server']};"
        f"DATABASE={ASUTP_CET2_CONFIG['database']};"
        f"UID={ASUTP_CET2_CONFIG['uid']};"
        f"PWD={ASUTP_CET2_CONFIG['pwd']}"
    )
    return pyodbc.connect(conn_str)


# ============ HELPERS ============
def get_scada_value(ip: str, param_id: str):
    """
    Берём значение параметра param_id с устройства ip через SCADA XML.
    Возвращает float (rounded) либо None.
    """
    try:
        url = f"http://{ip}/cgi-bin/xml/getcv.pl?params={param_id}"
        xml = SCADA_SESSION.get(url, timeout=5).content
        root = lxml.etree.fromstring(xml)
        for val in root.findall("value"):
            if val.text is None:
                return None
            val_text = val.text.strip()
            if val_text.lower() == "undefined":
                return None
            return round(float(val_text), 2)
    except Exception as e:
        logger.warning("[SCADA ERROR] %s param=%s → %s", ip, param_id, e)
        return None


def get_sqlserver_temperatures():
    """
    Fallback для объектов datasource_id == 'sql' (или '1').
    Кэшируется.
    """
    cached = cache.get(SQLSERVER_CACHE_KEY)
    if cached is not None:
        return cached

    data = {}
    conn = None
    try:
        conn = connect_sql_server()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                u.UNIT_NAME as name,
                ROUND(mc.MC_T1_VALUE_INSTANT, 2) as T1,
                ROUND(mc.MC_T2_VALUE_INSTANT, 2) as T2
            FROM UNITS u
            LEFT JOIN MULTICAL_CURRENT_DATA mc ON u.UNIT_ID = mc.UNIT_ID
            """
        )
        for row in cursor.fetchall():
            name = str(row[0]).replace("PT_", "").lower().strip()
            data[name] = {"T1": row[1], "T2": row[2]}
    except Exception as e:
        logger.exception("[SQLSERVER ERROR] %s", e)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    cache.set(SQLSERVER_CACHE_KEY, data, SQLSERVER_CACHE_TTL)
    return data


def get_asutp_cet2_signals(signal_ids: list[int]) -> dict[int, float | None]:
    """
    ASUTP CET2:
    Таблица: Asu_Show_Real
    idField: Signal_ID
    valueField: R_V_Value
    timeField: RTime

    Возвращаем последние значения по каждому Signal_ID.
    """
    ids = [int(x) for x in signal_ids if x is not None]
    if not ids:
        return {}

    cache_key = "asutp_cet2:signals:" + hashlib.md5(
        ",".join(map(str, sorted(ids))).encode("utf-8")
    ).hexdigest()

    out: dict[int, float | None] = {i: None for i in ids}
    placeholders = ",".join(["?"] * len(ids))

    # Последняя запись по времени на каждый Signal_ID
    sql = f"""
        SELECT t.Signal_ID, t.R_V_Value
        FROM Asu_Show_Real t
        INNER JOIN (
            SELECT Signal_ID, MAX(RTime) AS max_time
            FROM Asu_Show_Real
            WHERE Signal_ID IN ({placeholders})
            GROUP BY Signal_ID
        ) m ON t.Signal_ID = m.Signal_ID AND t.RTime = m.max_time
    """

    conn = None
    try:
        conn = connect_asutp_cet2()
        cur = conn.cursor()
        cur.execute(sql, ids)
        rows = cur.fetchall()

        for row in rows:
            sid = int(row[0])
            val = row[1]
            try:
                out[sid] = round(float(val), 2) if val is not None else None
            except Exception:
                out[sid] = None

    except Exception as e:
        logger.warning("[ASUTP_CET2 ERROR] %s", e)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    cache.set(cache_key, out, ASUTP_CET2_CACHE_TTL)
    return out


def _normalize_param_name(param_name: str) -> str:
    return (param_name or "").lower().replace("pt_", "").strip()



def get_live_temperature(param_name: str):
    """
    Возвращает (T1, T2) для любой точки (насос или котельная),
    используя GeoJSON как справочник.
    """
    name = _normalize_param_name(param_name)

    by_param = get_index_by_param()
    p = by_param.get(name)

    if not p:
        return None, None


    ds_raw = p.datasource_id
    ds = str(ds_raw).strip().lower()

    # ✅ ASUTP CET2 (отдельная база)
    if ds == "sql_asutp_cet2":
        try:
            id1 = int(p.props.get("T1")) if p.props.get("T1") else None
            id2 = int(p.props.get("T2")) if p.props.get("T2") else None
            ids = [i for i in (id1, id2) if i is not None]
            vals = get_asutp_cet2_signals(ids)
            return vals.get(id1), vals.get(id2)
        except Exception:
            return None, None

    # ✅ Fallback SQL Server (и на будущее — любые "sql_*")
    if ds in ("sql", "1") or ds.startswith("sql"):
        sql_fallback = get_sqlserver_temperatures()
        temps = sql_fallback.get(name, {})
        return temps.get("T1"), temps.get("T2")

    # ✅ SCADA по IP
    ip = str(ds_raw).strip()
    param_T1 = p.props.get("T1")
    param_T2 = p.props.get("T2")

    results = {"T1": None, "T2": None}

    def fetch(label: str, param: str):
        results[label] = get_scada_value(ip, param) if ip and param else None

    f1 = SCADA_EXECUTOR.submit(fetch, "T1", param_T1)
    f2 = SCADA_EXECUTOR.submit(fetch, "T2", param_T2)

    try:
        f1.result(timeout=6)
    except Exception:
        pass
    try:
        f2.result(timeout=6)
    except Exception:
        pass

    return results.get("T1"), results.get("T2")


def get_live_temperature_boiler(param_name: str):
    """
    Раньше это читало map_markers_cazan из MySQL.
    Теперь GeoJSON уже содержит эти точки — поэтому просто вызываем общий метод.
    """
    return get_live_temperature(param_name)


def get_boiler_onoff(param_name: str):
    """
    Возвращает 1/0/None для котельной/станции.
    Используем GeoJSON свойства (чаще всего PompaSI / PompaACM).
    """
    name = _normalize_param_name(param_name)

    by_param = get_index_by_param()
    p = by_param.get(name)

    if not p:
        return None

    ds_raw = p.datasource_id
    ds = str(ds_raw).strip().lower()

    # SQL точки — обычно нет on/off параметра
    if ds in ("sql", "1") or ds.startswith("sql"):
        return None

    ip = str(ds_raw).strip()

    onoff_param = (
        p.props.get("PompaSI")
        or p.props.get("PompaACM")
        or p.props.get("ON_OFF")
        or p.props.get("onoff")
    )

    if not ip or not onoff_param:
        return None

    v = get_scada_value(ip, onoff_param)
    if v is None:
        return None

    try:
        return 1 if float(v) >= 0.5 else 0
    except Exception:
        return None


def get_all_temperatures():
    """
    Bulk температуры для всех насосов (type_device 1/2).
    GeoJSON -> список точек -> параллельный SCADA опрос + SQL fallback.
    """
    cached = cache.get(ALL_TEMPS_CACHE_KEY)
    if cached is not None:
        return cached

    sql_fallback = get_sqlserver_temperatures()

    points = get_points()
    pumps, _ = split_pumps_boilers(points)

    results = []
    futures = []

    # 1) SQL объекты — быстро из одного словаря
    for p in pumps:
        name = _normalize_param_name(p.param_name)
        ds = str(p.datasource_id).strip().lower()
        if ds in ("sql", "1") or ds.startswith("sql"):
            temps = sql_fallback.get(name, {})
            results.append(
                {"name": name, "ip_id": 1, "T1": temps.get("T1"), "T2": temps.get("T2")}
            )

    # 2) SCADA объекты — параллельно
    def fetch_one(p_):
        name_ = _normalize_param_name(p_.param_name)
        ip_ = str(p_.datasource_id).strip()
        t1_id = p_.props.get("T1")
        t2_id = p_.props.get("T2")
        t1 = get_scada_value(ip_, t1_id) if ip_ and t1_id else None
        t2 = get_scada_value(ip_, t2_id) if ip_ and t2_id else None
        return {"name": name_, "ip_id": ip_, "T1": t1, "T2": t2}

    for p in pumps:
        ds = str(p.datasource_id).strip().lower()
        if ds not in ("sql", "1") and not ds.startswith("sql"):
            futures.append(SCADA_EXECUTOR.submit(fetch_one, p))

    for f in as_completed(futures):
        try:
            results.append(f.result())
        except Exception:
            continue

    cache.set(ALL_TEMPS_CACHE_KEY, results, ALL_TEMPS_CACHE_TTL)
    return results
