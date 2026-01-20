from __future__ import annotations

from typing import List, Tuple
from datetime import datetime

import pyodbc
from django.conf import settings


# --------- маппинг параметров ---------
# Параметр: (таблица, колонка значения, колонка времени)
TERMOCOM_PARAM_MAP = {
    'G1':     ('MULTICAL_CURRENT_DATA_HTREND', 'MC_G1_VALUE',     'MC_G1_TIMESTAMP'),
    'G2':     ('MULTICAL_CURRENT_DATA_HTREND', 'MC_G2_VALUE',     'MC_G2_TIMESTAMP'),
    'T1':     ('MULTICAL_CURRENT_DATA_HTREND', 'MC_T1_VALUE',     'MC_T1_TIMESTAMP'),
    'T2':     ('MULTICAL_CURRENT_DATA_HTREND', 'MC_T2_VALUE',     'MC_T2_TIMESTAMP'),
    'DT':     ('MULTICAL_CURRENT_DATA_HTREND', 'MC_DT_VALUE',     'MC_DT_TIMESTAMP'),
    'DG':     ('MULTICAL_CURRENT_DATA_HTREND', 'MC_DG_VALUE',     'MC_DG_TIMESTAMP'),
    'Q':      ('MULTICAL_CURRENT_DATA_HTREND', 'MC_POWER1_VALUE', 'MC_POWER1_TIMESTAMP'),
    'T31':    ('DCX7600_CURRENT_DATA_HTREND',  'DCX_TR01_VALUE',  'DCX_TR01_TIMESTAMP'),
    'T32':    ('DCX7600_CURRENT_DATA_HTREND',  'DCX_TR02_VALUE',  'DCX_TR02_TIMESTAMP'),
    'T41':    ('DCX7600_CURRENT_DATA_HTREND',  'DCX_TR07_VALUE',  'DCX_TR07_TIMESTAMP'),
    'T42':    ('DCX7600_CURRENT_DATA_HTREND',  'DCX_TR05_VALUE',  'DCX_TR05_TIMESTAMP'),
    'T43':    ('DCX7600_CURRENT_DATA_HTREND',  'DCX_TR04_VALUE',  'DCX_TR04_TIMESTAMP'),
    'T44':    ('DCX7600_CURRENT_DATA_HTREND',  'DCX_TR02_VALUE',  'DCX_TR02_TIMESTAMP'),
    'TACM':   ('DCX7600_CURRENT_DATA_HTREND',  'DCX_TR03_VALUE',  'DCX_TR03_TIMESTAMP'),
    'GACM':   ('MULTICAL_CURRENT_DATA_HTREND', 'MC_CINAVH_VALUE', 'MC_CINAVH_TIMESTAMP'),
    'GADAOS': ('MULTICAL_CURRENT_DATA_HTREND', 'MC_INB_VALUE',    'MC_INB_TIMESTAMP'),
    'SURSA':  ('DCX7600_CURRENT_DATA_HTREND',  'DCX_AI08_VALUE',  'DCX_AI08_TIMESTAMP'),
    'POMPA':  ('DCX7600_CURRENT_DATA_HTREND',  'DCX_AI01_VALUE',  'DCX_AI01_TIMESTAMP'),
    'POMPA2': ('DCX7600_CURRENT_DATA_HTREND',  'DCX_AI02_VALUE',  'DCX_AI02_TIMESTAMP'),
    'POMPA3': ('DCX7600_CURRENT_DATA_HTREND',  'DCX_AI03_VALUE',  'DCX_AI03_TIMESTAMP'),
}



def _dsn(cfg: dict) -> str:
    return ";".join(f"{k}={v}" for k, v in cfg.items())


def resolve_unit_id_by_ptc(pti: str | int) -> int | None:
    ptc_str = str(pti).strip()

    dsn = _dsn(settings.SQL_SERVER)
    with pyodbc.connect(dsn) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP (1) UNIT_ID, UNIT_NAME
            FROM UNITS
            WHERE UNIT_ENABLED = 1
              AND UNIT_NAME LIKE 'PT_' + ? + '%'
            ORDER BY UNIT_NAME
            """,
            ptc_str,
        )
        row = cur.fetchone()
        return int(row.UNIT_ID) if row and row.UNIT_ID is not None else None


def fetch_termocom_series(
    unit_id: int,
    param_code: str,
    start_dt: datetime,
    end_dt: datetime,
) -> List[Tuple[datetime, float]]:
    param_code = param_code.upper().strip()

    if param_code not in TERMOCOM_PARAM_MAP:
        return []

    table_name, value_col, ts_col = TERMOCOM_PARAM_MAP[param_code]

    sql = f"""
        SELECT {ts_col}, {value_col}
        FROM {table_name}
        WHERE UNIT_ID = ?
          AND {ts_col} BETWEEN ? AND ?
        ORDER BY {ts_col}
    """

    dsn = _dsn(settings.SQL_SERVER)
    out: List[Tuple[datetime, float]] = []

    with pyodbc.connect(dsn) as conn:
        cur = conn.cursor()
        cur.execute(sql, unit_id, start_dt, end_dt)

        for row in cur.fetchall():
            ts = getattr(row, ts_col, None)
            val = getattr(row, value_col, None)
            if ts is None or val is None:
                continue
            out.append((ts, float(val)))

    return out


def list_objects_tc() -> List[dict]:
    """
    Вернёт список объектов TERMOCOM5 для выпадающего списка.
    Формат: [{"pti": "5118", "adres": "...."}, ...]
    Адрес берём так же, как в Monitoring PTC: из LOVATI.dbo.PTC_adrese.
    """
    # 1) Собираем словарь адресов по PTC из LOVATI
    address_map: dict[str, str] = {}

    try:
        dsn_lovati = _dsn(settings.LOVATI_SERVER)
        with pyodbc.connect(dsn_lovati) as conn_l:
            cur_l = conn_l.cursor()
            cur_l.execute("""
                SELECT
                    RTRIM(PTC)    AS PTC,
                    RTRIM(adresa) AS adresa
                FROM [LOVATI].[dbo].[PTC_adrese]
                WHERE LEN(RTRIM(PTC)) = 4
                  AND LEFT(RTRIM(PTC), 1) IN ('1','2','3','4','5')
            """)
            address_map = {
                str(r.PTC).strip(): str(r.adresa or '').strip()
                for r in cur_l.fetchall()
            }
    except Exception:
        # Если по какой-то причине LOVATI недоступна — не падаем,
        # просто вернём объекты без адреса, как было раньше.
        address_map = {}

    # 2) Читаем объекты из TERMOCOM5.UNITS
    dsn_termo = _dsn(settings.SQL_SERVER)

    sql_units = """
        SELECT UNIT_ID, UNIT_NAME
        FROM UNITS
        WHERE UNIT_ENABLED = 1
          AND UNIT_NAME LIKE 'PT_%'
        ORDER BY UNIT_NAME
    """

    result: List[dict] = []
    with pyodbc.connect(dsn_termo) as conn_t:
        cur_t = conn_t.cursor()
        for row in cur_t.execute(sql_units):
            name = (row.UNIT_NAME or '').strip()
            if not name.startswith('PT_'):
                continue

            # "PT_5118" -> "5118" (как в monitoring/views.py)
            pti_raw = name[3:]
            pti = pti_raw.split('/')[0].strip()

            adres = address_map.get(pti, "")

            result.append({
                "pti": pti,
                "adres": adres,
            })

    return result
