from __future__ import annotations

from django.conf import settings
from django.urls import reverse
from django.urls.exceptions import NoReverseMatch

from typing import Dict, List, Any, Optional
import pyodbc

from .db import connect, dsn_from_dict


DB_CONNECT_TIMEOUT = getattr(settings, "DB_CONNECT_TIMEOUT", 5)

# LCS < 30% = желтый (как в Monitoring PTC)
LCS_NORM = 30.0

POMPA_MAP: Dict[str, List[int]] = {
    "2009": [2],
    "2055": [2, 3],
    "2056": [2],
    "2057": [2],
    "2201": [2],
    "2202": [2, 3],
    "2209": [1],
    "2216": [2],
    "3012": [2],
    "3125": [1, 2, 3],
    "4009": [2],
    "4012": [2],
    "4014": [2],
    "4016": [2],
    "4019": [2],
    "4021": [2],
    "4025": [2],
    "4027": [2],
    "4037": [2],
    "4040": [2],
    "4041": [2],
    "4050": [2],
    "4054": [2],
    "4058": [2],
    "4063": [2],
    "4065": [2],
    "4066": [2],
    "4068": [2],
    "4077": [2],
    "5002": [2],
    "5003": [2],
    "5008": [2],
    "5009": [2],
    "5014": [2],
    "5019": [2],
    "5047": [2],
    "5057": [2],
    "5058": [2],
    "5075": [2],
}

_CHART_PARAM_MAP = {
    # TERMOCOM charts param codes (см. monitoring_PTC/termocom_charts/repositories.py)
    "q": "Q",
    "q1": "Q",
    "t2": "T2",
    "pompa": "POMPA",
    "pompa2": "POMPA2",
    "pompa3": "POMPA3",
}

def _chart_url(ptc: str, param: str | None = None) -> str:
    """
    Ссылка на график TERMOCOM (tc-charts).
    Если param=None — открываем страницу объекта (без параметра, по умолчанию будет T1).
    """
    try:
        base = reverse("termocom_charts:chart_page")
    except NoReverseMatch:
        base = "/tc-charts/chart/"

    q = f"?pti={ptc}"
    if param:
        p = _CHART_PARAM_MAP.get((param or "").lower(), (param or "").upper())
        q += f"&param={p}"
    return f"{base}{q}"


def _to_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _load_lovati_address_map() -> Dict[str, str]:
    """
    Адреса берём из LOVATI.db (таблица PTC_adrese)
    """
    dsn = dsn_from_dict(settings.LOVATI_SERVER)
    with pyodbc.connect(dsn, timeout=DB_CONNECT_TIMEOUT) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                RTRIM(PTC)    AS PTC,
                RTRIM(adresa) AS adresa
            FROM [LOVATI].[dbo].[PTC_adrese]
            WHERE LEN(RTRIM(PTC)) = 4
              AND LEFT(RTRIM(PTC), 1) IN ('1','2','3','4','5')
            """
        )
        return {str(r.PTC).strip(): (r.adresa or "") for r in cur.fetchall()}


def fetch_termocom_pumps() -> List[Dict[str, Any]]:
    """
    TERMOCOM: берём Т2, Q, помпы из TERMOCOM DB.
    Важно: в таблицу попадают ТОЛЬКО объекты из POMPA_MAP.
    """
    address_map = _load_lovati_address_map()

    out: List[Dict[str, Any]] = []

    dsn = dsn_from_dict(settings.SQL_SERVER)
    with pyodbc.connect(dsn, timeout=DB_CONNECT_TIMEOUT) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                u.UNIT_ID,
                u.UNIT_NAME,
                mc.MC_T2_VALUE_INSTANT,
                mc.MC_POWER1_VALUE_INSTANT,
                mc.MC_DTIME_VALUE_INSTANT,
                dcx.DCX_AI01_VALUE,
                dcx.DCX_AI02_VALUE,
                dcx.DCX_AI03_VALUE,
                t3u.UNIT_LCS_VALUE
            FROM UNITS u
            LEFT JOIN MULTICAL_CURRENT_DATA mc ON u.UNIT_ID = mc.UNIT_ID
            LEFT JOIN DCX7600_CURRENT_DATA dcx ON u.UNIT_ID = dcx.UNIT_ID
            LEFT JOIN TERMOCOM3_UNIT t3u ON u.UNIT_ID = t3u.UNIT_ID
            WHERE u.UNIT_ENABLED = 1
              AND u.UNIT_NAME LIKE 'PT_%'
              AND (LEN(REPLACE(RTRIM(u.UNIT_NAME), 'PT_', '')) IN (4, 5))
            ORDER BY mc.MC_DTIME_VALUE_INSTANT DESC
            """
        )

        rows = cur.fetchall()

        for r in rows:
            ptc_full = str(r.UNIT_NAME or "").replace("PT_", "").strip()
            if not ptc_full:
                continue

            # 5019A и т.п. не выводим (как в Monitoring PTC)
            if ptc_full.endswith("A"):
                continue

            ptc = ptc_full[:4]
            if ptc not in POMPA_MAP:
                continue

            nums = POMPA_MAP.get(ptc, [])
            pompa_vals: List[float] = []
            for num in nums:
                if num == 1:
                    pompa_vals.append(_to_float(r.DCX_AI01_VALUE) or 0.0)
                elif num == 2:
                    pompa_vals.append(_to_float(r.DCX_AI02_VALUE) or 0.0)
                elif num == 3:
                    pompa_vals.append(_to_float(r.DCX_AI03_VALUE) or 0.0)

            lcs_raw = _to_float(r.UNIT_LCS_VALUE) or 0.0
            lcs_pct = round(lcs_raw * 100.0, 2)

            out.append({
                "src": "termocom",
                "ptc": ptc,
                "address": address_map.get(ptc, ""),
                "t2": round(_to_float(r.MC_T2_VALUE_INSTANT) or 0.0, 1),
                "q1": round(_to_float(r.MC_POWER1_VALUE_INSTANT) or 0.0, 2),
                "time": r.MC_DTIME_VALUE_INSTANT,
                "lcs": lcs_pct,
                "pompa": pompa_vals,
                "pompa_nums": nums,

                # ✅ ВАЖНО: PTC больше НЕ ведёт на T2, открывает страницу объекта
                "url_ptc": _ptc_view_url(ptc),
                "url_t2": _chart_url(ptc, "t2"),
                # ✅ ВАЖНО: TERMOCOM Q = "Q", не "Q1"
                "url_q1": _chart_url(ptc, "q"),
                "url_pompa": _chart_url(ptc, "pompa"),
                "url_pompa2": _chart_url(ptc, "pompa2"),
                "url_pompa3": _chart_url(ptc, "pompa3"),
            })

    out.sort(key=lambda x: x.get("ptc") or "")
    return out

def _ptc_view_url(ptc: str) -> str:
    tpl = getattr(settings, "PTC_VIEW_URL_TEMPLATE", "")
    return tpl.format(ptc=ptc) if tpl else ""
