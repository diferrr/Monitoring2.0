from __future__ import annotations

from django.urls import reverse
from django.urls.exceptions import NoReverseMatch

from typing import Optional, Dict, Any, List
import pyodbc

from django.conf import settings
from .db import connect


DB_CONNECT_TIMEOUT = getattr(settings, "DB_CONNECT_TIMEOUT", 5)

LOVATI_PUMP01_PTC = {
    "1012","1018","2113","2209","2407",
    "3001","3002","3003","3004","3005","3013","3025","3038","3043","3054","3064","3083","3107","3111","3118","3127",
    "4018","4044","5012","5013","5023","5051","5043",
}

_CHART_PARAM_MAP = {
    # LOVATI charts param codes (см. monitoring_PTC/charts/repositories.py)
    "q": "Q1",
    "q1": "Q1",
    "t2": "T2",
    "pompa": "POMPA",
    "pompa2": "POMPA2",
    "pompa3": "POMPA3",
}

def _chart_url(ptc: str, param: str | None = None) -> str:
    """
    Ссылка на график LOVATI (charts).
    Если param=None — открываем страницу объекта (без параметра, по умолчанию будет T1).
    """
    try:
        base = reverse("charts:chart_page")
    except NoReverseMatch:
        base = "/charts/chart/"

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

def _to_01(v) -> Optional[int]:
    try:
        if v is None:
            return None
        iv = int(float(v))
        return iv if iv in (0, 1) else None
    except Exception:
        return None


def _safe_fetch_ids_flags(conn) -> Dict[int, Dict[str, bool]]:
    """
    Пытаемся определить, есть ли pompa2/pompa3 в IDS.
    Если таблица/колонки отличаются — просто вернём пусто (и не покажем p2/p3).
    """
    cur = conn.cursor()

    # пробуем безопасно (если IDS другая — ловим исключение)
    try:
        cur.execute("""
            SELECT
                CAST(pid AS int)       AS pid,
                RTRIM(param)           AS param,
                CAST(id_lovati AS int) AS id_lovati
            FROM IDS
            WHERE id_lovati IS NOT NULL
        """)
    except Exception:
        return {}

    out: Dict[int, Dict[str, bool]] = {}
    for r in cur.fetchall():
        pid = int(r.pid)
        param = (r.param or "").strip().lower()
        if pid not in out:
            out[pid] = {"pompa2": False, "pompa3": False}
        if param in ("pompa2", "pompa_2", "pump2"):
            out[pid]["pompa2"] = True
        if param in ("pompa3", "pompa_3", "pump3"):
            out[pid]["pompa3"] = True
    return out


def fetch_lovati_pumps() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    with connect(settings.LOVATI_SERVER) as conn:
        ids_flags_by_pid = _safe_fetch_ids_flags(conn)

        cur = conn.cursor()
        placeholders = ",".join(["?"] * len(LOVATI_PUMP01_PTC))

        cur.execute(f"""
            SELECT
                p.id                  AS PID,
                RTRIM(p.pti)          AS PTC,
                p.dt1                 AS dt1,
                RTRIM(p.q1)           AS q1,
                RTRIM(p.t2)           AS t2,
                RTRIM(p.pompa)        AS pompa1,
                RTRIM(p.pompa2)       AS pompa2,
                RTRIM(p.pompa3)       AS pompa3
            FROM PTI p
            WHERE p.typeObj = 0
              AND LEN(RTRIM(p.pti)) = 4
              AND RTRIM(p.pti) IN ({placeholders})
            ORDER BY p.pti
        """, *sorted(LOVATI_PUMP01_PTC))

        for r in cur.fetchall():
            ptc = str(r.PTC).strip()
            pid = int(r.PID) if r.PID is not None else 0

            flags = ids_flags_by_pid.get(pid, {"pompa2": False, "pompa3": False})

            p1 = _to_01(r.pompa1)
            p2 = _to_01(r.pompa2) if flags.get("pompa2") else None
            p3 = _to_01(r.pompa3) if flags.get("pompa3") else None

            pumps: List[int] = []
            nums: List[int] = []

            if p1 is not None:
                pumps.append(p1); nums.append(1)
            if p2 is not None:
                pumps.append(p2); nums.append(2)
            if p3 is not None:
                pumps.append(p3); nums.append(3)

            if not pumps:
                continue

            out.append({
                "src": "lovati",
                "ptc": ptc,
                "t2": round(_to_float(r.t2) or 0.0, 1),
                "q1": round(_to_float(r.q1) or 0.0, 2),
                "time": r.dt1,
                "pompa": pumps,
                "pompa_nums": nums,
                "lcs": None,

                # ✅ ВАЖНО: PTC открывает страницу объекта без param
                "url_ptc": _ptc_view_url(ptc),
                "url_t2": _chart_url(ptc, "t2"),
                "url_q1": _chart_url(ptc, "q1"),
                "url_pompa": _chart_url(ptc, "pompa"),
                "url_pompa2": _chart_url(ptc, "pompa2"),
                "url_pompa3": _chart_url(ptc, "pompa3"),
            })

    out.sort(key=lambda x: x.get("ptc") or "")
    return out

def _ptc_view_url(ptc: str) -> str:
    tpl = getattr(settings, "PTC_VIEW_URL_TEMPLATE", "")
    return tpl.format(ptc=ptc) if tpl else ""
