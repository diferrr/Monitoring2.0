from __future__ import annotations

from typing import List, Dict, Any, Optional

from .repositories.termocom_repo import fetch_termocom_pumps, LCS_NORM
from .repositories.lovati_repo import fetch_lovati_pumps

PUMP_ALARM_CURRENT = 200.0


def _is_digital_01_list(vals: list) -> bool:
    try:
        only = [v for v in vals if v is not None]
        if not only:
            return False
        return all(float(v) in (0.0, 1.0) for v in only)
    except Exception:
        return False


def calc_overall_color(row: Dict[str, Any]) -> str:
    pumps = row.get("pompa") or []
    if not pumps:
        return "gray"

    # LOVATI 0/1: если есть 1 -> красный, иначе зелёный
    if _is_digital_01_list(pumps):
        only = [float(v) for v in pumps if v is not None]
        return "red" if any(v == 1.0 for v in only) else "green"

    # TERMOCOM токи
    lcs = row.get("lcs")
    try:
        lcs_val = float(lcs) if lcs is not None else None
    except Exception:
        lcs_val = None

    vals: List[float] = []
    for v in pumps:
        try:
            vals.append(float(v))
        except Exception:
            vals.append(0.0)

    if lcs_val is not None and lcs_val < LCS_NORM:
        return "yellow"

    if any(v > PUMP_ALARM_CURRENT for v in vals):
        return "red"
    if any(v > 0 for v in vals):
        return "green"
    return "gray"


def _t2_alert(t2: Optional[float], t2_min: Optional[float], t2_max: Optional[float]) -> bool:
    if t2 is None:
        return False
    if t2_min is not None and t2 < t2_min:
        return True
    if t2_max is not None and t2 > t2_max:
        return True
    return False


def get_pumps_rows(t2_min: Optional[float], t2_max: Optional[float]) -> List[Dict[str, Any]]:
    termo = fetch_termocom_pumps()
    lovati = fetch_lovati_pumps()

    # приоритет: TERMOCOM реальные > LOVATI реальные
    # но если TERMOCOM placeholder — заменяем на LOVATI реальные (если есть)
    by_ptc: Dict[str, Dict[str, Any]] = {}

    for r in termo:
        ptc = r.get("ptc")
        if ptc:
            by_ptc[ptc] = r

    for r in lovati:
        ptc = r.get("ptc")
        if not ptc:
            continue

        existing = by_ptc.get(ptc)
        if existing is None:
            by_ptc[ptc] = r
        else:
            if existing.get("is_placeholder") is True and r.get("is_placeholder") is False:
                by_ptc[ptc] = r

    rows = list(by_ptc.values())
    rows.sort(key=lambda x: x.get("ptc") or "")

    out: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        rr["overall_color"] = calc_overall_color(rr)
        rr["t2_alert"] = _t2_alert(rr.get("t2"), t2_min, t2_max)
        out.append(rr)

    return out
