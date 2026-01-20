# МОДУЛЬ: доступ к БД LOVATI для списка объектов и сопоставления параметров (IDs из таблицы IDS).
# Содержит:
#   - list_objects(...) — получить объекты с валидными id_* (можно по typeObj=0,1 или обоим)
#   - get_object_by_pti(...) — получить один объект по коду pti (без фильтра по typeObj)
#   - get_ips_and_param(...) — по (pti, param) вернуть {ips, param_id}
#   - PARAM_COLUMNS — карта "имя параметра в API" -> "колонка в IDS"

from __future__ import annotations  # поддержка аннотаций типов в ранних версиях Python  # не влияет на рантайм
from typing import List, Dict, Any, Iterable
from .utils.db import fetchall  # обёртка для выполнения SQL и получения списка dict


def _to_int_or_none(x):
    """Пытается привести x к int; при ошибке возвращает None (удобно для p.IPs)."""
    try:
        return int(str(x).strip())
    except (TypeError, ValueError):
        return None


def _where_typeobj(types: Iterable[int]) -> tuple[str, tuple]:
    """Собирает фрагмент WHERE для p.typeObj с плейсхолдерами %s и кортеж параметров."""
    t = [int(v) for v in types]
    if len(t) == 1:
        return "p.typeObj = %s", (t[0],)
    ph = ",".join(["%s"] * len(t))
    return f"p.typeObj IN ({ph})", tuple(t)


def list_objects(type_obj: int | list[int] = 0) -> List[Dict[str, Any]]:
    """
    Возвращает список объектов (pti, adres, ips, id_t1, id_t2) из LOVATI,
    у которых есть ХОТЯ БЫ ОДИН валидный id_* в IDS (не пусто, не '0', содержит букву).

    По умолчанию поведение прежнее: type_obj=0.
    Для объединённого списка используйте [0, 1].
    """
    if isinstance(type_obj, (list, tuple, set)):
        where_t, params_t = _where_typeobj(type_obj)
    else:
        where_t, params_t = _where_typeobj([type_obj])

    # адрес берём из unicode-поля
    sql = f"""
    SELECT
        p.pti,
        p.adres_unicode AS adres,
        p.IPs   AS ips,
        i.T1    AS id_t1,
        i.T2    AS id_t2
    FROM [LOVATI].[dbo].[PTI] AS p
    LEFT JOIN [LOVATI].[dbo].[IDS] AS i
           ON p.id = i.PTI
    WHERE {where_t}
      AND (
            (i.T1    IS NOT NULL AND LTRIM(RTRIM(i.T1    )) <> '' AND LTRIM(RTRIM(i.T1    )) <> '0' AND PATINDEX('%[A-Za-z]%', i.T1    ) > 0) OR
            (i.T2    IS NOT NULL AND LTRIM(RTRIM(i.T2    )) <> '' AND LTRIM(RTRIM(i.T2    )) <> '0' AND PATINDEX('%[A-Za-z]%', i.T2    ) > 0) OR
            (i.T31   IS NOT NULL AND LTRIM(RTRIM(i.T31   )) <> '' AND LTRIM(RTRIM(i.T31   )) <> '0' AND PATINDEX('%[A-Za-z]%', i.T31   ) > 0) OR
            (i.T32   IS NOT NULL AND LTRIM(RTRIM(i.T32   )) <> '' AND LTRIM(RTRIM(i.T32   )) <> '0' AND PATINDEX('%[A-Za-z]%', i.T32   ) > 0) OR
            (i.T41   IS NOT NULL AND LTRIM(RTRIM(i.T41   )) <> '' AND LTRIM(RTRIM(i.T41   )) <> '0' AND PATINDEX('%[A-Za-z]%', i.T41   ) > 0) OR
            (i.T42   IS NOT NULL AND LTRIM(RTRIM(i.T42   )) <> '' AND LTRIM(RTRIM(i.T42   )) <> '0' AND PATINDEX('%[A-Za-z]%', i.T42   ) > 0) OR
            (i.T43   IS NOT NULL AND LTRIM(RTRIM(i.T43   )) <> '' AND LTRIM(RTRIM(i.T43   )) <> '0' AND PATINDEX('%[A-Za-z]%', i.T43   ) > 0) OR
            (i.T44   IS NOT NULL AND LTRIM(RTRIM(i.T44   )) <> '' AND LTRIM(RTRIM(i.T44   )) <> '0' AND PATINDEX('%[A-Za-z]%', i.T44   ) > 0) OR
            (i.Q1    IS NOT NULL AND LTRIM(RTRIM(i.Q1    )) <> '' AND LTRIM(RTRIM(i.Q1    )) <> '0' AND PATINDEX('%[A-Za-z]%', i.Q1    ) > 0) OR
            (i.G1    IS NOT NULL AND LTRIM(RTRIM(i.G1    )) <> '' AND LTRIM(RTRIM(i.G1    )) <> '0' AND PATINDEX('%[A-Za-z]%', i.G1    ) > 0) OR
            (i.G2    IS NOT NULL AND LTRIM(RTRIM(i.G2    )) <> '' AND LTRIM(RTRIM(i.G2    )) <> '0' AND PATINDEX('%[A-Za-z]%', i.G2    ) > 0) OR
            (i.DG    IS NOT NULL AND LTRIM(RTRIM(i.DG    )) <> '' AND LTRIM(RTRIM(i.DG    )) <> '0' AND PATINDEX('%[A-Za-z]%', i.DG    ) > 0) OR
            (i.DT    IS NOT NULL AND LTRIM(RTRIM(i.DT    )) <> '' AND LTRIM(RTRIM(i.DT    )) <> '0' AND PATINDEX('%[A-Za-z]%', i.DT    ) > 0) OR
            (i.TACM  IS NOT NULL AND LTRIM(RTRIM(i.TACM  )) <> '' AND LTRIM(RTRIM(i.TACM  )) <> '0' AND PATINDEX('%[A-Za-z]%', i.TACM  ) > 0) OR
            (i.GACM  IS NOT NULL AND LTRIM(RTRIM(i.GACM  )) <> '' AND LTRIM(RTRIM(i.GACM  )) <> '0' AND PATINDEX('%[A-Za-z]%', i.GACM  ) > 0) OR
            (i.GADAOS IS NOT NULL AND LTRIM(RTRIM(i.GADAOS)) <> '' AND LTRIM(RTRIM(i.GADAOS)) <> '0' AND PATINDEX('%[A-Za-z]%', i.GADAOS) > 0) OR
            (i.SURSA IS NOT NULL AND LTRIM(RTRIM(i.SURSA )) <> '' AND LTRIM(RTRIM(i.SURSA )) <> '0' AND PATINDEX('%[A-Za-z]%', i.SURSA ) > 0)
          )
    ORDER BY p.pti ASC
    """
    rows = fetchall(sql, params_t)
    for d in rows:
        d["pti"] = str(d["pti"]).strip() if d.get("pti") is not None else None
        d["ips"] = _to_int_or_none(d.get("ips"))
        d["adres"] = (d.get("adres") or "").strip()
        d["id_t1"] = None if not d.get("id_t1") else str(d["id_t1"]).strip()
        d["id_t2"] = None if not d.get("id_t2") else str(d["id_t2"]).strip()
    return rows


def get_object_by_pti(pti: str) -> Dict[str, Any] | None:
    """
    Один объект по коду pti (строка). Нужны ips и ids T1/T2.
    БЕЗ ограничения по typeObj — подходит и для 0, и для 1.
    """
    sql = """
    SELECT
        p.pti,
        p.adres_unicode AS adres,
        p.IPs   AS ips,
        i.T1    AS id_t1,
        i.T2    AS id_t2
    FROM [LOVATI].[dbo].[PTI] AS p
    LEFT JOIN [LOVATI].[dbo].[IDS] AS i
           ON p.id = i.PTI
    WHERE LTRIM(RTRIM(CAST(p.pti AS NVARCHAR(64)))) = %s
    """
    rows = fetchall(sql, (str(pti).strip(),))
    if not rows:
        return None

    d = rows[0]
    d["pti"] = str(d.get("pti") or "").strip()
    d["ips"] = _to_int_or_none(d.get("ips"))
    d["adres"] = (d.get("adres") or "").strip()
    d["id_t1"] = None if not d.get("id_t1") else str(d["id_t1"]).strip()
    d["id_t2"] = None if not d.get("id_t2") else str(d["id_t2"]).strip()
    return d


# === карта поддерживаемых параметров (имя в API -> колонка в LOVATI.dbo.IDS) ===
PARAM_COLUMNS: Dict[str, str] = {
    "Q": "Q1",
    "Q1": "Q1",
    "G1": "G1",
    "G2": "G2",
    "DG": "DG",
    "DT": "DT",
    "T1": "T1",
    "T2": "T2",
    "T31": "T31",
    "T32": "T32",
    "T41": "T41",
    "T42": "T42",
    "T43": "T43",
    "T44": "T44",
    "TACM": "TACM",
    "GACM": "GACM",
    "GADAOS": "GADAOS",
    "SURSA": "SURSA",
    "POMPA": "pompa",
    "POMPA2": "pompa2",
    "POMPA3": "pompa3",
}


def get_ips_and_param(pti: str, param: str) -> Dict[str, Any] | None:
    """
    По pti и имени параметра (из PARAM_COLUMNS) вернуть {ips, param_id}.
    Берём PTI.IPs и IDS.<column> для данного параметра. Подходит для typeObj 0/1.
    """
    key = (param or "").strip().upper()
    col = PARAM_COLUMNS.get(key)
    if not col:
        return None

    sql = f"""
    SELECT
        p.IPs AS ips,
        i.{col} AS param_id
    FROM [LOVATI].[dbo].[PTI] AS p
    LEFT JOIN [LOVATI].[dbo].[IDS] AS i
           ON p.id = i.PTI
    WHERE LTRIM(RTRIM(CAST(p.pti AS NVARCHAR(64)))) = %s
    """
    rows = fetchall(sql, (str(pti).strip(),))
    if not rows:
        return None

    d = rows[0]
    return {
        "ips": _to_int_or_none(d.get("ips")),
        "param_id": (str(d.get("param_id")).strip() if d.get("param_id") else None),
    }