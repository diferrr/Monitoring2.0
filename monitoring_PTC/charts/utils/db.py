# monitoring_PTC/monitoring_PTC/charts/utils/db.py
import pyodbc
from django.conf import settings

def _conn_str() -> str:
    # Берём CHARTS_DB, если настроен, иначе LOVATI_SERVER
    cfg = getattr(settings, "CHARTS_DB", None) or settings.LOVATI_SERVER
    parts = []
    parts.append(f"DRIVER={cfg.get('DRIVER', '{ODBC Driver 17 for SQL Server}')}")
    parts.append(f"SERVER={cfg['SERVER']}")
    parts.append(f"DATABASE={cfg['DATABASE']}")
    if cfg.get("UID"): parts.append(f"UID={cfg['UID']}")
    if cfg.get("PWD"): parts.append(f"PWD={cfg['PWD']}")
    if cfg.get("Trusted_Connection"): parts.append(f"Trusted_Connection={cfg['Trusted_Connection']}")
    parts.append("TrustServerCertificate=yes")
    return ";".join(parts) + ";"

def fetchall(sql: str, params=(), timeout: int = 15):
    """
    Выполняет SELECT и возвращает список dict.
    Поддерживает плейсхолдеры '?' (pyodbc) и '%s' (автоконвертируем в '?').
    Если плейсхолдеров нет — вызываем execute без params.
    """
    cn = pyodbc.connect(_conn_str(), autocommit=True, timeout=timeout)
    try:
        cur = cn.cursor()

        sql_exec = sql
        has_q = "?" in sql_exec
        has_ps = "%s" in sql_exec

        if has_ps:
            # конвертируем стиль '%s' → '?'
            sql_exec = sql_exec.replace("%s", "?")
            has_q = True

        if params and has_q:
            cur.execute(sql_exec, params)
        else:
            cur.execute(sql_exec)

        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cn.close()
