from __future__ import annotations
from typing import Dict
import pyodbc

def dsn_from_dict(cfg: Dict[str, str]) -> str:
    return ";".join(f"{k}={v}" for k, v in cfg.items() if v is not None)

def connect(cfg: Dict[str, str], timeout: int = 10) -> pyodbc.Connection:
    return pyodbc.connect(dsn_from_dict(cfg), timeout=timeout)
