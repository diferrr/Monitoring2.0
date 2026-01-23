"""Температурные лимиты и цветовая индикация.

Важно:
- Никакого Flask внутри Django.
- Здесь только чистые функции (без HTTP/DB).
"""

from __future__ import annotations
from decimal import Decimal, ROUND_HALF_UP

TACM = 53
T1MIN_CONST_MIN = 56
T1MIN_CONST_MAX = 82
T1MAX_CONST_MIN = 59
T1MAX_CONST_MAX = 87
T2_FROST = 60
T2_WARM = 49


def round_half_up(value: float) -> int:
    return int(Decimal(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def calculate_limits(texterior: float | None) -> dict | None:
    if texterior is None:
        return None

    value = texterior

    if value >= 9:
        T1min, T1max = 56, 59
    elif value < -9.5:
        T1min, T1max = 82, 87
    else:
        T1 = 0.0011 * (value**3) + 0.0211 * (value**2) - 1.5394 * value + 69.2908
        T1min = round_half_up(T1 * 0.97 - 0.3)
        T1max = round_half_up(T1 * 1.03 - 0.3)
        T1min = max(T1MIN_CONST_MIN, min(T1min, T1MIN_CONST_MAX))
        T1max = max(T1MAX_CONST_MIN, min(T1max, T1MAX_CONST_MAX))

    T2 = (
        0.0000001004 * (value**7)
        + 0.0000039547 * (value**6)
        + 0.0000482673 * (value**5)
        + 0.0001080336 * (value**4)
        - 0.0012740181 * (value**3)
        - 0.0008486334 * (value**2)
        - 0.6404921573 * value
        + 43.8954699268
    )

    T2 = round_half_up((T2 * 1.2) - 0.3)

    if T2 < 35:
        T2 = 35
    elif value <= 0 and T2 > T2_FROST:
        T2 = T2_FROST
    elif value > 0 and T2 > T2_WARM:
        T2 = T2_WARM

    return {"Text": round(value, 1), "Tacm": TACM, "T1min": T1min, "T1max": T1max, "T2": T2}


def _safe_float(v) -> float | None:
    try:
        if v in (None, "", "N", "—"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def define_color(param_name: str, param_value, limits: dict) -> str:
    value = _safe_float(param_value)
    if value is None:
        return "white"

    if param_name == "T1":
        if value < limits["T1min"]:
            return "blue"
        if value > limits["T1max"]:
            return "red"
    elif param_name == "T2":
        if value < 35:
            return "blue"
        if value > limits["T2"]:
            return "red"
    elif param_name == "Tacm":
        if value < (limits["Tacm"] - 3):
            return "blue"
        if value > (limits["Tacm"] + 3):
            return "red"

    return "green"
