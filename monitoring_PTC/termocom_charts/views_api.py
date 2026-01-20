# monitoring_PTC/termocom_charts/views_api.py
#
# HTML-страница и API для графиков TERMOCOM5.

from __future__ import annotations

from typing import Any, Dict, List
from datetime import datetime

from django.shortcuts import render
from django.http import JsonResponse

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from monitoring_PTC.charts.timezone_utils import parse_local_iso
from monitoring_PTC.charts.serializers import SeriesResponseSerializer

from .repositories import (
    TERMOCOM_PARAM_MAP,
    resolve_unit_id_by_ptc,
    fetch_termocom_series,
    list_objects_tc,
)

# Специальное правило:
# для GACM некоторых объектов (5019, 4046) график строим по G1 их "A"-вариантов.
# 5019  -> G1(5019A)
# 4046  -> G1(4046A)
PTC_GACM_FROM_A: Dict[str, str] = {
    "5019": "5019A",
    "4046": "4046A",
}


# ---------- HTML-страница ----------

def chart_page(request):
    """
    Страница графика TERMOCOM5.
    """
    return render(request, "charts/termocom_chart.html")


# ---------- Список объектов TERMOCOM5 ----------

def api_objects(request):
    """
    Список объектов TERMOCOM5 для селекта `II obiect`.
    """
    data = list_objects_tc()
    return JsonResponse(data, safe=False)


# ---------- Серия для графика по TERMOCOM5 ----------

class TermocomSeriesView(APIView):
    """
    GET /tc-charts/api/series/
        ?pti=5020
        &param=G1
        &start=2025-11-17T00:00
        &end=2025-11-17T23:59
    """

    def get(self, request, *args, **kwargs):
        pti_raw = (request.query_params.get("pti") or "").strip()
        param_raw = (request.query_params.get("param") or "").strip()
        start_s = (request.query_params.get("start") or "").strip()
        end_s = (request.query_params.get("end") or "").strip()

        if not pti_raw or not param_raw or not start_s or not end_s:
            return Response(
                {"detail": "required: pti, param, start, end"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Всегда работаем с верхним регистром кода параметра
        param = param_raw.upper()
        if param not in TERMOCOM_PARAM_MAP:
            return Response(
                {"detail": f"param not supported. allowed: {', '.join(sorted(TERMOCOM_PARAM_MAP.keys()))}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # По умолчанию берём серию по самому объекту и тому же параметру
        lookup_pti = pti_raw
        lookup_param = param

        # ОСОБОЕ ПРАВИЛО:
        # если просим GACM для 5019 или 4046, то фактически читаем G1 от 5019A/4046A
        if param == "GACM" and pti_raw in PTC_GACM_FROM_A:
            lookup_pti = PTC_GACM_FROM_A[pti_raw]   # '5019A' или '4046A'
            lookup_param = "G1"

        # 1) ищем UNIT_ID по (lookup_pti) в UNITS
        unit_id = resolve_unit_id_by_ptc(lookup_pti)
        if not unit_id:
            return Response(
                {"detail": f"PTC '{lookup_pti}' not found or not enabled in TERMOCOM5"},
                status=status.HTTP_404_NOT_FOUND,
            )

        # 2) парсим время (локальное, как в LOVATI)
        try:
            dt_start = parse_local_iso(start_s)
            dt_end = parse_local_iso(end_s)
        except Exception as e:
            return Response({"detail": f"bad datetime: {e}"}, status=status.HTTP_400_BAD_REQUEST)

        if dt_end < dt_start:
            dt_start, dt_end = dt_end, dt_start

        # 3) забираем серию из таблицы
        try:
            # ВАЖНО: используем lookup_param (иногда это G1 вместо GACM)
            pairs = fetch_termocom_series(unit_id, lookup_param, dt_start, dt_end)
        except Exception as e:
            return Response({"detail": f"db error: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        labels: List[str] = []
        values: List[float] = []
        for ts, val in pairs:
            if isinstance(ts, datetime):
                labels.append(ts.isoformat())
                values.append(float(val))

        # 4) простая статистика (в том же формате, что и LOVATI)
        if values:
            sorted_vals = sorted(values)
            n = len(values)
            median_val = sorted_vals[n // 2] if n % 2 == 1 else (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2.0
            summary: Dict[str, Any] = {
                "count": n,
                "min": min(values),
                "max": max(values),
                "avg": sum(values) / n,
                "median": median_val,
                "stdev": 0.0,  # при желании можно доработать через statistics.pstdev
            }
        else:
            summary = {"count": 0, "min": None, "max": None, "avg": None, "median": None, "stdev": None}

        payload: Dict[str, Any] = {"labels": labels, "values": values, "summary": summary}
        ser = SeriesResponseSerializer(payload)
        return Response(ser.data, status=status.HTTP_200_OK)
