# charts/views_api.py
# МОДУЛЬ: DRF-вью для API графиков.
# Содержит три endpoint-а:
#   - ObjectsView  → список объектов (c поддержкой ?types=0,1 и обратной совместимостью ?typeObj=0)
#   - SeriesView   → временной ряд по pti+param и интервалу времени (тянет XML с прибора и парсит)
#   - ParamIdView  → получить для pti+param связку {ips, param_id}

from __future__ import annotations                         # аннотации типов на старых версиях Python
from typing import Any, Dict, List                         # подсказки типов

from rest_framework.views import APIView                   # базовый класс DRF-вью
from rest_framework.response import Response               # HTTP-ответ DRF
from rest_framework import status                          # коды статусов

from .repositories import list_objects, get_ips_and_param, PARAM_COLUMNS  # доступ к БД/маппингам
from .serializers import ObjectItemSerializer, SeriesResponseSerializer    # схемы ответа
from .timezone_utils import parse_local_iso, to_epoch_seconds              # разбор дат и конвертация в epoch
from .http_clients import fetch_xml, SERVER_MAP                            # HTTP-клиент к приборам
from .xml_parser import parse_series                                       # парсер XML → (ts, value)

from statistics import mean, median, pstdev                 # базовая статистика
from urllib.parse import urlencode                          # сборка URL в debug-ответах


class ObjectsView(APIView):
    """
    GET /charts/api/objects/?types=0,1
    Совместимо назад: GET /charts/api/objects/?typeObj=0
    """
    def get(self, request, *args, **kwargs):
        # Новый способ: ?types=0,1 | ?types=all
        types_qs = (request.query_params.get("types") or "").strip().lower()  # читаем и нормализуем ?types

        if types_qs:                                                           # если новый параметр указан —
            if types_qs in ("all",):                                           # спец.значение 'all' → оба типа
                types = [0, 1]                                                 # объединённый список
            else:
                try:
                    parts = [p.strip() for p in types_qs.split(",") if p.strip() != ""]  # разбиваем "0,1"
                    types = [int(p) for p in parts]                                       # валидируем как int
                except ValueError:
                    return Response({"detail": "types must be a comma-separated list of ints, e.g. '0,1' or 'all'"},
                                    status=status.HTTP_400_BAD_REQUEST)        # ошибка формата запроса
        else:
            # Старый способ: ?typeObj=0 (по умолчанию 0)
            type_obj_param = request.query_params.get("typeObj", "0")          # читаем старый параметр
            try:
                types = [int(type_obj_param)]                                  # делаем список для унификации
            except ValueError:
                return Response({"detail": "typeObj must be int"}, status=status.HTTP_400_BAD_REQUEST)

        rows = list_objects(type_obj=types)                                     # берём объекты из репозитория
        payload: List[Dict[str, Any]] = [                                        # собираем полезный ответ
            {
                "pti": r["pti"],                                                # код объекта (строка)
                "adres": r["adres"],                                            # адрес
                "ips": r["ips"],                                                # сервер (код)
                "ids": {                                                        # вложенные ID параметров (минимально: T1/T2)
                    "t1": r["id_t1"],
                    "t2": r["id_t2"],
                },
            }
            for r in rows
        ]
        ser = ObjectItemSerializer(payload, many=True)                           # валидация/серилизация структурой DRF
        return Response(ser.data, status=status.HTTP_200_OK)                     # 200 + данные


class SeriesView(APIView):
    """
    GET /charts/api/series/?pti=3107&param=T1&start=2025-03-01T00:00&end=2025-10-31T23:59
    `param` в: Q/Q1, G1, G2, DG, DT, T1, T2, T31, T32, T41, T42, T43, T44, TACM, GACM, GADAOS, SURSA.
    """
    def get(self, request, *args, **kwargs):
        # pti — СТРОКА (поддерживает '2050.01', '28.145' и т.п.)
        pti = (request.query_params.get("pti") or "").strip()                   # обязательный идентификатор объекта
        param = (request.query_params.get("param") or "").upper().strip()       # код параметра (в верхнем регистре)
        start_s = (request.query_params.get("start") or "").strip()             # начало интервала, локальное ISO
        end_s = (request.query_params.get("end") or "").strip()                 # конец интервала, локальное ISO
        debug = (request.query_params.get("debug") or "").lower() in ("1", "true", "yes")  # флаг debug-режима

        if not pti or not start_s or not end_s:                                 # проверка обязательных полей
            return Response(
                {"detail": "required: pti, start, end (+ param in: " + ", ".join(sorted(PARAM_COLUMNS.keys())) + ")"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if param not in PARAM_COLUMNS:                                          # параметр не поддержан?
            return Response({"detail": f"param not supported. allowed: {', '.join(sorted(PARAM_COLUMNS.keys()))}"},
                            status=status.HTTP_400_BAD_REQUEST)

        # 1) ips и ID параметра
        info = get_ips_and_param(pti, param)                                    # ищем сервер и id параметра в IDS
        if not info:
            return Response({"detail": f"pti '{pti}' not found or no mapping"}, status=status.HTTP_404_NOT_FOUND)

        ips = info.get("ips")                                                   # код сервера
        if not ips:
            return Response({"detail": "object has no server (ips)"}, status=status.HTTP_400_BAD_REQUEST)

        param_id = info.get("param_id")                                         # строковый id LOVATI параметра
        if not param_id:
            return Response({"detail": f"no parameter id for {param}"}, status=status.HTTP_400_BAD_REQUEST)

        # 2) локальное время → epoch
        try:
            dt_start = parse_local_iso(start_s)                                  # парсим локальную дату (Chisinau)
            dt_end = parse_local_iso(end_s)                                      # парсим локальную дату (Chisinau)
            start_epoch = to_epoch_seconds(dt_start)                              # → epoch UTC (сек)
            stop_epoch = to_epoch_seconds(dt_end)                                 # → epoch UTC (сек)
        except Exception as e:
            return Response({"detail": f"bad datetime: {e}"}, status=status.HTTP_400_BAD_REQUEST)

        if stop_epoch < start_epoch:                                             # если даты перепутаны —
            start_epoch, stop_epoch = stop_epoch, start_epoch                    # меняем местами

        # 3) HTTP к прибору
        try:
            ips_int = int(ips)                                                   # SERVER_MAP обычно по int-ключам
        except Exception:
            ips_int = ips                                                        # fallback (не ломаемся)
        try:
            raw_xml = fetch_xml(ips_int, str(param_id), start_epoch, stop_epoch) # тянем XML байтами
        except Exception as e:
            return Response({"detail": f"http error: {e}"}, status=status.HTTP_502_BAD_GATEWAY)

        # 4) парсим серию
        pairs = parse_series(raw_xml)                                            # bytes XML → [(iso_ts, value), ...]
        labels = [ts for ts, _ in pairs]                                         # список меток времени
        values = [v for _, v in pairs]                                           # список значений

        # 5) статистика
        if values:                                                               # если есть точки —
            summary = {
                "count": len(values),                                            # количество
                "min": min(values),                                              # минимум
                "max": max(values),                                              # максимум
                "avg": mean(values),                                             # среднее (популяционное)
                "median": median(values),                                        # медиана
                "stdev": pstdev(values) if len(values) > 1 else 0.0,            # σ (pstdev) или 0 при 1 точке
            }
        else:
            summary = {"count": 0, "min": None, "max": None, "avg": None, "median": None, "stdev": None}  # пусто

        payload: Dict[str, Any] = {"labels": labels, "values": values, "summary": summary}  # итоговый ответ

        if debug:                                                                # режим отладки — добавляем URL
            base = SERVER_MAP.get(ips_int)                                       # находим базовый CGI по серверу
            if base:
                payload["debug"] = {
                    "url": f"{base}?{urlencode({'param': param_id, 'start': start_epoch, 'stop': stop_epoch})}",  # прямой запрос
                    "points": len(values),                                       # сколько точек получили
                    "server": ips_int,                                           # код сервера
                    "param_id": param_id,                                        # какой id параметра
                }
            return Response(payload, status=status.HTTP_200_OK)                  # возвращаем как есть (без DRF-схемы)

        ser = SeriesResponseSerializer(payload)                                  # валидация/сериализация по схеме
        return Response(ser.data, status=status.HTTP_200_OK)                     # 200 + данные под схему


class ParamIdView(APIView):
    """
    GET /charts/api/param-id/?pti=<object_id>&param=<код_параметра>
    -> {"ips": 1013, "param_id": "512a1MCC0056"}
    """
    def get(self, request, *args, **kwargs):
        # pti — СТРОКА, не приводим к int
        pti = (request.query_params.get("pti") or "").strip()                    # идентификатор объекта
        param = (request.query_params.get("param") or "").strip()                # код параметра (как на фронте)

        if not pti or not param:                                                 # обязательные параметры
            return Response({"detail": "pti and param are required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            info = get_ips_and_param(pti, param)                                 # ищем {ips, param_id}
            if not info:                                                         # не нашли объект/маппинг
                return Response({"ips": None, "param_id": None}, status=status.HTTP_404_NOT_FOUND)

            pid = (                                                               # нормализуем имя поля с ID
                info.get("param_id")
                or info.get("param")
                or info.get("id")
                or info.get("lovati_id")
                or info.get("lovati")
            )
            payload = {"ips": info.get("ips"), "param_id": pid}                  # итоговый JSON
            return Response(payload, status=status.HTTP_200_OK)                  # 200

        except Exception as exc:                                                 # защита от неожиданных ошибок
            return Response({"detail": f"ParamIdView error: {exc}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)