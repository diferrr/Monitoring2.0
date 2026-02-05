from rest_framework import generics
from rest_framework.response import Response
from rest_framework.views import APIView
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.cache import cache_page
from django.utils.decorators import method_decorator
from django.core.cache import cache

# ✅ GeoJSON store (вместо MySQL)
from .geo_store import get_points, split_pumps_boilers

# ⚠️ Старые импорты моделей/сериализаторов оставляем,
# но больше НЕ используем, чтобы не ломать структуру проекта.
from .models import HeatPump
from .models import Boiler
from .serializers import HeatPumpSerializer
from .serializers import BoilerSerializer

from .Update_Temperatures import (
    get_live_temperature,
    get_live_temperature_boiler,
    get_all_temperatures,
    get_boiler_onoff
)
from .Texterior import get_texterior
from .limit import calculate_limits, define_color

import logging

logger = logging.getLogger(__name__)


def _normalize_param_name(name: str) -> str:
    """
    Приводим имена к одному виду, чтобы совпадали ключи:
    'PT_XXX' и 'XXX' и разные регистры.
    """
    return (name or "").replace("PT_", "").strip().lower()



def map_view(request):
    return render(request, "map.html")


# =============================================================================
# 1) /api/pumps/  (раньше было HeatPump.objects.all(), теперь GeoJSON)
# ВАЖНО: формат ответа сохраняем максимально совместимым с фронтом.
# =============================================================================
class HeatPumpList(generics.ListAPIView):
    # queryset/serializer_class оставляем, чтобы класс выглядел "как раньше",
    # но переопределяем get() и возвращаем данные из geojson.
    queryset = HeatPump.objects.none()
    serializer_class = HeatPumpSerializer

    def get(self, request, *args, **kwargs):
        points = get_points()
        pumps, _ = split_pumps_boilers(points)

        out = []
        for i, p in enumerate(pumps, start=1):
            props = p.props or {}

            out.append({
                "id": i,
                "address": p.address,
                "param_name": p.param_name,
                "longitude": p.lon,
                "lat": p.lat,
                "number_map": props.get("number_map") or 0,
                "datasource_id": p.datasource_id,
                "id_T1": props.get("T1"),
                "id_T2": props.get("T2"),
                "id_G1": props.get("G1"),
                "id_dG": props.get("dG"),
                "type_device": p.type_device,
            })

        return Response(out)


# =============================================================================
# 2) /api/boilers/  (раньше Boiler.objects.all(), теперь GeoJSON)
# =============================================================================
class BoilerListView(APIView):
    def get(self, request):
        try:
            points = get_points()
            _, boilers = split_pumps_boilers(points)

            out = []
            for p in boilers:
                props = p.props or {}

                out.append({
                    "address": p.address,
                    "param_name": p.param_name,
                    "datasource_id": p.datasource_id,
                    "id_T1": props.get("T1"),
                    "id_T2": props.get("T2"),
                    "name_device": props.get("name_device") or props.get("name_scheme"),
                    "type_device": p.type_device,
                    "latitude": p.lat,
                    "longitude": p.lon,
                })

            return Response(out)
        except Exception as e:
            return Response({"error": str(e)}, status=500)


# =============================================================================
# 3) Температура по одному объекту (НЕ меняем)
# =============================================================================
class LiveTemperatureView(APIView):
    def get(self, request, param_name):
        try:
            t1, t2 = get_live_temperature(param_name)
            return Response({
                "T1": t1 if t1 is not None else "—",
                "T2": t2 if t2 is not None else "—"
            })
        except Exception as e:
            return Response({"error": str(e)}, status=500)


# ====== Новый эндпоинт для котельных (НЕ меняем) ======
class LiveTemperatureBoilerView(APIView):
    def get(self, request, param_name):
        try:
            t1, t2 = get_live_temperature_boiler(param_name)
            return Response({
                "T1": t1 if t1 is not None else "—",
                "T2": t2 if t2 is not None else "—"
            })
        except Exception as e:
            return Response({"error": str(e)}, status=500)


class BoilerOnOffView(APIView):
    def get(self, request, param_name):
        value = get_boiler_onoff(param_name)
        return Response({"onoff": value})


def exterior_temp(request):
    try:
        temperature = get_texterior()
        if temperature is None:
            return JsonResponse({'error': 'Нет данных о температуре'}, status=500)
        return JsonResponse({'temperature': temperature})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


class TemperatureLimitsAPIView(APIView):
    def get(self, request, *args, **kwargs):
        try:
            texterior = get_texterior()
            result = calculate_limits(texterior)
            if result is None:
                return Response({"error": "Ошибка при получении температуры"}, status=500)
            return Response(result)
        except Exception as e:
            return Response({"error": str(e)}, status=500)


@method_decorator(cache_page(30), name='dispatch')
class LiveTemperatureBulkView(APIView):
    throttle_scope = "bulk"  # важно: в settings.py должен быть DRF throttling по scope bulk

    def get(self, request):
        try:
            data = get_all_temperatures()
            result = {r["name"]: {"T1": r["T1"], "T2": r["T2"]} for r in data}
            return Response(result)
        except Exception:
            logger.exception("Ошибка в LiveTemperatureBulkView")
            return Response({"error": "Internal error"}, status=500)



class TemperatureColorAPIView(APIView):
    def get(self, request):
        param_name = request.GET.get('param_name')
        param_value = request.GET.get('param_value')

        if not param_name or param_value is None:
            return Response({"error": "Не заданы необходимые параметры"}, status=400)

        texterior = get_texterior()
        limits = calculate_limits(texterior)
        if not limits:
            return Response({"error": "Ошибка расчёта лимитов температур"}, status=500)

        color = define_color(param_name, param_value, limits)

        return Response({
            "param_name": param_name,
            "param_value": param_value,
            "color": color,
            "limits": limits
        })


# =============================================================================
# 4) /api/pumps-geojson/  (раньше строили по ORM, теперь строим по GeoJSON)
# Формат ответа тот же: FeatureCollection с T1/T2 (значения)
# =============================================================================
def pumps_geojson(request):
    cache_key = "geojson:pumps:v4"
    cached = cache.get(cache_key)
    if cached is not None:
        return JsonResponse(cached)

    # Температуры (уже кэшируются внутри get_all_temperatures)
    temp_data = get_all_temperatures()
    temp_lookup = {_normalize_param_name(d.get("name")): d for d in temp_data}

    # Точки из geojson
    points = get_points()
    pumps, _ = split_pumps_boilers(points)

    features = []
    for p in pumps:
        try:
            lon = float(p.lon)
            lat = float(p.lat)
        except Exception:
            continue

        key = _normalize_param_name(p.param_name)
        temps = temp_lookup.get(key, {})
        t1 = temps.get("T1")
        t2 = temps.get("T2")

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "param_name": p.param_name,
                "address": p.address,
                "T1": t1,
                "T2": t2,
            },
        })

    geojson = {"type": "FeatureCollection", "features": features}
    cache.set(cache_key, geojson, 30)
    return JsonResponse(geojson)



def get_ip(request):
    ip = request.META.get("HTTP_X_FORWARDED_FOR")
    if ip:
        ip = ip.split(",")[0].strip()
    else:
        ip = request.META.get("REMOTE_ADDR")
    return JsonResponse({"ip": ip})
