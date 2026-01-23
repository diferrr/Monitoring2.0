from rest_framework import generics
from rest_framework.response import Response
from rest_framework.views import APIView
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.cache import cache_page
from django.utils.decorators import method_decorator
from django.core.cache import cache


from .models import HeatPump
from .models import Boiler
from .serializers import HeatPumpSerializer
from .serializers import BoilerSerializer
from .Update_Temperatures import (
    get_live_temperature,
    get_live_temperature_boiler,  # добавь импорт!
    get_all_temperatures
)
from .Texterior import get_texterior
from .limit import calculate_limits, define_color

def map_view(request):
    return render(request, "map.html")

class HeatPumpList(generics.ListAPIView):
    queryset = HeatPump.objects.all()
    serializer_class = HeatPumpSerializer

class BoilerListView(APIView):
    def get(self, request):
        try:
            boilers = Boiler.objects.all()
            serializer = BoilerSerializer(boilers, many=True)
            return Response(serializer.data)
        except Exception as e:
            return Response({"error": str(e)}, status=500)

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

# ====== Новый эндпоинт для котельных ======
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
    def get(self, request):
        try:
            data = get_all_temperatures()
            result = {
                r['name']: {'T1': r['T1'], 'T2': r['T2']}
                for r in data
            }
            return Response(result)
        except Exception as e:
            print("🔥 Ошибка в LiveTemperatureBulkView:", e)
            return Response({"error": str(e)}, status=500)

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

def pumps_geojson(request):
    # 1) Кэшируем целиком GeoJSON (максимально быстро для карты)
    cache_key = "geojson:pumps:v2"
    cached = cache.get(cache_key)
    if cached is not None:
        return JsonResponse(cached, content_type="application/json")

    # 2) Температуры уже кэшируются внутри get_all_temperatures()
    temp_data = get_all_temperatures()
    temp_lookup = {d["name"].lower(): d for d in temp_data}

    # 3) ORM: берём только нужные поля + iterator (меньше RAM)
    qs = HeatPump.objects.values("param_name", "address", "longitude", "lat").iterator(chunk_size=2000)

    features = []
    for pump in qs:
        try:
            lon = float(pump["longitude"])
            lat = float(pump["lat"])
        except Exception:
            continue

        pump_name = pump["param_name"].replace("PT_", "").strip().lower()
        temps = temp_lookup.get(pump_name, {})
        t1 = temps.get("T1")
        t2 = temps.get("T2")

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "param_name": pump["param_name"],
                "address": pump["address"],
                "T1": t1,
                "T2": t2,
            },
        })

    geojson = {"type": "FeatureCollection", "features": features}
    cache.set(cache_key, geojson, 30)
    return JsonResponse(geojson, content_type="application/json")



def get_ip(request):
    # IP клиента
    ip = request.META.get("HTTP_X_FORWARDED_FOR")
    if ip:
        ip = ip.split(",")[0].strip()
    else:
        ip = request.META.get("REMOTE_ADDR")
    return JsonResponse({"ip": ip})

