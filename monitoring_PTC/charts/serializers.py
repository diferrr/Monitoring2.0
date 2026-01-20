# charts/serializers.py
# МОДУЛЬ: DRF-сериализаторы для API графиков.
# Описывает формы данных для:
#  - списка объектов (ObjectItemSerializer) с вложенными ID параметров (ObjectIDsSerializer),
#  - ответа со временными рядами и краткой статистикой (SeriesResponseSerializer + SeriesSummarySerializer).

from __future__ import annotations            # поддержка аннотаций типов на старых версиях Python
from rest_framework import serializers        # базовые классы DRF для описания схем/валидации

# --- Вложенный блок ID параметров объекта (используется внутри ObjectItemSerializer) ---
class ObjectIDsSerializer(serializers.Serializer):    # объявление сериализатора с конкретными полями
    t1 = serializers.CharField(allow_null=True, required=False)  # строковый ID для T1; может быть null; поле опционально
    t2 = serializers.CharField(allow_null=True, required=False)  # строковый ID для T2; может быть null; поле опционально

# --- Элемент списка объектов, который фронт получает для выпадающего списка и сайдбара ---
class ObjectItemSerializer(serializers.Serializer):   # сериализатор одного объекта PTC/PTI
    pti = serializers.CharField()                     # код объекта (строка), обязателен
    adres = serializers.CharField()                   # адрес (строка), обязателен
    ips = serializers.IntegerField(allow_null=True)   # код сервера (целое) или null, если не задан
    ids = ObjectIDsSerializer()                       # вложенный блок с ID параметров (t1/t2)

# --- Короткая статистика по ряду значений (возвращается вместе с данными графика) ---
class SeriesSummarySerializer(serializers.Serializer):  # агрегированная статистика по values
    count = serializers.IntegerField()                  # количество точек
    min = serializers.FloatField(allow_null=True)       # минимум (float) или null, если нет данных
    max = serializers.FloatField(allow_null=True)       # максимум (float) или null
    avg = serializers.FloatField(allow_null=True)       # среднее (float) или null
    median = serializers.FloatField(allow_null=True)    # медиана (float) или null
    stdev = serializers.FloatField(allow_null=True)     # стандартное отклонение (float) или null

# --- Ответ эндпоинта /charts/api/series: метки времени, значения и статистика ---
class SeriesResponseSerializer(serializers.Serializer):   # структура ответа для графика
    labels = serializers.ListField(child=serializers.CharField())     # список меток времени (строки ISO)
    values = serializers.ListField(child=serializers.FloatField())    # список значений (float)
    summary = SeriesSummarySerializer()                               # вложенная статистика по values