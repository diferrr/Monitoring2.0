# charts/views.py
# МОДУЛЬ: простые Django-вьюхи для раздела графиков.
#  - chart_page: отдать HTML-страницу с графиком.
#  - api_objects: вернуть JSON-список объектов LOVATI (по умолчанию только typeObj=0; можно запросить 0 и 1).

from django.shortcuts import render           # рендеринг HTML-шаблонов
from django.http import JsonResponse          # удобный ответ JSON
from .repositories import list_objects        # репозиторий для чтения объектов из БД


# ВЬЮ: страница графика (HTML)
def chart_page(request):
    return render(request, "charts/pit_chart.html")   # отдаём шаблон charts/pit_chart.html без контекста


# ВЬЮ: JSON-список объектов для фронта
def api_objects(request):
    """
    GET /charts/api/objects
    - по умолчанию: только typeObj=0 (поведение как раньше)
    - если ?types=0,1 или ?types=all: вернёт объединённый список (0 и 1)
    """
    types_qs = (request.GET.get('types') or '0').strip().lower()   # читаем ?types, по умолчанию '0'; нормализуем регистр/пробелы
    types = [0, 1] if types_qs in ('all', '0,1', '1,0') else [0]   # определяем набор типов: либо [0,1], либо только [0]
    data = list_objects(types)                                     # получаем список объектов из репозитория по выбранным типам
    return JsonResponse(data, safe=False)                          # отдаём список как JSON (safe=False — разрешает список в корне)