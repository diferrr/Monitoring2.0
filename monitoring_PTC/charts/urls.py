# monitoring_PTC/charts/urls.py
# МОДУЛЬ URL-роутинга приложения "charts".
# Определяет namespace 'charts' и 4 маршрута:
#   - /charts/api/objects/  → список объектов LOVATI (для выпадающего списка и сравнения)
#   - /charts/api/series/   → данные временного ряда для выбранного параметра
#   - /charts/api/param-id/ → получить id LOVATI параметра по pti+param
#   - /charts/chart/        → страница с графиком (HTML + JS)

from django.urls import path                           # path() — декларативное описание маршрутов
from .views import chart_page                           # view страницы графика
from .views_api import ObjectsView, SeriesView, ParamIdView  # DRF-классы для API

app_name = "charts"  # ← полезно для namespace              # позволит делать reverse('charts:имя_маршрута')

urlpatterns = [                                           # список маршрутов (в порядке проверки)
    path("api/objects/", ObjectsView.as_view(), name="api_objects"),   # GET список объектов: /charts/api/objects/
    path("api/series/",  SeriesView.as_view(),  name="api_series"),    # GET серия значений: /charts/api/series/
    path("api/param-id/", ParamIdView.as_view(), name="api_param_id"), # GET id параметра: /charts/api/param-id/
    path("chart/", chart_page, name="chart_page"),                     # HTML-страница графика: /charts/chart/
]