# monitoring_PTC/termocom_charts/urls.py

from django.urls import path
from . import views_api

app_name = "termocom_charts"

urlpatterns = [
    # HTML-страница графика TERMOCOM5
    path("chart/", views_api.chart_page, name="chart_page"),

    # Список объектов TERMOCOM5 (пока пустой)
    path("api/objects/", views_api.api_objects, name="api_objects"),

    # Серия для графика TERMOCOM5
    path("api/series/", views_api.TermocomSeriesView.as_view(), name="api_series"),
]
