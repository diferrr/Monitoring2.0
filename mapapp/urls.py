from django.urls import path
from .views import (
    HeatPumpList,
    map_view,
    LiveTemperatureView,
    LiveTemperatureBoilerView,  # добавлено!
    exterior_temp,
    TemperatureLimitsAPIView,
    LiveTemperatureBulkView,
    TemperatureColorAPIView,
    BoilerListView,
    pumps_geojson
)
from . import views

# Импорты для статики
from django.conf import settings
from django.conf.urls.static import static
import os

urlpatterns = [
    path('api/pumps/', HeatPumpList.as_view(), name="heat-pump-list"),
    path('api/live_temp/<str:param_name>/', LiveTemperatureView.as_view(), name="live-temp"),
    path('api/live_temp_boiler/<str:param_name>/', LiveTemperatureBoilerView.as_view(), name="live-temp-boiler"),  # <-- новый маршрут
    path('api/live_temp_bulk/', LiveTemperatureBulkView.as_view(), name="live-temp-bulk"),
    path('', map_view, name="map"),
    path('api/exterior_temp/', exterior_temp, name='exterior_temp'),
    path('api/temperature_limits/', TemperatureLimitsAPIView.as_view(), name='temperature_limits'),
    path('api/temperature_color/', TemperatureColorAPIView.as_view(), name='temperature_color'),
    path('api/boilers/', BoilerListView.as_view(), name='boiler-list'),
    path('api/pumps-geojson/', pumps_geojson, name='pumps_geojson'),
    path('api/get_ip/', views.get_ip, name='get_ip'),
]

# Добавляем поддержку static файлов в режиме разработки
if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=os.path.join(settings.BASE_DIR, 'static'))
