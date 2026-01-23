from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('monitoring.urls')),
    path('charts/', include('monitoring_PTC.charts.urls')),
    path('tc-charts/', include('monitoring_PTC.termocom_charts.urls')),
    path('pumps/', include('pumps.urls')),
    path('harta/', include('mapapp.urls')),


]
