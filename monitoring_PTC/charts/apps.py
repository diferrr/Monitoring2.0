# monitoring_PTC/charts/apps.py
from django.apps import AppConfig

class ChartsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "monitoring_PTC.charts"  # <-- ВАЖНО: полный путь к пакету
    label = "charts"                # короткая метка (оставь так)
