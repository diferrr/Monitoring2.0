from django.urls import path
from .views_web import pumps_page
from .views_api import pumps_table_api

urlpatterns = [
    path("", pumps_page, name="pumps-page"),
    path("api/table/", pumps_table_api, name="pumps-api-table"),
]
