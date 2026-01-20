from django.urls import path

from . import views

urlpatterns = [
    path('', views.ptc_table, name='ptc_table'),  # Главная страница с таблицей
    path('api/ptc/', views.api_ptc_data, name='api_ptc_data'),  # API для данных
    path('export-excel/', views.export_ptc_excel, name='export_ptc_excel'),  # <--- НОВЫЙ ЭНДПОИНТ
    path("exclude/<str:ptc>/", views.exclude_view, name="exclude_view"),
    path("comment/<str:ptc>/", views.comment_view, name="comment_view"),
]
