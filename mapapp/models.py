from django.db import models


class HeatPump(models.Model):
    id = models.AutoField(primary_key=True)  # ID (ключ)
    address = models.CharField(max_length=255)  # Адрес
    param_name = models.CharField(max_length=255)  # Название насоса
    longitude = models.FloatField()  # Долгота
    lat = models.FloatField()  # Широта
    number_map = models.IntegerField()  # ID объекта
    datasource_id = models.CharField(
        max_length=255, null=True, blank=True
    )  # ID источника
    id_T1 = models.CharField(max_length=255, null=True, blank=True)  # ID T1
    id_T2 = models.CharField(max_length=255, null=True, blank=True)  # ID T2
    id_G1 = models.CharField(max_length=255, null=True, blank=True)  # ID G1
    id_dG = models.CharField(max_length=255, null=True, blank=True)  # ID dG
    type_device = models.IntegerField()  # Тип устройства (1 = PTC, 2 = PTI)

    class Meta:
        managed = False  # Django НЕ управляет таблицей!
        db_table = "map_markers"  # Используем существующую таблицу в SSMS

    def __str__(self):
        return self.param_name



class Boiler(models.Model):
    id = models.AutoField(primary_key=True)
    address = models.CharField(max_length=255)
    param_name = models.CharField(max_length=255)
    lat = models.CharField(max_length=64)         # 👈 строка!
    longitude = models.CharField(max_length=64)   # 👈 строка!
    datasource_id = models.IntegerField()
    id_T1 = models.CharField(max_length=255, null=True, blank=True)
    id_T2 = models.CharField(max_length=255, null=True, blank=True)
    name_device = models.CharField(max_length=255)
    type_device = models.IntegerField()

    class Meta:
        managed = False
        db_table = "map_markers_cazan"

    def __str__(self):
        return self.param_name
