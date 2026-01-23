from rest_framework import serializers
from .models import HeatPump
from .models import Boiler


class HeatPumpSerializer(serializers.ModelSerializer):
    class Meta:
        model = HeatPump
        fields = "__all__"


class BoilerSerializer(serializers.ModelSerializer):
    latitude = serializers.SerializerMethodField()
    longitude = serializers.SerializerMethodField()

    class Meta:
        model = Boiler
        fields = [
            "address", "param_name", "datasource_id",
            "id_T1", "id_T2", "name_device", "type_device",
            "latitude", "longitude"
        ]

    def get_latitude(self, obj):
        try:
            return float(obj.lat.split(',')[0])
        except Exception:
            return None

    def get_longitude(self, obj):
        try:
            return float(obj.lat.split(',')[1]) if ',' in obj.lat else float(obj.longitude)
        except Exception:
            return None
