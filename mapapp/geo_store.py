from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

from django.conf import settings
from django.core.cache import cache

# Кэш для списка точек (ключ зависит от файла, чтобы не ловить "старые" данные)
_GEO_CACHE_TTL = 3600  # 1 час

# Кэш для индекса param_name -> GeoPoint
_INDEX_CACHE_TTL = 3600  # 1 час


# Путь к файлу (положи файл сюда)
DEFAULT_GEO_PATH = Path(settings.BASE_DIR) / "mapapp" / "static" / "data" / "acb.geojson"


@dataclass(frozen=True)
class GeoPoint:
    param_name: str
    address: str
    type_device: int
    datasource_id: Any
    lon: float
    lat: float
    props: Dict[str, Any]


def _load_geojson(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def get_points(path: Path = DEFAULT_GEO_PATH) -> List[GeoPoint]:
    geo_cache_key = f"geo:{path.name}:features:v1"
    index_cache_key = f"geo:{path.name}:index_by_param:v1"

    cached = cache.get(geo_cache_key)
    if cached is not None:
        return cached


    data = _load_geojson(path)
    features = data.get("features", [])
    points: List[GeoPoint] = []

    for f in features:
        try:
            props = f.get("properties") or {}
            geom = f.get("geometry") or {}
            coords = geom.get("coordinates") or [None, None]
            lon, lat = float(coords[0]), float(coords[1])

            points.append(
                GeoPoint(
                    param_name=str(props.get("param_name", "")).strip(),
                    address=str(props.get("address", "")).strip(),
                    type_device=int(props.get("type_device")),
                    datasource_id=props.get("datasource_id"),
                    lon=lon,
                    lat=lat,
                    props=props,
                )
            )
        except Exception:
            # пропускаем битые точки, чтобы карта не падала
            continue

    cache.set(geo_cache_key, points, _GEO_CACHE_TTL)
    # индекс зависит от points — сбрасываем, чтобы пересобрался по новым данным
    cache.delete(index_cache_key)
    return points


def index_by_param(points: List[GeoPoint]) -> Dict[str, GeoPoint]:
    # ключ как в твоей логике: PT_ убираем, lower
    out: Dict[str, GeoPoint] = {}
    for p in points:
        key = p.param_name.lower().replace("pt_", "").strip()
        if key:
            out[key] = p
    return out

def get_index_by_param(path: Path = DEFAULT_GEO_PATH) -> Dict[str, GeoPoint]:
    """
    Кэшируем индекс, чтобы Update_Temperatures не пересобирал его на каждый запрос.
    """
    index_cache_key = f"geo:{path.name}:index_by_param:v1"

    cached = cache.get(index_cache_key)
    if cached is not None:
        return cached

    points = get_points(path)
    idx = index_by_param(points)
    cache.set(index_cache_key, idx, _INDEX_CACHE_TTL)
    return idx




def split_pumps_boilers(points: List[GeoPoint]) -> Tuple[List[GeoPoint], List[GeoPoint]]:
    # Сохраняем старую структуру: HeatPump ~ type_device 1/2, Boiler ~ остальные
    # ✅ type_device == 8 полностью исключаем (не отдаём на фронт вообще)
    pumps: List[GeoPoint] = []
    boilers: List[GeoPoint] = []

    for p in points:
        if p.type_device == 8:
            continue

        if p.type_device in (1, 2):
            pumps.append(p)
        else:
            boilers.append(p)

    return pumps, boilers
