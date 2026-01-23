from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pytz
import requests
from django.core.cache import cache
from lxml import etree

logger = logging.getLogger(__name__)

SCADA_IP = "10.1.1.242"
PARAM_ID = "001aiisMEDA"
TIMEOUT = 10

# Сколько секунд держать Texterior в кэше.
TEXTERIOR_CACHE_TTL = 120


def build_url_day_range(scada_ip, param_id):
    tz = pytz.timezone("Europe/Chisinau")
    now = datetime.now(tz)
    start_of_day = tz.localize(datetime(now.year, now.month, now.day))
    end_of_day = start_of_day + timedelta(days=1)

    start = int(start_of_day.timestamp())
    stop = int(end_of_day.timestamp())

    logger.debug("URL range: start=%s (%s), stop=%s (%s)", start, start_of_day, stop, end_of_day)
    return f"http://{scada_ip}/cgi-bin/xml/getrep.pl?param={param_id}&start={start}&stop={stop}"


def get_texterior():
    # 1) Быстро отдаём из кэша
    cached = cache.get("texterior:avg_day:v1")
    if cached is not None:
        return cached

    url = build_url_day_range(SCADA_IP, PARAM_ID)

    # 2) Session = keep-alive (быстрее)
    session = requests.Session()

    try:
        response = session.get(url, timeout=TIMEOUT)
        response.raise_for_status()

        root = etree.fromstring(response.content)
        records = root.findall(".//record")
        if not records:
            raise ValueError("Нет <record> в XML")

        values = [
            float(rec.findtext("value"))
            for rec in records
            if rec.findtext("value") not in (None, "N")
        ]
        if not values:
            raise ValueError("Нет валидных значений температуры")

        avg_temp = round(sum(values) / len(values), 1)

        cache.set("texterior:avg_day:v1", avg_temp, TEXTERIOR_CACHE_TTL)
        logger.info("🌡️ Texterior (среднее за сутки) = %s °C", avg_temp)
        return avg_temp

    except (requests.RequestException, ValueError, etree.XMLSyntaxError) as e:
        logger.exception("Ошибка при получении Texterior: %s", e)
        return None
