# charts/xml_parser.py
# МОДУЛЬ: парсинг XML, который присылают приборы LR.
# Задача: превратить сырые XML-байты/строку в ОТСОРТИРОВАННЫЙ список точек
#         вида [(ISO-время, значение_float), ...], где ISO — локальное время Europe/Chisinau.
# Использует parse_device_timestamp для корректной интерпретации форматов времени прибора (UTC → Chisinau).

from __future__ import annotations
from typing import List, Tuple, Union
import xml.etree.ElementTree as ET

from .timezone_utils import parse_device_timestamp


def parse_series(xml: Union[str, bytes]) -> List[Tuple[str, float]]:
    """
    ФУНКЦИЯ: принять сырой XML (bytes/str) и вернуть отсортированный список точек:
        [("2025-03-01T00:00:00+02:00", 57.7), ...]
    Берём ВСЕ <record> из XML (ничего не отбрасываем специально).
    """
    if isinstance(xml, bytes):                               # если пришли байты (как из requests), а не строка —
        xml = xml.decode("utf-8", "ignore")                  # декодируем в UTF-8, игнорируя битые символы

    try:
        root = ET.fromstring(xml)                            # пробуем распарсить XML в ElementTree
    except Exception:
        # если прибор прислал мусор/обрезанный XML — вернуть пустой список безопаснее
        return []                                            # пустой результат без падения

    points: List[Tuple[str, float]] = []                     # сюда будем накапливать пары (iso_timestamp, value)

    # В типовом XML записи лежат под путём report_data/record, но берём в общем виде ".//record"
    for rec in root.findall(".//record"):                    # обходим все теги <record> где бы они ни находились
        # 1) время (timestamp)
        ts_raw = rec.get("round_time")                       # сначала пробуем атрибут round_time у <record>
        if not ts_raw:                                       # если его нет или он пустой —
            rt = rec.findtext("real_time")                   # пробуем взять текст из вложенного <real_time>
            if rt:
                ts_raw = rt.strip()                          # нормализуем пробелы

        # 2) значение (value)
        val_raw = rec.findtext("value")                      # значение обычно лежит в элементе <value>
        if not ts_raw or not val_raw:                        # если нет времени или значения —
            continue                                         # пропускаем эту запись

        try:
            dt = parse_device_timestamp(ts_raw.strip())      # парсим формат времени прибора (UTC → Chisinau aware)
            val = float(val_raw.strip())                     # приводим значение к float
        except Exception:
            # если время или число не парсятся — пропускаем только эту запись (остальные продолжаем)
            continue

        points.append((dt.isoformat(), val))                 # добавляем пару (ISO-строка локального времени, float)

    # ОБЯЗАТЕЛЬНО сортируем по времени, чтобы фронт не зависел от исходного порядка <record>
    points.sort(key=lambda x: x[0])                          # сортировка лексикографически по ISO-времени
    return points                                            # отдаём готовый список точек