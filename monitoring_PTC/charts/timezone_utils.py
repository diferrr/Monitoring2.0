# charts/timezone_utils.py
# МОДУЛЬ: вспомогательные функции для времени/часовых поясов.
# Задачи:
#   - Разобрать ISO-строку из фронта и интерпретировать её как локальную (Кишинёв), если нет зоны.
#   - Конвертировать локальное время -> epoch секунд в UTC (как требуют приборы).
#   - Парсить формат времени, который присылают приборы (UTC) в двух вариантах записи.
#   - Возвращать осознанные (aware) datetime в зоне Europe/Chisinau.

from __future__ import annotations                      # поддержка аннотаций в ранних версиях Python
from datetime import datetime, timezone, timedelta      # базовые типы и функции для времени/дат
from zoneinfo import ZoneInfo                           # стандартные таймзоны (Python 3.9+)
import re                                               # регулярные выражения для очистки строк

# наша локальная зона
TZ_CHISINAU = ZoneInfo("Europe/Chisinau")               # фиксируем локальную TZ для проекта
# гринвич
TZ_UTC = timezone.utc                                   # объект таймзоны UTC (встроенный)


def parse_local_iso(s: str) -> datetime:
    """
    Приходит из фронта строка без зоны -> считаем, что это Кишинёв.
    Приходит со смещением -> приводим к Кишинёву.
    """
    s = s.strip()                                       # убираем пробелы по краям
    dt = datetime.fromisoformat(s)                      # парсим ISO-формат (может быть naive/aware)
    if dt.tzinfo is None:                               # если без tz (naive) —
        dt = dt.replace(tzinfo=TZ_CHISINAU)            # считаем это локальным временем Кишинёва
    else:
        dt = dt.astimezone(TZ_CHISINAU)                # иначе переводим в зону Кишинёва
    return dt                                           # возвращаем aware datetime в локальной TZ


def to_epoch_seconds(dt_local: datetime) -> int:
    """
    Локальное время -> UTC -> epoch (секунды).
    Это мы шлём в прибор. Сериям это подходит.
    """
    return int(dt_local.astimezone(TZ_UTC).timestamp()) # переводим локальное время в UTC и берём epoch-секунды


def to_iso(dt_local: datetime) -> str:
    return dt_local.isoformat()                         # удобный helper: вернуть ISO-строку как есть (с tz)


def parse_device_timestamp(raw: str) -> datetime:
    """
    Прибор прислал время в ФОРМАТЕ ПРИБОРА, но В UTC.
    Наша задача:
      1) распарсить цифры;
      2) повесить на них tz=UTC;
      3) перевести в Europe/Chisinau;
      4) вернуть aware-datetime.

    Поддерживаем:
      - 'YYYYMMDDHHMMSS...'  (календарная дата, UTC)
      - 'YYYYJJJHHMMSS...'   (день в году, UTC)
    Лишние символы/милисекунды игнорируем.
    """
    s = re.sub(r"\D", "", raw or "")                    # оставляем только цифры (срезаем пробелы/символы)
    if len(s) < 12:                                     # меньше 12 цифр — точно недостаточно
        raise ValueError("timestamp too short")         # бросаем явную ошибку формата

    # ---- вариант 1: классический YYYYMMDDHHMMSS ----
    s14 = s[:14] if len(s) >= 14 else s.ljust(14, "0")  # берём 14 цифр (или добиваем нулями справа)
    year = int(s14[:4])                                 # год (может не пригодиться, оставляем для читабельности)
    maybe_month = int(s14[4:6])                         # берём предполагаемый месяц (чтобы отличить формат)

    if 1 <= maybe_month <= 12:                          # если похоже на нормальную календарную дату —
        # 1) парсим как UTC
        dt_utc = datetime.strptime(s14[:14], "%Y%m%d%H%M%S").replace(tzinfo=TZ_UTC)  # строим aware UTC datetime
        # 2) переводим в Chisinau
        return dt_utc.astimezone(TZ_CHISINAU)          # возвращаем уже в локальной TZ

    # ---- вариант 2: YYYY + day-of-year + HHMMSS ----
    s13 = s[:13] if len(s) >= 13 else s.ljust(13, "0")  # тут 13 цифр: YYYY JJJ HHMMSS
    year = int(s13[:4])                                 # год
    doy = int(s13[4:7])                                 # порядковый день в году (1..365/366)
    hh = int(s13[7:9])                                  # часы
    mm = int(s13[9:11])                                 # минуты
    ss = int(s13[11:13])                                # секунды

    # дата в UTC
    base_utc = datetime(year, 1, 1, tzinfo=TZ_UTC) + timedelta(days=doy - 1)  # 1 января + (doy-1) дней
    dt_utc = base_utc.replace(hour=hh, minute=mm, second=ss)                  # подставляем время суток

    # и в Chisinau
    return dt_utc.astimezone(TZ_CHISINAU)               # конвертируем UTC -> Europe/Chisinau и возвращаем