
# МОДУЛЬ: HTTP-клиент для получения временных рядов с приборов LR по протоколу "getrep.pl".
# Хранит карту серверов (IPS -> URL) и одну функцию fetch_xml, которая делает GET-запрос и
# возвращает сырые байты XML без преобразований. Это удобно, потому что дальше парсер сам
# разбирает bytes и не страдает от ошибок перекодировки.

from __future__ import annotations  # поддержка аннотаций типов в ранних версиях Python# нет влияния на рантайм
import requests  # внешняя библиотека для HTTP-запросов# используем для GET

# Сопоставление числового кода сервера (IPS из БД) к базовому URL CGI-скрипта прибора
SERVER_MAP = {
    214: "http://10.1.1.214/cgi-bin/xml/getrep.pl",  # сервер с кодом 214              # URL CGI
    173: "http://10.1.1.173/cgi-bin/xml/getrep.pl",  # сервер с кодом 173              # URL CGI
    242: "http://10.1.1.242/cgi-bin/xml/getrep.pl",  # сервер с кодом 242              # URL CGI
}

def fetch_xml(ips: int, param_id: str, start_epoch: int, stop_epoch: int, timeout: int = 15) -> bytes:
    """
    ФУНКЦИЯ: забирает из прибора сырые XML-данные по одному параметру.
    ВХОД:
      - ips: код сервера (из PTI.IPs), например 214/173/242
      - param_id: идентификатор параметра в LOVATI (строка с буквами+цифрами)
      - start_epoch / stop_epoch: границы интервала в секундах UNIX epoch (локальное время уже конвертировано ранее)
      - timeout: секунд ожидания HTTP-ответа
    ВЫХОД:
      - bytes с XML (без декодирования), чтобы парсер позже сам разобрал кодировку/содержимое.
    Исключения:
      - ValueError, если для ips нет URL в SERVER_MAP
      - requests.HTTPError (через r.raise_for_status()), если HTTP-ответ не 2xx
      - Любые сетевые исключения requests.* (соединение/таймаут и т.д.)
    """
    base = SERVER_MAP.get(int(ips))                    # берём базовый URL по коду сервера
    if not base:                                       # если код не найден в карте —
        raise ValueError(f"Unknown server code: {ips}")# бросаем понятную ошибку конфигурации

    params = {"param": str(param_id),                  # собираем query-параметры запроса:
              "start": int(start_epoch),               #  - идентификатор параметра
              "stop": int(stop_epoch)}                 #  - диапазон времени в epoch (сек)

    r = requests.get(base, params=params, timeout=timeout)  # делаем HTTP GET с таймаутом
    r.raise_for_status()                                  # если код ответа не 2xx — бросит HTTPError
    return r.content  # <-- байты                         # отдаём сырые байты XML без .text