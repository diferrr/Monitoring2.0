# monitoring_PTC/monitoring/utils.py
from urllib.parse import urlencode
from django.urls import reverse
from django.conf import settings




def chart_url(ptc: str, param: str, start: str | None = None, end: str | None = None, agg: str | None = None) -> str:
    """
    Генерирует ссылку на новую страницу графика.
    Пример: /charts/chart/?pti=3107&param=T1
    """
    base = reverse("charts:chart_page")  # это name из charts/urls.py
    q = {"pti": str(ptc), "param": str(param).upper()}
    if start: q["from"] = start           # 'YYYY-MM-DDTHH:MM'
    if end:   q["to"]   = end
    if agg:   q["agg"]  = agg             # 'detail'|'hour'|'day'
    return f"{base}?{urlencode(q)}"



def get_client_ip(request):
    """IP клиента: X-Forwarded-For (первый) или REMOTE_ADDR."""
    xff = request.META.get('HTTP_X_FORWARDED_FOR')
    if xff:
        return xff.split(',')[0].strip()
    return request.META.get('REMOTE_ADDR', '')

def can_edit_from_request(request) -> bool:
    """Есть ли право редактировать (по IP из settings.EDITORS_IPS)."""
    ip = get_client_ip(request)
    return ip in getattr(settings, 'EDITORS_IPS', set())







