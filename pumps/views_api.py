from __future__ import annotations

from rest_framework.decorators import api_view
from rest_framework.response import Response

from .service import get_pumps_rows


@api_view(["GET"])
def pumps_table_api(request):
    def to_float(q):
        try:
            if q is None or q == "":
                return None
            return float(q)
        except Exception:
            return None

    t2_min = request.query_params.get("t2_min")
    t2_max = request.query_params.get("t2_max")

    t2_min = to_float(t2_min)
    t2_max = to_float(t2_max)

    rows = get_pumps_rows(t2_min=t2_min, t2_max=t2_max)

    payload = []
    for r in rows:
        payload.append({
            "ptc": r.get("ptc"),
            "t2": r.get("t2"),
            "q1": r.get("q1"),
            "t2_alert": r.get("t2_alert"),
            "overall_color": r.get("overall_color"),
            "pompa": r.get("pompa"),
            "pompa_nums": r.get("pompa_nums"),
            "lcs": r.get("lcs"),

            # ссылки
            "url_ptc": r.get("url_ptc"),
            "url_t2": r.get("url_t2"),
            "url_q1": r.get("url_q1"),
            "url_pompa": r.get("url_pompa"),
            "url_pompa2": r.get("url_pompa2"),
            "url_pompa3": r.get("url_pompa3"),
        })

    return Response(payload)
