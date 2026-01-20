import pyodbc
from django.conf import settings


def get_units():
    dsn = ';'.join(f"{k}={v}" for k, v in settings.SQL_SERVER.items())
    conn = pyodbc.connect(dsn)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT UNIT_ID, UNIT_NAME, UNIT_DESC
        FROM UNITS
        WHERE UNIT_NAME LIKE 'PT_%'
    """)
    rows = cursor.fetchall()
    conn.close()

    return [
        {
            'id': row.UNIT_ID,
            'name': row.UNIT_NAME.replace('PT_', ''),
            'full_name': row.UNIT_NAME,
            'address': row.UNIT_DESC,
        }
        for row in rows
    ]


def get_unit_data(unit_name):
    """
    Получает параметры по конкретному PTC
    """
    dsn = ';'.join(f"{k}={v}" for k, v in settings.SQL_SERVER.items())
    conn = pyodbc.connect(dsn)
    cursor = conn.cursor()

    data = {
        't1': '-', 't2': '-', 'g1': '-', 'g2': '-', 'q': '-', 'dg': '-', 'dg_pct': '-',
        'gacm': '-', 'tacm': '-', 'g_adaos': '-', 'v220': False, 'pump': False, 'time': '-',
    }

    try:
        cursor.execute("""
            SELECT 
                mc.MC_T1_VALUE_INSTANT, mc.MC_T2_VALUE_INSTANT, mc.MC_G1_VALUE_INSTANT, mc.MC_G2_VALUE_INSTANT,
                mc.MC_POWER1_VALUE_INSTANT, mc.MC_CINAVH_VALUE_INSTANT,
                dcx.DCX_TR03_VALUE_INSTANT, dcx.DCX_AI08_VALUE_INSTANT, dcx.DCX_AI02_VALUE_INSTANT,
                pt.PT_MC_GINB_VALUE_INSTANT, dcx.DCX_DTIME_VALUE_INSTANT
            FROM MULTICAL_CURRENT_DATA mc
            LEFT JOIN DCX7600_CURRENT_DATA dcx ON dcx.UNIT_NAME = mc.UNIT_NAME
            LEFT JOIN PT_MC_COMPUTED_DATA pt ON pt.UNIT_NAME = mc.UNIT_NAME
            WHERE mc.UNIT_NAME = ?
        """, (unit_name,))
        row = cursor.fetchone()
        if row:
            t1 = row[0] or 0
            t2 = row[1] or 0
            g1 = row[2] or 0
            g2 = row[3] or 0
            dg = round(g1 - g2, 2)
            dg_pct = round((dg / g1 * 100), 1) if g1 else 0

            data.update({
                't1': t1, 't2': t2, 'g1': g1, 'g2': g2, 'q1': row[4], 'dg': dg, 'dg_pct': dg_pct,
                'gacm': row[5], 'tacm': row[6], 'v220': row[7] > 1.0, 'pump': row[8] > 1.0,
                'g_adaos': row[9], 'time': row[10].strftime("%d.%m.%y %H:%M") if row[10] else '-',
            })
    except Exception as e:
        print("Ошибка SQL:", e)

    conn.close()
    return data
