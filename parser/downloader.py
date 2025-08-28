from __future__ import annotations

import datetime as dt
from typing import Iterator, Optional
import requests



BASE = "https://spimex.com/upload/reports/oil_xls/oil_xls_{stamp}.xls"
HDRS = {"User-Agent": "Mozilla/5.0 (spimex-loader)"}

def daterange_days(start: dt.date, stop_exclusive: dt.date) -> Iterator[dt.date]:
    cur = start
    while cur < stop_exclusive:
        yield cur
        cur += dt.timedelta(days=1)

def url_for_day(day: dt.date, time_str: str = "162000") -> str:
    # time_str — "HHMMSS" (ровно 6 цифр)
    return BASE.format(stamp=f"{day:%Y%m%d}{time_str}")

def try_get(url: str, timeout: int = 30) -> Optional[bytes]:
    resp = requests.get(url, headers=HDRS, timeout=timeout)
    if resp.status_code == 200 and resp.content:
        return resp.content
    if resp.status_code in (404, 400):
        return None
    # На прочие коды — тоже None, но выведем для информации
    return None

def iter_daily_files(start: dt.date, end_exclusive: dt.date, time_str: str = "162000") -> Iterator[tuple[dt.date, str, bytes]]:
    """
    Идём по дням, конструируем URL вида oil_xls_YYYYMMDDHHMMSS.xls и
    возвращаем (дата, url, контент) для существующих файлов.
    """
    for day in daterange_days(start, end_exclusive):
        url = url_for_day(day, time_str=time_str)
        payload = try_get(url)
        if payload is None:
            # файл за день отсутствует — пропускаем молча
            continue
        yield day, url, payload
