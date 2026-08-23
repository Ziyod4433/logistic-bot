# -*- coding: utf-8 -*-
"""«Fura statuslari» tab of the status spreadsheet.

Per batch (reys) the logists track:
    C  REYS NOMERI       — "BL<DDMMYYYY>" = the batch (its name date is
                           the WAREHOUSE DEPARTURE date)
    V  BOJXONAGA TUSHDI  — arrival in Tashkent; same event as the live
                           "Toshkent(Chuqursoy ULS da)" status
    W  YUKLAR TARQATILDI — handed out to clients; once this date appears
                           the batch must automatically become INACTIVE

Headers are located by name substrings (gviz merges the multi-row
header unpredictably), with a fallback to the fixed C/V/W letters.
"""

import os
import re
import threading
import time

from services import ombor_service

FURA_STATUS_SHEET_ID = (
    os.environ.get("FURA_STATUS_SHEET_ID", "").strip()
    or "1idVBaJ0xInTRne5cyzbUIXqaG1r4jdhRbunGTinO8jQ"
)
FURA_STATUS_TAB = os.environ.get("FURA_STATUS_TAB", "").strip() or "Fura statuslari"

CACHE_TTL_SECONDS = 240
_lock = threading.Lock()
_cache: dict = {}

_REYS_RE = re.compile(r"BL\s*-?\s*(\d{8})")


def warehouse_suffix(text: str) -> str:
    """YIWU | ZH | '' — склад из хвоста имени партии/рейса
    («14.08.2026 YIWU», «BL14082026 ZH», «BL01072026-YW»)."""
    tail = str(text or "").upper()
    if re.search(r"\b(YIWU|YW)\b", tail):
        return "YIWU"
    if re.search(r"\b(ZHONGSHAN|ZH)\b", tail):
        return "ZH"
    return ""


def _find_col(header: list, needle: str, fallback: int) -> int:
    needle = needle.upper()
    for j, cell in enumerate(header):
        if needle in str(cell or "").upper():
            return j
    return fallback


def get_fura_statuses(force: bool = False) -> dict:
    """{ok, records: {ddmmyyyy: {reys, bojxona, tarqatildi}}} — dates as
    datetime.date or None."""
    now = time.monotonic()
    with _lock:
        cached = _cache.get("data")
        if not force and cached and cached["expires_at"] > now:
            return cached["value"]

    try:
        rows = ombor_service._fetch_csv(FURA_STATUS_SHEET_ID, FURA_STATUS_TAB)
    except Exception as exc:
        return {"ok": False, "error": str(exc), "records": {}}
    if not rows:
        return {"ok": False, "error": "Лист пуст", "records": {}}

    header = rows[0]
    reys_col = _find_col(header, "REYS NOMERI", 2)
    bojxona_col = _find_col(header, "BOJXONAGA TUSHDI", 21)
    tarqatildi_col = _find_col(header, "YUKLAR TARQATILDI", 22)

    records: dict = {}
    records_all: dict = {}
    for row in rows[1:]:
        reys_raw = row[reys_col].strip() if reys_col < len(row) else ""
        match = _REYS_RE.search(reys_raw.upper())
        if not match:
            continue  # маршрутные заголовки типа "QOZOQ CHEGARASI" и пустые
        key = match.group(1)
        bojxona = ombor_service._parse_date(row[bojxona_col].strip()) if bojxona_col < len(row) else None
        tarqatildi = ombor_service._parse_date(row[tarqatildi_col].strip()) if tarqatildi_col < len(row) else None
        rec = {
            "reys": reys_raw,
            "bojxona": bojxona,
            "tarqatildi": tarqatildi,
            # суффикс склада в номере рейса («BL14082026 ZH», «BL01072026-YW»,
            # «YIWU BL14082026»): две партии одной даты различаются только им
            "suffix": warehouse_suffix(reys_raw),
        }
        # последняя строка по данному рейсу побеждает (обычно уникальны)
        records[key] = rec
        records_all.setdefault(key, []).append(rec)

    value = {"ok": True, "records": records, "records_all": records_all, "count": len(records)}
    with _lock:
        _cache["data"] = {"value": value, "expires_at": time.monotonic() + CACHE_TTL_SECONDS}
    return value
