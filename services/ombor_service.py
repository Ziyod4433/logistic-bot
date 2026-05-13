from __future__ import annotations

import csv
import io
import re
import threading
import time
from datetime import date, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

CACHE_TTL_SECONDS = 120  # 2 minutes
RETENTION_SELLER = "Retention"

_lock = threading.Lock()
_cache: dict[str, dict[str, Any]] = {}

MONTH_NAMES = {
    "01": "Yanvar", "02": "Fevral", "03": "Mart", "04": "Aprel",
    "05": "May", "06": "Iyun", "07": "Iyul", "08": "Avgust",
    "09": "Sentabr", "10": "Oktabr", "11": "Noyabr", "12": "Dekabr",
}


def _col_to_index(col: str) -> int:
    """Convert column letter(s) to 0-based index: A→0, Z→25, AA→26, AH→33."""
    col = col.strip().upper()
    result = 0
    for char in col:
        result = result * 26 + (ord(char) - ord("A") + 1)
    return result - 1


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_float(value: Any) -> float:
    text = str(value or "").replace(" ", "").replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", text)
    return float(m.group(0)) if m else 0.0


def _month_label(ym: str) -> str:
    try:
        year, month = ym.split("-")
        return f"{MONTH_NAMES.get(month, month)} {year}"
    except ValueError:
        return ym


def _fetch_csv(sheet_id: str, sheet_name: str) -> list[list[str]]:
    url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        f"/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    )
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urlopen(req, timeout=30) as response:
            raw = response.read().decode("utf-8-sig", errors="replace")
    except HTTPError as exc:
        raise RuntimeError(f"Google Sheets: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"Google Sheets: {exc.reason}") from exc
    return list(csv.reader(io.StringIO(raw)))


def fetch_ombor_data(
    sheet_id: str,
    sheet_name: str,
    cbm_col: str,
    date_col: str,
    seller_col: str,
    header_rows: int,
    date_from: date | None,
    date_to: date | None,
    force: bool = False,
) -> dict[str, Any]:
    """
    Fetch Ombor sheet data, filter by date range, return aggregated CBM by seller.
    Results cached for CACHE_TTL_SECONDS (2 minutes).
    Empty SOTUVCHI cells are assigned to RETENTION_SELLER.
    """
    cache_key = f"{sheet_id}|{sheet_name}|{cbm_col}|{date_col}|{seller_col}|{header_rows}|{date_from}|{date_to}"
    now = time.monotonic()

    if not force:
        with _lock:
            cached = _cache.get(cache_key)
            if cached and cached["expires_at"] > now:
                return cached["data"]

    cbm_idx = _col_to_index(cbm_col or "V")
    date_idx = _col_to_index(date_col or "Z")
    seller_idx = _col_to_index(seller_col or "AG")

    rows = _fetch_csv(sheet_id, sheet_name)
    data_rows = rows[max(0, int(header_rows)):]

    sellers: dict[str, dict[str, Any]] = {}
    monthly: dict[str, dict[str, Any]] = {}
    total_cbm = 0.0
    total_bl = 0
    # Diagnostics: help users debug why their data shows 0%
    diag = {
        "rows_total": len(data_rows),
        "rows_used": 0,
        "rows_no_cbm": 0,           # CBM empty or 0
        "rows_bad_date": 0,         # date unparseable
        "rows_outside_period": 0,   # date OK but outside plan period
        "sample_dates": [],          # up to 5 sample raw dates from data rows
    }

    def safe_cell(row: list[str], idx: int) -> str:
        return row[idx].strip() if idx < len(row) else ""

    for row in data_rows:
        cbm = _parse_float(safe_cell(row, cbm_idx))
        if cbm <= 0:
            diag["rows_no_cbm"] += 1
            continue

        raw_date_cell = safe_cell(row, date_idx)
        if len(diag["sample_dates"]) < 5 and raw_date_cell:
            diag["sample_dates"].append(raw_date_cell)
        row_date = _parse_date(raw_date_cell)
        if row_date is None:
            diag["rows_bad_date"] += 1
            continue
        if (date_from and row_date < date_from) or (date_to and row_date > date_to):
            diag["rows_outside_period"] += 1
            continue

        seller = safe_cell(row, seller_idx) or RETENTION_SELLER

        if seller not in sellers:
            sellers[seller] = {"name": seller, "cbm": 0.0, "bl_count": 0}
        sellers[seller]["cbm"] += cbm
        sellers[seller]["bl_count"] += 1

        if row_date:
            ym = row_date.strftime("%Y-%m")
            if ym not in monthly:
                monthly[ym] = {"month": ym, "label": _month_label(ym), "cbm": 0.0, "bl_count": 0}
            monthly[ym]["cbm"] += cbm
            monthly[ym]["bl_count"] += 1

        total_cbm += cbm
        total_bl += 1

    diag["rows_used"] = total_bl

    seller_list = sorted(sellers.values(), key=lambda x: x["cbm"], reverse=True)
    for s in seller_list:
        s["cbm"] = round(s["cbm"], 2)
        s["share_percent"] = round(s["cbm"] / total_cbm * 100 if total_cbm else 0, 1)

    monthly_list = sorted(monthly.values(), key=lambda x: x["month"])
    for m in monthly_list:
        m["cbm"] = round(m["cbm"], 2)

    result: dict[str, Any] = {
        "ok": True,
        "total_cbm": round(total_cbm, 2),
        "total_bl": total_bl,
        "sellers": seller_list,
        "monthly": monthly_list,
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "diagnostics": diag,
    }

    with _lock:
        _cache[cache_key] = {"data": result, "expires_at": now + CACHE_TTL_SECONDS}

    return result


def invalidate_cache() -> None:
    with _lock:
        _cache.clear()
