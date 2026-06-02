from __future__ import annotations

import csv
import io
import re
import threading
import time
from datetime import date, datetime, timezone, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

CACHE_TTL_SECONDS = 30  # 30 sec — only enough to coalesce concurrent
                        # requests within one render. The real "every 2 min"
                        # refresh cadence is the frontend poller, which now
                        # always sends force=true to bypass this cache.
RETENTION_SELLER = "Retention"
TASHKENT_TZ = timezone(timedelta(hours=5))  # Toshkent UTC+5

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


# Month-name lookup for parsing free-text dates like "1 iyun 2026" or
# "01 июн 2026". Keys are normalized (lowercased + stripped trailing dot).
_MONTH_NAME_INDEX: dict[str, int] = {}
def _populate_month_name_index() -> None:
    if _MONTH_NAME_INDEX:
        return
    entries = [
        # English
        (1, ["jan", "january"]),
        (2, ["feb", "february"]),
        (3, ["mar", "march"]),
        (4, ["apr", "april"]),
        (5, ["may"]),
        (6, ["jun", "june"]),
        (7, ["jul", "july"]),
        (8, ["aug", "august"]),
        (9, ["sep", "sept", "september"]),
        (10, ["oct", "october"]),
        (11, ["nov", "november"]),
        (12, ["dec", "december"]),
        # Russian (case-folded)
        (1, ["янв", "январь", "января"]),
        (2, ["фев", "февраль", "февраля"]),
        (3, ["мар", "март", "марта"]),
        (4, ["апр", "апрель", "апреля"]),
        (5, ["май", "мая"]),
        (6, ["июн", "июнь", "июня"]),
        (7, ["июл", "июль", "июля"]),
        (8, ["авг", "август", "августа"]),
        (9, ["сен", "сент", "сентябрь", "сентября"]),
        (10, ["окт", "октябрь", "октября"]),
        (11, ["ноя", "ноябрь", "ноября"]),
        (12, ["дек", "декабрь", "декабря"]),
        # Uzbek (Latin)
        (1, ["yan", "yanvar"]),
        (2, ["fev", "fevral"]),
        (3, ["mar", "mart"]),
        (4, ["apr", "aprel"]),
        (5, ["may"]),
        (6, ["iyn", "iyun"]),
        (7, ["iyl", "iyul"]),
        (8, ["avg", "avgust"]),
        (9, ["sen", "sent", "sentabr", "sentyabr"]),
        (10, ["okt", "oktabr", "oktyabr"]),
        (11, ["noy", "noyabr"]),
        (12, ["dek", "dekabr"]),
        # Uzbek (Cyrillic)
        (1, ["янв", "январ"]),
        (2, ["фев", "феврал"]),
        (3, ["мар", "март"]),
        (4, ["апр", "апрел"]),
        (5, ["май"]),
        (6, ["июн"]),
        (7, ["июл"]),
        (8, ["авг", "август"]),
        (9, ["сен", "сент", "сентябр"]),
        (10, ["окт", "октябр"]),
        (11, ["ноя", "ноябр"]),
        (12, ["дек", "декабр"]),
    ]
    for month, names in entries:
        for n in names:
            _MONTH_NAME_INDEX[n] = month
_populate_month_name_index()


# Excel/Sheets store dates as serial numbers (days since 1899-12-30 for
# Lotus-compat). gviz CSV usually formats them, but if the column type
# is unformatted Number some operators see raw integers in the export.
_EXCEL_EPOCH = date(1899, 12, 30)


def _parse_date(value: Any) -> date | None:
    """Parse a wide range of date representations into a date.

    Supported:
      - Standard numeric formats: DD.MM.YYYY, DD.MM.YY, DD/MM/YYYY,
        DD-MM-YYYY, YYYY-MM-DD, YYYY/MM/DD, MM/DD/YYYY (US).
      - Text dates with month names in EN/RU/UZ-Latn/UZ-Cyrl:
        "1 iyun 2026", "01 июн 2026", "27 may 2026", etc.
      - gviz raw Date format: "Date(2026,5,1)" (month is 0-indexed).
      - Excel/Sheets serial numbers (40000-99999 range, ≈ 2009-2173).
    """
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value or "").strip()
    if not text:
        return None

    # gviz raw "Date(2026,5,1)" — month is 0-indexed in this format.
    m = re.match(r"^\s*Date\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)", text, re.IGNORECASE)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)) + 1, int(m.group(3)))
        except (ValueError, TypeError):
            pass

    # Plain Excel/Sheets serial number. Avoid matching short numbers that
    # could be just a year or a day — require 5 digits (range 10000 →
    # 1927-05-18) which covers any plausible business date.
    if re.fullmatch(r"\d{5,6}(\.\d+)?", text):
        try:
            serial = int(float(text))
            if 10000 < serial < 80000:   # ≈ 1927 .. 2118
                return _EXCEL_EPOCH + timedelta(days=serial)
        except (ValueError, OverflowError):
            pass

    # Strict numeric formats (most common path — fast).
    for fmt in (
        "%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d",
        "%d.%m.%y",                # 01.06.26
        "%Y.%m.%d",                # 2026.06.01
        "%m/%d/%Y",                # 06/01/2026 (US)
    ):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    # Text date with month name: "1 iyun 2026" / "27 may 2026" /
    # "01 июн 2026" / "1-iyun-2026" / "01.iyun.2026" etc.
    txt_match = re.match(
        r"^\s*(\d{1,2})[\s./\-]+([A-Za-zА-Яа-яЁё]+)[\s./\-]+(\d{2,4})\s*$",
        text,
    )
    if txt_match:
        day_s, name, year_s = txt_match.group(1), txt_match.group(2), txt_match.group(3)
        key = name.lower().rstrip(".")
        month = _MONTH_NAME_INDEX.get(key)
        if month is not None:
            try:
                year = int(year_s)
                if year < 100:
                    year = 2000 + year
                return date(year, month, int(day_s))
            except (ValueError, TypeError):
                pass

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


# Apostrophe variants that should collapse to the same person key.
# Uzbek names regularly mix ASCII "'", U+02BB "ʻ", U+02BC "ʼ", U+2018 "'",
# U+2019 "'" and U+0060 "`" depending on the keyboard / paste source.
# Treating them as different sellers fragments a single SOTUVCHI into 2-3
# leaderboard rows, each with a partial CBM total → user sees less than
# the sheet's manual sum for that name.
_APOSTROPHE_VARIANTS = ("ʻ", "ʼ", "'", "`", "‘", "’")


def _normalize_person_key(name: str) -> str:
    """Canonical bucket key for SOTUVCHI / LOGIST names.

    Apostrophe-insensitive (treats "o'g'li", "oʻgʻli", "oʼgʼli", "o`g`li"
    as one identity), case-insensitive, whitespace-collapsing. Used both
    within a single stage and across stages, so a name never splits into
    multiple leaderboard rows.
    """
    n = (name or "").casefold()
    for ch in _APOSTROPHE_VARIANTS:
        n = n.replace(ch, "")
    return " ".join(n.split()).strip()


def _cache_buster() -> str:
    """A query-parameter value that changes every second.

    Google Sheets' `gviz/tq` endpoint is fronted by a CDN that ignores
    most Cache-Control headers and serves stale CSV for tens of minutes.
    Adding a unique query string is the only reliable way to force the
    edge to revalidate against the live spreadsheet.
    """
    return str(int(time.time() * 1000))


def _fetch_csv(sheet_id: str, sheet_name: str) -> list[list[str]]:
    from urllib.parse import quote
    # URL-encode the sheet name so tabs with spaces ("Ortilgan furalar") work.
    # `_=<unix_ms>` busts Google CDN's CSV cache (otherwise edits in the
    # spreadsheet can lag the monitor by 10-30+ minutes).
    url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        f"/gviz/tq?tqx=out:csv&sheet={quote(sheet_name or '')}"
        f"&_={_cache_buster()}"
    )
    return _fetch_csv_url(url, label="Google Sheets")


def _fetch_csv_by_gid(sheet_id: str, gid_or_name: str) -> list[list[str]]:
    """Fetch a sheet CSV by either a numeric gid (619267330) or a sheet tab
    name ("Seliy"). Numeric values use ?gid=…, strings use ?sheet=…."""
    from urllib.parse import quote
    val = (gid_or_name or "").strip()
    if val.isdigit():
        url = (
            f"https://docs.google.com/spreadsheets/d/{sheet_id}"
            f"/gviz/tq?tqx=out:csv&gid={val}"
            f"&_={_cache_buster()}"
        )
    else:
        url = (
            f"https://docs.google.com/spreadsheets/d/{sheet_id}"
            f"/gviz/tq?tqx=out:csv&sheet={quote(val)}"
            f"&_={_cache_buster()}"
        )
    return _fetch_csv_url(url, label="FTL Sheets")


def _fetch_csv_url(url: str, label: str = "Google Sheets") -> list[list[str]]:
    # Belt-and-suspenders no-cache headers. Most CDNs ignore these on GET
    # without auth, but cost nothing to send and occasionally help.
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Cache-Control": "no-cache, no-store, max-age=0",
            "Pragma": "no-cache",
        },
    )
    try:
        with urlopen(req, timeout=30) as response:
            raw = response.read().decode("utf-8-sig", errors="replace")
    except HTTPError as exc:
        raise RuntimeError(f"{label}: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"{label}: {exc.reason}") from exc
    return list(csv.reader(io.StringIO(raw)))


def _truck_count(type_str: str) -> float:
    """Business rule for FTL counting:
       - 20GP, 20HQ → 0.5 truck (two 20s = 1 full truck)
       - 40HQ, 40GP, 45HQ, 96M3, 130M3, 120M3, 145M3, REF FURA, etc → 1 full truck each
       - Anything else (e.g. header text like "Container type", "Container №") → 0
    """
    t = (type_str or "").upper().replace(" ", "")
    if not t:
        return 0.0
    if t in {"20GP", "20HQ"}:
        return 0.5
    # Real container labels always contain a digit (40HQ, 96M3, 130M3, …)
    # or are the special REF FURA marker. Header/junk text returns 0.
    has_digit = any(c.isdigit() for c in t)
    if not has_digit and "FURA" not in t:
        return 0.0
    return 1.0


def fetch_ftl_data(
    sheet_id: str,
    gid: str,
    type_col: str = "J",
    date_col: str = "L",
    seller_col: str = "AB",
    header_rows: int = 1,
    date_from: date | None = None,
    date_to: date | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """
    Fetch FTL (full-truckload) sales sheet. For each row:
      - Parse container type from `type_col` (J)        → truck count (0.5 or 1)
      - Parse date from `date_col` (L)                  → filter by plan period
      - Group by seller name from `seller_col` (AB)
    Returns: { "by_seller": { name: { trucks: float, bl: int } }, "diagnostics": {...} }
    """
    cache_key = f"FTL|{sheet_id}|{gid}|{type_col}|{date_col}|{seller_col}|{header_rows}|{date_from}|{date_to}"
    now = time.monotonic()

    if not force:
        with _lock:
            cached = _cache.get(cache_key)
            if cached and cached["expires_at"] > now:
                data = dict(cached["data"])
                diag = dict(data.get("diagnostics") or {})
                diag["served_from_cache"] = True
                data["diagnostics"] = diag
                return data

    type_idx   = _col_to_index(type_col   or "J")
    date_idx   = _col_to_index(date_col   or "L")
    seller_idx = _col_to_index(seller_col or "AB")

    rows = _fetch_csv_by_gid(sheet_id, gid)
    data_rows = rows[max(0, int(header_rows)):]

    by_seller: dict[str, dict[str, Any]] = {}
    # NEW: by_month_seller — {"YYYY-MM": {seller_name: {trucks, bl}}}.
    # Used for Oylik dinamika click-to-detail popup so we can show which
    # SAVDO/LOGISTIKA sellers contributed trucks per month without
    # refetching Google for every click.
    by_month_seller: dict[str, dict[str, dict[str, Any]]] = {}
    diag = {
        "rows_total": len(data_rows),
        "rows_used": 0,
        "rows_no_type": 0,
        "rows_bad_date": 0,
        "rows_outside_period": 0,
        "rows_no_seller": 0,
        "trucks_total": 0.0,
        "sample_types": [],
    }

    def safe_cell(row: list[str], idx: int) -> str:
        return row[idx].strip() if idx < len(row) else ""

    for row in data_rows:
        type_str = safe_cell(row, type_idx)
        trucks = _truck_count(type_str)
        if len(diag["sample_types"]) < 5 and type_str:
            diag["sample_types"].append(type_str)
        if trucks <= 0:
            diag["rows_no_type"] += 1
            continue

        row_date = _parse_date(safe_cell(row, date_idx))
        if row_date is None:
            diag["rows_bad_date"] += 1
            continue
        if (date_from and row_date < date_from) or (date_to and row_date > date_to):
            diag["rows_outside_period"] += 1
            continue

        seller = safe_cell(row, seller_idx)
        if not seller:
            diag["rows_no_seller"] += 1
            continue

        key = seller.strip()
        if key not in by_seller:
            by_seller[key] = {"name": key, "trucks": 0.0, "bl": 0}
        by_seller[key]["trucks"] += trucks
        by_seller[key]["bl"] += 1

        # Per-(month, seller) breakdown.
        ym = row_date.strftime("%Y-%m")
        month_bucket = by_month_seller.setdefault(ym, {})
        seller_bucket = month_bucket.setdefault(key, {"name": key, "trucks": 0.0, "bl": 0})
        seller_bucket["trucks"] += trucks
        seller_bucket["bl"] += 1

        diag["rows_used"] += 1
        diag["trucks_total"] += trucks

    diag["trucks_total"] = round(diag["trucks_total"], 2)
    diag["served_from_cache"] = False
    # Round trucks to 2dp on the way out so JSON stays compact.
    for s in by_seller.values():
        s["trucks"] = round(s["trucks"], 2)
    for ym_bucket in by_month_seller.values():
        for s in ym_bucket.values():
            s["trucks"] = round(s["trucks"], 2)
    result: dict[str, Any] = {
        "by_seller": by_seller,
        "by_month_seller": by_month_seller,
        "diagnostics": diag,
    }

    with _lock:
        _cache[cache_key] = {"data": result, "expires_at": now + CACHE_TTL_SECONDS}

    return result


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
    logist_col: str = "AH",
    bl_col: str = "",
) -> dict[str, Any]:
    """
    Fetch Ombor sheet data, filter by date range, return aggregated CBM by
    seller (SOTUVCHI) AND by logist (LOGIST PLANI READY FOR LOAD).
    Each row contributes to both groupings independently (same CBM counted
    once for seller, once for logist — totals are identical).
    If bl_col is provided, each row's BL count increments only when that cell
    (BRAND NAME) is non-empty; otherwise every valid row counts as 1 BL.
    Results cached for CACHE_TTL_SECONDS (2 minutes).
    Empty cells in either column → "Retention".
    """
    cache_key = f"{sheet_id}|{sheet_name}|{cbm_col}|{date_col}|{seller_col}|{logist_col}|{bl_col}|{header_rows}|{date_from}|{date_to}"
    now = time.monotonic()

    if not force:
        with _lock:
            cached = _cache.get(cache_key)
            if cached and cached["expires_at"] > now:
                data = dict(cached["data"])
                # Tag the response so the monitor UI can show "served from cache"
                # vs "fresh from Google" if it wants. Doesn't mutate the cached
                # entry — just the copy we return.
                diag = dict(data.get("diagnostics") or {})
                diag["served_from_cache"] = True
                data["diagnostics"] = diag
                return data

    cbm_idx = _col_to_index(cbm_col or "V")
    date_idx = _col_to_index(date_col or "Z")
    seller_idx = _col_to_index(seller_col or "AG")
    logist_idx = _col_to_index(logist_col or "AH")
    bl_idx = _col_to_index(bl_col) if bl_col else None

    rows = _fetch_csv(sheet_id, sheet_name)
    data_rows = rows[max(0, int(header_rows)):]

    sellers: dict[str, dict[str, Any]] = {}
    logists: dict[str, dict[str, Any]] = {}        # NEW: aggregation by logist (col AH)
    monthly: dict[str, dict[str, Any]] = {}
    total_cbm = 0.0
    rows_used = 0
    # BL = UNIQUE BRAND NAME values within the date period.
    # Same BL (same BRAND NAME string) appearing in multiple rows counts ONCE.
    # Tracked globally and per-seller / per-logist / per-month.
    total_bl_set: set[str] = set()
    seller_bl_sets: dict[str, set[str]] = {}
    logist_bl_sets: dict[str, set[str]] = {}
    monthly_bl_sets: dict[str, set[str]] = {}
    # Per-(month, seller_key) breakdown for the click-to-detail Oylik
    # dinamika popup. CBM accumulates; BL is a unique set per bucket.
    monthly_sellers: dict[str, dict[str, dict[str, Any]]] = {}
    monthly_seller_bl_sets: dict[str, dict[str, set[str]]] = {}
    # Diagnostics: help users debug why their data shows 0%
    diag = {
        "rows_total": len(data_rows),
        "rows_used": 0,
        "rows_no_cbm": 0,           # CBM empty or 0
        "rows_bad_date": 0,         # date unparseable
        "rows_outside_period": 0,   # date OK but outside plan period
        "rows_no_bl": 0,             # BRAND NAME (bl_col) empty — counts CBM but not BL
        "sample_dates": [],          # up to 5 raw "date" cells with parse result
        "sample_bad_dates": [],      # up to 5 raw cells that FAILED to parse
        "sample_outside_period": [],  # up to 5 dates we DID parse but rejected as out-of-range
    }

    def safe_cell(row: list[str], idx: int) -> str:
        return row[idx].strip() if idx < len(row) else ""

    for row in data_rows:
        cbm = _parse_float(safe_cell(row, cbm_idx))
        if cbm <= 0:
            diag["rows_no_cbm"] += 1
            continue

        raw_date_cell = safe_cell(row, date_idx)
        row_date = _parse_date(raw_date_cell)
        if len(diag["sample_dates"]) < 5 and raw_date_cell:
            diag["sample_dates"].append({
                "raw": raw_date_cell,
                "parsed": row_date.isoformat() if row_date else None,
            })
        if row_date is None:
            diag["rows_bad_date"] += 1
            if raw_date_cell and len(diag["sample_bad_dates"]) < 5:
                diag["sample_bad_dates"].append(raw_date_cell)
            continue
        if (date_from and row_date < date_from) or (date_to and row_date > date_to):
            diag["rows_outside_period"] += 1
            if len(diag["sample_outside_period"]) < 5:
                diag["sample_outside_period"].append({
                    "raw": raw_date_cell,
                    "parsed": row_date.isoformat(),
                    "window": f"{date_from.isoformat() if date_from else ''}..{date_to.isoformat() if date_to else ''}",
                })
            continue

        # BL identifier — BRAND NAME cell. Empty → this row contributes CBM
        # but no BL (it's a "draft" entry). If bl_col is not configured, every
        # valid row gets a synthetic unique BL id so legacy behaviour is preserved.
        if bl_idx is None:
            bl_id = f"__row_{rows_used}__"          # synthetic unique per row
        else:
            bl_id = safe_cell(row, bl_idx)
            if not bl_id:
                diag["rows_no_bl"] += 1

        rows_used += 1

        # SELLER bucket — keyed by APOSTROPHE-NORMALIZED form so the same
        # person spelled "o'g'li" / "oʻgʻli" / "oʼgʼli" collapses into one
        # row instead of fragmenting CBM across 2-3 buckets.
        raw_seller = safe_cell(row, seller_idx) or RETENTION_SELLER
        seller_key = _normalize_person_key(raw_seller) or RETENTION_SELLER.casefold()
        if seller_key not in sellers:
            sellers[seller_key] = {"name": raw_seller, "cbm": 0.0, "bl_count": 0}
            seller_bl_sets[seller_key] = set()
        elif len(raw_seller) > len(sellers[seller_key]["name"]):
            # Prefer the longest raw spelling we've seen (typically the
            # most complete / properly-typeset version with ʻ rather than ').
            sellers[seller_key]["name"] = raw_seller
        sellers[seller_key]["cbm"] += cbm
        if bl_id:
            seller_bl_sets[seller_key].add(bl_id)

        # LOGIST bucket — same normalization, same rationale.
        raw_logist = safe_cell(row, logist_idx) or RETENTION_SELLER
        logist_key = _normalize_person_key(raw_logist) or RETENTION_SELLER.casefold()
        if logist_key not in logists:
            logists[logist_key] = {"name": raw_logist, "cbm": 0.0, "bl_count": 0}
            logist_bl_sets[logist_key] = set()
        elif len(raw_logist) > len(logists[logist_key]["name"]):
            logists[logist_key]["name"] = raw_logist
        logists[logist_key]["cbm"] += cbm
        if bl_id:
            logist_bl_sets[logist_key].add(bl_id)

        if row_date:
            ym = row_date.strftime("%Y-%m")
            if ym not in monthly:
                monthly[ym] = {"month": ym, "label": _month_label(ym), "cbm": 0.0, "bl_count": 0}
                monthly_bl_sets[ym] = set()
            monthly[ym]["cbm"] += cbm
            if bl_id:
                monthly_bl_sets[ym].add(bl_id)

            # Per-(month, seller_key) detail for click-popup.
            ms_month = monthly_sellers.setdefault(ym, {})
            ms_bl_month = monthly_seller_bl_sets.setdefault(ym, {})
            ms_seller = ms_month.get(seller_key)
            if ms_seller is None:
                ms_seller = {
                    "name": sellers[seller_key]["name"],
                    "seller_key": seller_key,
                    "cbm": 0.0,
                    "bl_count": 0,
                }
                ms_month[seller_key] = ms_seller
                ms_bl_month[seller_key] = set()
            ms_seller["cbm"] += cbm
            # Keep the upgraded display name in sync with the global bucket.
            if len(sellers[seller_key]["name"]) > len(ms_seller["name"]):
                ms_seller["name"] = sellers[seller_key]["name"]
            if bl_id:
                ms_bl_month[seller_key].add(bl_id)

        if bl_id:
            total_bl_set.add(bl_id)

        total_cbm += cbm

    # Convert per-entity BL sets to counts
    for k, v in sellers.items():
        v["bl_count"] = len(seller_bl_sets.get(k, set()))
    for k, v in logists.items():
        v["bl_count"] = len(logist_bl_sets.get(k, set()))
    for k, v in monthly.items():
        v["bl_count"] = len(monthly_bl_sets.get(k, set()))
    # Per-(month, seller) BL counts.
    for ym_key, sellers_in_month in monthly_sellers.items():
        bl_sets_in_month = monthly_seller_bl_sets.get(ym_key, {})
        for skey, sval in sellers_in_month.items():
            sval["bl_count"] = len(bl_sets_in_month.get(skey, set()))
            sval["cbm"] = round(sval["cbm"], 2)
    total_bl = len(total_bl_set)
    diag["rows_used"] = rows_used

    seller_list = sorted(sellers.values(), key=lambda x: x["cbm"], reverse=True)
    for s in seller_list:
        s["cbm"] = round(s["cbm"], 2)
        s["share_percent"] = round(s["cbm"] / total_cbm * 100 if total_cbm else 0, 1)

    logist_list = sorted(logists.values(), key=lambda x: x["cbm"], reverse=True)
    for l in logist_list:
        l["cbm"] = round(l["cbm"], 2)
        l["share_percent"] = round(l["cbm"] / total_cbm * 100 if total_cbm else 0, 1)

    monthly_list = sorted(monthly.values(), key=lambda x: x["month"])
    for m in monthly_list:
        m["cbm"] = round(m["cbm"], 2)

    # Mark this response as a *fresh* fetch from Google (vs served-from-cache,
    # which is tagged in the cache-hit branch above).
    diag["served_from_cache"] = False
    result: dict[str, Any] = {
        "ok": True,
        "total_cbm": round(total_cbm, 2),
        "total_bl": total_bl,
        "sellers": seller_list,
        "logists": logist_list,                 # NEW: aggregation by logist (col AH)
        "monthly": monthly_list,
        # NEW: per-(month, seller_key) breakdown for click-popup.
        # Shape: {"2026-05": {seller_key: {"name", "seller_key", "cbm", "bl_count"}}}
        "monthly_sellers": monthly_sellers,
        "fetched_at": datetime.now(TASHKENT_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "diagnostics": diag,
    }

    with _lock:
        _cache[cache_key] = {"data": result, "expires_at": now + CACHE_TTL_SECONDS}

    return result


def invalidate_cache() -> None:
    with _lock:
        _cache.clear()


# ──────────────────────────────────────────────────────────────────────────
# Multi-stage LTL pipeline:
#   Stage 1 — Ombor             (newly recorded, awaiting load)
#   Stage 2 — Ortilgan furalar  (loaded on trucks)
#   Stage 3 — Yetib keldi       (delivered/arrived)
#
# Each tab has DIFFERENT column letters. The schema below maps them all.
# Aggregation is by seller name (case-fold + apostrophe-strip) across stages.
# ──────────────────────────────────────────────────────────────────────────
LTL_PIPELINE_STAGES = (
    # Stage 1 — Ombor (columns CAN be overridden per-plan via ⚙ Sozlash).
    # Stages 2 & 3 are fixed: same business meaning, different column letters per tab.
    # BL = BRAND NAME column (user-confirmed):  Ombor E · Ortilgan D · Yetib C
    # DATE: same business meaning across all 3 — "DATE OF ARRIVE"
    #       (NOT AI "Yetib kelgan sana" which is actual arrival; we use planned X.)
    {"sheet_name": "Ortilgan furalar", "cbm_col": "U", "date_col": "Y", "seller_col": "AF", "logist_col": "AG", "bl_col": "D", "header_rows": 1},
    {"sheet_name": "Yetib keldi",      "cbm_col": "T", "date_col": "X", "seller_col": "AE", "logist_col": "AF", "bl_col": "C", "header_rows": 1},
)


def fetch_combined_ltl_data(
    sheet_id: str,
    primary_sheet_name: str,
    primary_cbm_col: str,
    primary_date_col: str,
    primary_seller_col: str,
    primary_logist_col: str,
    primary_header_rows: int,
    date_from: date | None,
    date_to: date | None,
    force: bool = False,
    primary_bl_col: str = "E",
) -> dict[str, Any]:
    """Read Stage-1 (Ombor) + Stage-2 (Ortilgan furalar) + Stage-3 (Yetib keldi)
    from the SAME spreadsheet and merge their seller/logist aggregations.

    Each stage uses fetch_ombor_data internally — its caching, diagnostics,
    and date filtering all work per-stage. We then sum CBM and BL counts
    by seller-name across stages. BL count = rows with non-empty BRAND NAME
    in the configured bl_col (Ombor E · Ortilgan D · Yetib C).
    """
    stages_to_fetch = [
        {
            "sheet_name": primary_sheet_name,
            "cbm_col":    primary_cbm_col,
            "date_col":   primary_date_col,
            "seller_col": primary_seller_col,
            "logist_col": primary_logist_col,
            "bl_col":     primary_bl_col,
            "header_rows": primary_header_rows,
        },
        *LTL_PIPELINE_STAGES,
    ]

    per_stage: list[tuple[str, dict[str, Any] | None, str | None]] = []
    for cfg in stages_to_fetch:
        try:
            r = fetch_ombor_data(
                sheet_id=sheet_id,
                sheet_name=cfg["sheet_name"],
                cbm_col=cfg["cbm_col"],
                date_col=cfg["date_col"],
                seller_col=cfg["seller_col"],
                logist_col=cfg["logist_col"],
                bl_col=cfg.get("bl_col", ""),
                header_rows=cfg["header_rows"],
                date_from=date_from,
                date_to=date_to,
                force=force,
            )
            per_stage.append((cfg["sheet_name"], r, None))
        except Exception as exc:
            per_stage.append((cfg["sheet_name"], None, str(exc)))

    # Merge by SELLER / LOGIST name across stages — uses the same
    # apostrophe-collapsing canonicalization as inside a single stage so
    # the bucket is consistent end-to-end.
    sellers_acc: dict[str, dict[str, Any]] = {}
    logists_acc: dict[str, dict[str, Any]] = {}
    monthly_acc: dict[str, dict[str, Any]] = {}
    # Per-(month, seller_key) breakdown across stages.
    monthly_sellers_acc: dict[str, dict[str, dict[str, Any]]] = {}
    total_cbm = 0.0
    total_bl = 0

    for _sheet_name, r, _err in per_stage:
        if not r:
            continue
        for s in r.get("sellers", []):
            k = _normalize_person_key(s["name"])
            if not k:
                continue
            if k not in sellers_acc:
                sellers_acc[k] = {"name": s["name"], "cbm": 0.0, "bl_count": 0}
            elif len(s["name"]) > len(sellers_acc[k]["name"]):
                # Prefer the longest raw spelling across stages, just like
                # within a stage. Keeps "o'g'li" → "oʻgʻli" upgrade behavior.
                sellers_acc[k]["name"] = s["name"]
            sellers_acc[k]["cbm"]      += float(s.get("cbm") or 0)
            sellers_acc[k]["bl_count"] += int(s.get("bl_count") or 0)
        for l in r.get("logists", []):
            k = _normalize_person_key(l["name"])
            if not k:
                continue
            if k not in logists_acc:
                logists_acc[k] = {"name": l["name"], "cbm": 0.0, "bl_count": 0}
            elif len(l["name"]) > len(logists_acc[k]["name"]):
                logists_acc[k]["name"] = l["name"]
            logists_acc[k]["cbm"]      += float(l.get("cbm") or 0)
            logists_acc[k]["bl_count"] += int(l.get("bl_count") or 0)
        for m in r.get("monthly", []):
            key = m["month"]
            if key not in monthly_acc:
                monthly_acc[key] = {"month": key, "label": m.get("label") or key, "cbm": 0.0, "bl_count": 0}
            monthly_acc[key]["cbm"]      += float(m.get("cbm") or 0)
            monthly_acc[key]["bl_count"] += int(m.get("bl_count") or 0)
        # Merge per-(month, seller) breakdown.
        for ym_key, sellers_in_ym in (r.get("monthly_sellers") or {}).items():
            ms_target = monthly_sellers_acc.setdefault(ym_key, {})
            for raw_skey, sval in sellers_in_ym.items():
                norm_skey = _normalize_person_key(sval.get("name") or raw_skey)
                if not norm_skey:
                    continue
                target = ms_target.get(norm_skey)
                if target is None:
                    target = {
                        "name": sval.get("name") or raw_skey,
                        "seller_key": norm_skey,
                        "cbm": 0.0,
                        "bl_count": 0,
                    }
                    ms_target[norm_skey] = target
                elif len(str(sval.get("name") or "")) > len(target["name"]):
                    target["name"] = sval.get("name") or target["name"]
                target["cbm"]      += float(sval.get("cbm") or 0)
                target["bl_count"] += int(sval.get("bl_count") or 0)
        total_cbm += float(r.get("total_cbm") or 0)
        total_bl  += int(r.get("total_bl") or 0)

    seller_list = sorted(sellers_acc.values(), key=lambda x: x["cbm"], reverse=True)
    for s in seller_list:
        s["share_percent"] = round(s["cbm"] / total_cbm * 100, 1) if total_cbm else 0
        s["cbm"] = round(s["cbm"], 2)

    logist_list = sorted(logists_acc.values(), key=lambda x: x["cbm"], reverse=True)
    for l in logist_list:
        l["share_percent"] = round(l["cbm"] / total_cbm * 100, 1) if total_cbm else 0
        l["cbm"] = round(l["cbm"], 2)

    monthly_list = sorted(monthly_acc.values(), key=lambda x: x["month"])
    for m in monthly_list:
        m["cbm"] = round(m["cbm"], 2)

    # Per-stage diagnostics so the user can see what each tab contributed
    per_stage_diag: dict[str, Any] = {}
    any_fresh = False        # did *any* stage actually hit Google this call?
    all_cached = True        # did *every* successful stage come from cache?
    for sheet_name, r, err in per_stage:
        if err:
            per_stage_diag[sheet_name] = {"error": err}
        elif r:
            stage_diag = r.get("diagnostics") or {}
            cached = bool(stage_diag.get("served_from_cache"))
            if not cached:
                any_fresh = True
            else:
                # we only count cache-misses against "all_cached"; this stage hit cache
                pass
            if not cached:
                all_cached = False
            per_stage_diag[sheet_name] = {
                "total_cbm": round(r.get("total_cbm") or 0, 2),
                "total_bl":  r.get("total_bl") or 0,
                "rows_used": stage_diag.get("rows_used"),
                "rows_total": stage_diag.get("rows_total"),
                "rows_no_cbm":     stage_diag.get("rows_no_cbm"),
                "rows_bad_date":   stage_diag.get("rows_bad_date"),
                "rows_outside_period": stage_diag.get("rows_outside_period"),
                "rows_no_bl":      stage_diag.get("rows_no_bl"),
                "sample_dates":    stage_diag.get("sample_dates"),
                "sample_bad_dates": stage_diag.get("sample_bad_dates"),
                "sample_outside_period": stage_diag.get("sample_outside_period"),
                "fetched_at": r.get("fetched_at"),
                "served_from_cache": cached,
            }

    # Use the most-recent stage fetched_at as the combined timestamp so the
    # monitor's "last_updated" reflects when data was actually pulled from
    # Google — NOT just `datetime.now()`. Falls back to now() if everything
    # failed.
    latest_fetched_at = ""
    for _sheet_name, r, _err in per_stage:
        if r:
            ts = r.get("fetched_at") or ""
            if ts and ts > latest_fetched_at:
                latest_fetched_at = ts
    if not latest_fetched_at:
        latest_fetched_at = datetime.now(TASHKENT_TZ).strftime("%Y-%m-%d %H:%M:%S")

    # Round CBM in the merged monthly_sellers map (it's been summed across
    # stages so the floats are now ugly).
    for ym_key, sellers_map in monthly_sellers_acc.items():
        for sval in sellers_map.values():
            sval["cbm"] = round(sval["cbm"], 2)

    return {
        "ok": True,
        "total_cbm": round(total_cbm, 2),
        "total_bl": total_bl,
        "sellers": seller_list,
        "logists": logist_list,
        "monthly": monthly_list,
        "monthly_sellers": monthly_sellers_acc,
        "fetched_at": latest_fetched_at,
        "diagnostics": {
            "stages_fetched": len([r for _, r, _ in per_stage if r]),
            "stages_failed":  len([e for _, _, e in per_stage if e]),
            "any_fresh": any_fresh,           # at least one stage hit Google
            "all_cached": bool(all_cached and per_stage_diag),  # everything served from cache
            "per_stage": per_stage_diag,
        },
    }
