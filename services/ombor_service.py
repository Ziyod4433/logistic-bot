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

CACHE_TTL_SECONDS = 120  # 2 minutes
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
    from urllib.parse import quote
    # URL-encode the sheet name so tabs with spaces ("Ortilgan furalar") work
    url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        f"/gviz/tq?tqx=out:csv&sheet={quote(sheet_name or '')}"
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
        )
    else:
        url = (
            f"https://docs.google.com/spreadsheets/d/{sheet_id}"
            f"/gviz/tq?tqx=out:csv&sheet={quote(val)}"
        )
    return _fetch_csv_url(url, label="FTL Sheets")


def _fetch_csv_url(url: str, label: str = "Google Sheets") -> list[list[str]]:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
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
                return cached["data"]

    type_idx   = _col_to_index(type_col   or "J")
    date_idx   = _col_to_index(date_col   or "L")
    seller_idx = _col_to_index(seller_col or "AB")

    rows = _fetch_csv_by_gid(sheet_id, gid)
    data_rows = rows[max(0, int(header_rows)):]

    by_seller: dict[str, dict[str, Any]] = {}
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
        diag["rows_used"] += 1
        diag["trucks_total"] += trucks

    diag["trucks_total"] = round(diag["trucks_total"], 2)
    result: dict[str, Any] = {"by_seller": by_seller, "diagnostics": diag}

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
) -> dict[str, Any]:
    """
    Fetch Ombor sheet data, filter by date range, return aggregated CBM by
    seller (SOTUVCHI) AND by logist (LOGIST PLANI READY FOR LOAD).
    Each row contributes to both groupings independently (same CBM counted
    once for seller, once for logist — totals are identical).
    Results cached for CACHE_TTL_SECONDS (2 minutes).
    Empty cells in either column → "Retention".
    """
    cache_key = f"{sheet_id}|{sheet_name}|{cbm_col}|{date_col}|{seller_col}|{logist_col}|{header_rows}|{date_from}|{date_to}"
    now = time.monotonic()

    if not force:
        with _lock:
            cached = _cache.get(cache_key)
            if cached and cached["expires_at"] > now:
                return cached["data"]

    cbm_idx = _col_to_index(cbm_col or "V")
    date_idx = _col_to_index(date_col or "Z")
    seller_idx = _col_to_index(seller_col or "AG")
    logist_idx = _col_to_index(logist_col or "AH")

    rows = _fetch_csv(sheet_id, sheet_name)
    data_rows = rows[max(0, int(header_rows)):]

    sellers: dict[str, dict[str, Any]] = {}
    logists: dict[str, dict[str, Any]] = {}        # NEW: aggregation by logist (col AH)
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

        # Aggregate the same row also by logist (column AH)
        logist = safe_cell(row, logist_idx) or RETENTION_SELLER
        if logist not in logists:
            logists[logist] = {"name": logist, "cbm": 0.0, "bl_count": 0}
        logists[logist]["cbm"] += cbm
        logists[logist]["bl_count"] += 1

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

    logist_list = sorted(logists.values(), key=lambda x: x["cbm"], reverse=True)
    for l in logist_list:
        l["cbm"] = round(l["cbm"], 2)
        l["share_percent"] = round(l["cbm"] / total_cbm * 100 if total_cbm else 0, 1)

    monthly_list = sorted(monthly.values(), key=lambda x: x["month"])
    for m in monthly_list:
        m["cbm"] = round(m["cbm"], 2)

    result: dict[str, Any] = {
        "ok": True,
        "total_cbm": round(total_cbm, 2),
        "total_bl": total_bl,
        "sellers": seller_list,
        "logists": logist_list,                 # NEW: aggregation by logist (col AH)
        "monthly": monthly_list,
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
    # Stage 1 — Ombor (Stage 1 columns CAN be overridden per-plan; the user's
    # ⚙ Sozlash dialog still controls THIS stage. The other two stages always
    # use the hard-coded layout below — they're not currently configurable.)
    # NOTE: For Stage 3 (Yetib keldi) we use column X = "SOTUV XARAJATLARI DATE OF ARRIVE"
    # (the planned arrive date) — NOT AI which is "Yetib kelgan sana" (actual arrival).
    # This keeps the date semantics consistent across all 3 pipeline stages.
    {"sheet_name": "Ortilgan furalar", "cbm_col": "U", "date_col": "Y", "seller_col": "AF", "logist_col": "AG", "header_rows": 1},
    {"sheet_name": "Yetib keldi",      "cbm_col": "T", "date_col": "X", "seller_col": "AE", "logist_col": "AF", "header_rows": 1},
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
) -> dict[str, Any]:
    """Read Stage-1 (Ombor) + Stage-2 (Ortilgan furalar) + Stage-3 (Yetib keldi)
    from the SAME spreadsheet and merge their seller/logist aggregations.

    Each stage uses fetch_ombor_data internally — its caching, diagnostics,
    and date filtering all work per-stage. We then sum CBM and BL counts
    by seller-name across stages.
    """
    stages_to_fetch = [
        {
            "sheet_name": primary_sheet_name,
            "cbm_col":    primary_cbm_col,
            "date_col":   primary_date_col,
            "seller_col": primary_seller_col,
            "logist_col": primary_logist_col,
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
                header_rows=cfg["header_rows"],
                date_from=date_from,
                date_to=date_to,
                force=force,
            )
            per_stage.append((cfg["sheet_name"], r, None))
        except Exception as exc:
            per_stage.append((cfg["sheet_name"], None, str(exc)))

    # ─── Merge by seller/logist name (case-fold + apostrophe-strip) ───
    def _norm(name: str) -> str:
        n = (name or "").casefold()
        for ch in ("ʻ", "ʼ", "'", "`", "‘", "’"):
            n = n.replace(ch, "")
        return " ".join(n.split()).strip()

    sellers_acc: dict[str, dict[str, Any]] = {}
    logists_acc: dict[str, dict[str, Any]] = {}
    monthly_acc: dict[str, dict[str, Any]] = {}
    total_cbm = 0.0
    total_bl = 0

    for _sheet_name, r, _err in per_stage:
        if not r:
            continue
        for s in r.get("sellers", []):
            k = _norm(s["name"])
            if not k:
                continue
            if k not in sellers_acc:
                sellers_acc[k] = {"name": s["name"], "cbm": 0.0, "bl_count": 0}
            sellers_acc[k]["cbm"]      += float(s.get("cbm") or 0)
            sellers_acc[k]["bl_count"] += int(s.get("bl_count") or 0)
        for l in r.get("logists", []):
            k = _norm(l["name"])
            if not k:
                continue
            if k not in logists_acc:
                logists_acc[k] = {"name": l["name"], "cbm": 0.0, "bl_count": 0}
            logists_acc[k]["cbm"]      += float(l.get("cbm") or 0)
            logists_acc[k]["bl_count"] += int(l.get("bl_count") or 0)
        for m in r.get("monthly", []):
            key = m["month"]
            if key not in monthly_acc:
                monthly_acc[key] = {"month": key, "label": m.get("label") or key, "cbm": 0.0, "bl_count": 0}
            monthly_acc[key]["cbm"]      += float(m.get("cbm") or 0)
            monthly_acc[key]["bl_count"] += int(m.get("bl_count") or 0)
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
    for sheet_name, r, err in per_stage:
        if err:
            per_stage_diag[sheet_name] = {"error": err}
        elif r:
            per_stage_diag[sheet_name] = {
                "total_cbm": round(r.get("total_cbm") or 0, 2),
                "total_bl":  r.get("total_bl") or 0,
                "rows_used": (r.get("diagnostics") or {}).get("rows_used"),
                "rows_total": (r.get("diagnostics") or {}).get("rows_total"),
            }

    return {
        "ok": True,
        "total_cbm": round(total_cbm, 2),
        "total_bl": total_bl,
        "sellers": seller_list,
        "logists": logist_list,
        "monthly": monthly_list,
        "fetched_at": datetime.now(TASHKENT_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "diagnostics": {
            "stages_fetched": len([r for _, r, _ in per_stage if r]),
            "stages_failed":  len([e for _, _, e in per_stage if e]),
            "per_stage": per_stage_diag,
        },
    }
