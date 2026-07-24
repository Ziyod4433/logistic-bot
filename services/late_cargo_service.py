# -*- coding: utf-8 -*-
"""Late-cargo analysis for the Dashboard "Опаздывают" stat.

Reads the cargo-status spreadsheet (three lifecycle tabs: Ombor →
Ortilgan furalar → Yetib keldi; rows migrate between tabs as the cargo
physically moves) and computes which cargos are LATE against the client
SLA:

    warehouse arrival → loaded onto a truck:   3–6 days
    truck departure   → arrival in Toshkent:  18–25 days
    ──────────────────────────────────────────────────────
    total: a cargo that still hasn't arrived 30+ days after its
    DATE OF ARRIVE (warehouse intake date) counts as late,
    by (elapsed − 30) days.

Columns are located BY HEADER NAME (BRAND NAME / DATE OF ARRIVE /
REYS RAQAMI) because the three tabs share header names but not column
letters, and gviz merges multi-row headers differently per tab.
"""

import os
import threading
import time
from collections import Counter
from datetime import datetime

from services import ombor_service

LATE_CARGO_SHEET_ID = (
    os.environ.get("LATE_CARGO_SHEET_ID", "").strip()
    or "1idVBaJ0xInTRne5cyzbUIXqaG1r4jdhRbunGTinO8jQ"
)

SHEET_OMBOR = "Ombor"
SHEET_TRANSIT = "Ortilgan furalar"
SHEET_ARRIVED = "Yetib keldi"

TOTAL_SLA_DAYS = 30   # warehouse intake → Toshkent; beyond this = late
LOAD_SLA_DAYS = 6     # warehouse intake → loaded onto a truck

CACHE_TTL_SECONDS = 120
_lock = threading.Lock()
_cache: dict[str, dict] = {}


def _norm(value: str) -> str:
    return " ".join(str(value or "").upper().split())


def _find_header(rows: list[list[str]]) -> int:
    """Header row = first of the top rows that mentions BRAND NAME."""
    for i, row in enumerate(rows[:4]):
        if any("BRAND NAME" in _norm(c) for c in row):
            return i
    return 0


def _locate_columns(header: list[str], data_rows: list[list[str]]) -> dict:
    brand_idx = arrive_idx = None
    reys_candidates: list[int] = []
    for j, cell in enumerate(header):
        h = _norm(cell)
        if brand_idx is None and "BRAND NAME" in h:
            brand_idx = j
        if arrive_idx is None and "DATE OF ARRIVE" in h:
            arrive_idx = j
        if "REYS RAQAMI" in h:
            reys_candidates.append(j)
    # Ortilgan furalar has two REYS RAQAMI columns (in-table + helper);
    # keep the one that actually holds data.
    reys_idx = None
    best = 0
    for j in reys_candidates:
        filled = sum(1 for r in data_rows if j < len(r) and r[j].strip())
        if filled > best:
            best, reys_idx = filled, j
    if reys_idx is None and reys_candidates:
        reys_idx = reys_candidates[0]
    return {"brand": brand_idx, "arrive": arrive_idx, "reys": reys_idx}


def _read_stage(sheet_name: str) -> list[dict]:
    """Return the real cargo rows of one tab: parsed arrive date + brand.

    Template/empty rows (no DATE OF ARRIVE) and stray repeated header
    rows are dropped.
    """
    rows = ombor_service._fetch_csv(LATE_CARGO_SHEET_ID, sheet_name)
    if not rows:
        return []
    header_i = _find_header(rows)
    data_rows = rows[header_i + 1:]
    cols = _locate_columns(rows[header_i], data_rows)
    if cols["brand"] is None or cols["arrive"] is None:
        raise ValueError(f"{sheet_name}: BRAND NAME / DATE OF ARRIVE ustunlari topilmadi")
    out = []
    for row in data_rows:
        raw_arrive = row[cols["arrive"]].strip() if cols["arrive"] < len(row) else ""
        arrive = ombor_service._parse_date(raw_arrive)
        if arrive is None:
            continue
        brand = row[cols["brand"]].strip() if cols["brand"] < len(row) else ""
        if _norm(brand) == "BRAND NAME":
            continue
        reys = ""
        if cols["reys"] is not None and cols["reys"] < len(row):
            reys = row[cols["reys"]].strip()
        out.append({"brand": brand or "—", "arrive": arrive, "reys": reys})
    return out


def get_late_cargo_report(force: bool = False) -> dict:
    cache_key = LATE_CARGO_SHEET_ID
    now = time.monotonic()
    if not force:
        with _lock:
            cached = _cache.get(cache_key)
            if cached and cached["expires_at"] > now:
                return cached["data"]

    errors = []
    try:
        ombor_rows = _read_stage(SHEET_OMBOR)
    except Exception as exc:
        ombor_rows = None
        errors.append(f"{SHEET_OMBOR}: {exc}")
    try:
        transit_rows = _read_stage(SHEET_TRANSIT)
    except Exception as exc:
        transit_rows = None
        errors.append(f"{SHEET_TRANSIT}: {exc}")
    try:
        arrived_rows = _read_stage(SHEET_ARRIVED)
    except Exception as exc:
        arrived_rows = []  # non-fatal: only used to filter out copy-lag rows
        errors.append(f"{SHEET_ARRIVED}: {exc}")

    if ombor_rows is None or transit_rows is None:
        return {"ok": False, "error": "; ".join(errors) or "Sheets fetch failed"}

    # Rows migrate Ombor → Ortilgan furalar → Yetib keldi, but a manual
    # process can lag: the copy already exists in the next tab while the
    # original still sits in the previous one. Filter those out so a
    # cargo is only counted at its FURTHEST stage (multiset-aware: two
    # identical cargos on the same date are two real rows).
    arrived_keys = Counter(
        (_norm(r["brand"]), _norm(r["reys"]), r["arrive"].isoformat()) for r in arrived_rows
    )
    active_transit = []
    for r in transit_rows:
        key = (_norm(r["brand"]), _norm(r["reys"]), r["arrive"].isoformat())
        if arrived_keys.get(key, 0) > 0:
            arrived_keys[key] -= 1
            continue
        active_transit.append(r)

    transit_keys = Counter((_norm(r["brand"]), r["arrive"].isoformat()) for r in active_transit)
    active_ombor = []
    for r in ombor_rows:
        key = (_norm(r["brand"]), r["arrive"].isoformat())
        if transit_keys.get(key, 0) > 0:
            transit_keys[key] -= 1
            continue
        active_ombor.append(r)

    today = datetime.now(ombor_service.TASHKENT_TZ).date()

    def enrich(row: dict, stage: str) -> dict:
        days_total = max(0, (today - row["arrive"]).days)
        return {
            "brand": row["brand"],
            "reys": row["reys"],
            "arrived": row["arrive"].strftime("%d.%m.%Y"),
            "days_total": days_total,
            "days_late": max(0, days_total - TOTAL_SLA_DAYS),
            "stage": stage,
        }

    items = [enrich(r, "ombor") for r in active_ombor] + [
        enrich(r, "transit") for r in active_transit
    ]
    late_items = [it for it in items if it["days_late"] > 0]

    # Group late cargo by REYS RAQAMI; warehouse cargo has no reys yet
    # and forms its own group.
    groups_map: dict[str, dict] = {}
    for it in late_items:
        key = "__ombor__" if it["stage"] == "ombor" else (it["reys"] or "__no_reys__")
        g = groups_map.setdefault(key, {"key": key, "reys": it["reys"], "items": []})
        g["items"].append(it)
    groups = []
    for key, g in groups_map.items():
        g["items"].sort(key=lambda x: x["days_late"], reverse=True)
        g["max_late"] = g["items"][0]["days_late"]
        if key == "__ombor__":
            g["label"] = "📦 Omborda — furaga ortilmagan"
        elif key == "__no_reys__":
            g["label"] = "🚛 Reys ko'rsatilmagan"
        else:
            g["label"] = f"🚛 Reys {g['reys']}"
        groups.append(g)
    groups.sort(key=lambda g: g["max_late"], reverse=True)

    # Warehouse loading-SLA watchlist: sitting in Ombor longer than the
    # 6-day loading window but not yet late overall (those go above).
    ombor_warning = sorted(
        (
            {"brand": it["brand"], "arrived": it["arrived"], "days_in_ombor": it["days_total"]}
            for it in items
            if it["stage"] == "ombor" and LOAD_SLA_DAYS < it["days_total"] <= TOTAL_SLA_DAYS
        ),
        key=lambda x: x["days_in_ombor"],
        reverse=True,
    )

    result = {
        "ok": True,
        "generated_at": datetime.now(ombor_service.TASHKENT_TZ).strftime("%d.%m.%Y %H:%M"),
        "threshold_days": TOTAL_SLA_DAYS,
        "load_sla_days": LOAD_SLA_DAYS,
        "total_late": len(late_items),
        "late_in_ombor": sum(1 for it in late_items if it["stage"] == "ombor"),
        "late_in_transit": sum(1 for it in late_items if it["stage"] == "transit"),
        "active_ombor": len(active_ombor),
        "active_transit": len(active_transit),
        "groups": groups,
        "ombor_warning": ombor_warning,
        "warnings": errors,
    }
    with _lock:
        _cache[cache_key] = {"data": result, "expires_at": now + CACHE_TTL_SECONDS}
    return result
