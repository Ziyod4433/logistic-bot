from __future__ import annotations

import re

from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import Any, Callable

import database as db
from services import analytics_importer

BASE_CURRENCY = "USD"
DELAY_THRESHOLD_DAYS = 25
PLAN_METRIC_LABELS = {
    "amount_usd": "USD",
    "cbm": "m³",
    "bl_count": "BL",
}
PLAN_METRIC_LABELS["cbm"] = "m³"
MONTH_NAMES = {
    "01": "Yanvar",
    "02": "Fevral",
    "03": "Mart",
    "04": "Aprel",
    "05": "May",
    "06": "Iyun",
    "07": "Iyul",
    "08": "Avgust",
    "09": "Sentabr",
    "10": "Oktabr",
    "11": "Noyabr",
    "12": "Dekabr",
}
STATUS_BUCKETS = {
    "xitoy": "Xitoy",
    "yiwu": "Xitoy",
    "zhongshan": "Xitoy",
    "horgos": "Horgos",
    "horgos (qozoq)": "Horgos",
    "nurjo'li": "Qozog'iston",
    "nurjo‘li": "Qozog'iston",
    "jarkent": "Qozog'iston",
    "almata": "Qozog'iston",
    "taraz": "Qozog'iston",
    "shimkent": "Qozog'iston",
    "qonusbay": "Qozog'iston",
    "saryagash": "Qozog'iston",
    "yallama": "Yallama",
    "toshkent": "Toshkent",
    "toshkent(chuqursoy uls da)": "Toshkent",
    "bojxona": "Chuqursoy / bojxona",
    "chuqursoy": "Chuqursoy / bojxona",
    "dostlik": "Chuqursoy / bojxona",
    "andijon": "Chuqursoy / bojxona",
}


def _clean_text(value: Any) -> str:
    return str(value or "").replace("\u00a0", " ").strip()


def _safe_lower(value: Any) -> str:
    return _clean_text(value).lower()


def _to_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(round(float(value or 0)))
    except (TypeError, ValueError):
        return 0


def _parse_date(value: Any) -> date | None:
    raw = _clean_text(value)
    if not raw:
        return None
    for fmt in (
        "%d.%m.%Y",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y %H:%M:%S",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _date_to_str(value: date | None) -> str:
    return value.strftime("%d.%m.%Y") if value else ""


def _month_key(value: Any) -> str:
    parsed = _parse_date(value)
    return parsed.strftime("%Y-%m") if parsed else ""


def _month_label(month_key: str) -> str:
    if not month_key:
        return "—"
    year, month = month_key.split("-")
    return f"{MONTH_NAMES.get(month, month)} {year}"


def _format_number(value: float) -> str:
    numeric = float(value or 0)
    if abs(numeric - round(numeric)) < 0.00001:
        return f"{int(round(numeric)):,}".replace(",", " ")
    return f"{numeric:,.2f}".replace(",", " ").rstrip("0").rstrip(".")


def _format_money(value: float, currency: str = BASE_CURRENCY) -> str:
    return f"{_format_number(value)} {currency}".strip()


def _round(value: float, digits: int = 2) -> float:
    return round(float(value or 0), digits)


def _percent_change(current: float, previous: float) -> float | None:
    if abs(previous) < 0.00001:
        return None
    return ((current - previous) / previous) * 100.0


def _daterange_from_preset(preset: str, date_from_raw: str, date_to_raw: str) -> tuple[date | None, date | None]:
    today = datetime.now().date()
    mode = _clean_text(preset or "month").lower()
    if mode == "today":
        return today, today
    if mode == "week":
        return today - timedelta(days=today.weekday()), today
    if mode == "month":
        return today.replace(day=1), today
    if mode == "year":
        return today.replace(month=1, day=1), today
    if mode == "custom":
        return _parse_date(date_from_raw), _parse_date(date_to_raw)
    return today.replace(day=1), today


def _coerce_bounds(start: date | None, end: date | None) -> tuple[date | None, date | None]:
    if start and end and end < start:
        return end, start
    return start, end


@dataclass
class AnalyticsFilters:
    preset: str
    date_from: date | None
    date_to: date | None
    sales_plan_id: int | None
    manager: str
    logist: str
    client: str
    bl_code: str
    reys_number: str
    fura_number: str
    status: str
    currency: str
    bank_or_cash: str
    category: str
    warehouse: str


def parse_filters(args: Any) -> AnalyticsFilters:
    preset = _clean_text(args.get("period") or "month").lower()
    date_from, date_to = _daterange_from_preset(preset, args.get("date_from"), args.get("date_to"))
    date_from, date_to = _coerce_bounds(date_from, date_to)
    try:
        sales_plan_id = int(args.get("sales_plan_id")) if args.get("sales_plan_id") else None
    except (TypeError, ValueError):
        sales_plan_id = None
    return AnalyticsFilters(
        preset=preset,
        date_from=date_from,
        date_to=date_to,
        sales_plan_id=sales_plan_id,
        manager=_clean_text(args.get("manager") or args.get("salesperson")),
        logist=_clean_text(args.get("logist")),
        client=_clean_text(args.get("client")),
        bl_code=_clean_text(args.get("bl_code")),
        reys_number=_clean_text(args.get("reys_number")),
        fura_number=_clean_text(args.get("fura")),
        status=_clean_text(args.get("status")),
        currency=_clean_text(args.get("currency")).upper(),
        bank_or_cash=_clean_text(args.get("bank_or_cash")),
        category=_clean_text(args.get("category")),
        warehouse=_clean_text(args.get("warehouse")),
    )


def _filters_without_dates(filters: AnalyticsFilters) -> AnalyticsFilters:
    return replace(filters, date_from=None, date_to=None)


def _fetch_table(table_name: str) -> list[dict[str, Any]]:
    conn = db.get_conn()
    try:
        rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _load_dataset() -> dict[str, list[dict[str, Any]]]:
    return {
        "sales": _fetch_table("analytics_sales_records"),
        "cashflow": _fetch_table("analytics_cashflow_records"),
        "rates": _fetch_table("analytics_currency_rates"),
        "logists": _fetch_table("analytics_logist_assignments"),
        "shipments": _fetch_table("analytics_shipment_summary"),
        "statuses": _fetch_table("analytics_shipment_statuses"),
        "plans": _fetch_table("analytics_sales_plans"),
        "sync_logs": _fetch_table("analytics_sync_logs"),
    }


def _active_plan(plans: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not plans:
        return None
    active = [plan for plan in plans if _to_int(plan.get("is_active")) == 1]
    pool = active or plans
    pool.sort(
        key=lambda item: (
            _parse_date(item.get("period_start")) or date.min,
            _to_int(item.get("id")),
        ),
        reverse=True,
    )
    return pool[0] if pool else None


def _get_selected_plan(filters: AnalyticsFilters, plans: list[dict[str, Any]]) -> dict[str, Any] | None:
    if filters.sales_plan_id:
        for plan in plans:
            if _to_int(plan.get("id")) == filters.sales_plan_id:
                return plan
    return _active_plan(plans)


def _apply_plan_dates(filters: AnalyticsFilters, plan: dict[str, Any] | None) -> AnalyticsFilters:
    if not plan:
        return filters
    return replace(
        filters,
        date_from=_parse_date(plan.get("period_start")) or filters.date_from,
        date_to=_parse_date(plan.get("period_end")) or filters.date_to,
    )


def _date_match(raw_value: Any, filters: AnalyticsFilters) -> bool:
    parsed = _parse_date(raw_value)
    if filters.date_from and (parsed is None or parsed < filters.date_from):
        return False
    if filters.date_to and (parsed is None or parsed > filters.date_to):
        return False
    return True


def _latest_status_map(status_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in status_rows:
        reys = _clean_text(row.get("reys_number"))
        if not reys:
            continue
        current = latest.get(reys)
        current_date = _parse_date(current.get("status_date")) if current else None
        row_date = _parse_date(row.get("status_date"))
        if current is None:
            latest[reys] = row
            continue
        if row_date and (current_date is None or row_date >= current_date):
            latest[reys] = row
    return latest


def _shipment_map(shipment_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    for row in shipment_rows:
        reys = _clean_text(row.get("reys_number"))
        if reys:
            mapped[reys] = row
    return mapped


def _logists_by_reys(logist_rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    mapped: dict[str, list[str]] = defaultdict(list)
    for row in logist_rows:
        reys = _clean_text(row.get("reys_number"))
        name = _clean_text(row.get("logist_name"))
        if reys and name and name not in mapped[reys]:
            mapped[reys].append(name)
    return mapped


def _normalize_status_bucket(status: str) -> str:
    raw = _safe_lower(status)
    if not raw:
        return ""
    return STATUS_BUCKETS.get(raw, _clean_text(status))


def _resolve_shipment_status(shipment_row: dict[str, Any] | None, latest_status_row: dict[str, Any] | None) -> str:
    if latest_status_row and _clean_text(latest_status_row.get("status")):
        return _clean_text(latest_status_row.get("status"))
    row = shipment_row or {}
    if _clean_text(row.get("distributed_date")):
        return "Yetib keldi"
    if _clean_text(row.get("customs_date")):
        return "Chuqursoy / bojxona"
    if _clean_text(row.get("tashkent_date")):
        return "Toshkent(Chuqursoy ULS da)"
    if _clean_text(row.get("kazakh_truck_date")):
        return "Yallama"
    if _clean_text(row.get("horgos_date")):
        return "Horgos"
    if _clean_text(row.get("loaded_date")):
        return "Xitoy"
    return ""


def _sales_row_matches(
    row: dict[str, Any],
    filters: AnalyticsFilters,
    latest_statuses: dict[str, dict[str, Any]],
    shipment_by_reys: dict[str, dict[str, Any]],
    logists_by_reys_map: dict[str, list[str]],
) -> bool:
    reys = _clean_text(row.get("reys_number"))
    latest_status = latest_statuses.get(reys)
    shipment = shipment_by_reys.get(reys)

    if not _date_match(row.get("sale_date") or row.get("invoice_date"), filters):
        return False
    if filters.manager and _safe_lower(row.get("salesperson")) != _safe_lower(filters.manager):
        return False
    if filters.client and _safe_lower(row.get("client_name")) != _safe_lower(filters.client):
        return False
    if filters.bl_code and _safe_lower(row.get("shipping_mark")) != _safe_lower(filters.bl_code):
        return False
    if filters.reys_number and _safe_lower(reys) != _safe_lower(filters.reys_number):
        return False
    if filters.logist:
        logists = [_safe_lower(item) for item in logists_by_reys_map.get(reys, [])]
        if _safe_lower(filters.logist) not in logists:
            return False
    if filters.status:
        status = _resolve_shipment_status(shipment, latest_status)
        if _safe_lower(_normalize_status_bucket(status)) != _safe_lower(_normalize_status_bucket(filters.status)):
            return False
    if filters.fura_number:
        trucks = {
            _safe_lower(shipment.get("china_truck_number") if shipment else ""),
            _safe_lower(shipment.get("kazakh_truck_number") if shipment else ""),
            _safe_lower(latest_status.get("truck_number") if latest_status else ""),
        }
        if _safe_lower(filters.fura_number) not in trucks:
            return False
    if filters.warehouse and _safe_lower(shipment.get("agent") if shipment else "") != _safe_lower(filters.warehouse):
        return False
    return True


def _shipment_row_matches(
    row: dict[str, Any],
    filters: AnalyticsFilters,
    latest_status_row: dict[str, Any] | None,
    sales_by_reys: dict[str, list[dict[str, Any]]],
    logists_by_reys_map: dict[str, list[str]],
) -> bool:
    reys = _clean_text(row.get("reys_number"))
    related_sales = sales_by_reys.get(reys, [])

    if not _date_match(row.get("loaded_date") or row.get("tashkent_date") or row.get("distributed_date"), filters):
        return False
    if filters.reys_number and _safe_lower(reys) != _safe_lower(filters.reys_number):
        return False
    if filters.client:
        sales_clients = {_safe_lower(item.get("client_name")) for item in related_sales}
        if _safe_lower(filters.client) not in sales_clients:
            return False
    if filters.bl_code:
        sales_marks = {_safe_lower(item.get("shipping_mark")) for item in related_sales}
        if _safe_lower(filters.bl_code) not in sales_marks:
            return False
    if filters.manager:
        sales_managers = {_safe_lower(item.get("salesperson")) for item in related_sales}
        if _safe_lower(filters.manager) not in sales_managers:
            return False
    if filters.logist:
        logists = [_safe_lower(item) for item in logists_by_reys_map.get(reys, [])]
        if _safe_lower(filters.logist) not in logists:
            return False
    if filters.fura_number:
        trucks = {
            _safe_lower(row.get("china_truck_number")),
            _safe_lower(row.get("kazakh_truck_number")),
            _safe_lower(latest_status_row.get("truck_number") if latest_status_row else ""),
        }
        if _safe_lower(filters.fura_number) not in trucks:
            return False
    if filters.status:
        status = _resolve_shipment_status(row, latest_status_row)
        if _safe_lower(_normalize_status_bucket(status)) != _safe_lower(_normalize_status_bucket(filters.status)):
            return False
    if filters.warehouse and _safe_lower(row.get("agent")) != _safe_lower(filters.warehouse):
        return False
    return True


def _filter_sales(
    rows: list[dict[str, Any]],
    filters: AnalyticsFilters,
    latest_statuses: dict[str, dict[str, Any]] | None = None,
    shipment_by_reys: dict[str, dict[str, Any]] | None = None,
    logists_by_reys_map: dict[str, list[str]] | None = None,
) -> list[dict[str, Any]]:
    latest_statuses = latest_statuses or {}
    shipment_by_reys = shipment_by_reys or {}
    logists_by_reys_map = logists_by_reys_map or {}
    return [
        row
        for row in rows
        if _sales_row_matches(row, filters, latest_statuses, shipment_by_reys, logists_by_reys_map)
    ]


def _filter_cashflow(rows: list[dict[str, Any]], filters: AnalyticsFilters) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        if not _date_match(row.get("operation_date"), filters):
            continue
        if filters.currency and _safe_lower(row.get("currency")) != _safe_lower(filters.currency):
            continue
        if filters.bank_or_cash and _safe_lower(row.get("wallet")) != _safe_lower(filters.bank_or_cash):
            continue
        if filters.category and _safe_lower(row.get("category")) != _safe_lower(filters.category):
            continue
        if filters.reys_number and _safe_lower(row.get("reys_number")) != _safe_lower(filters.reys_number):
            continue
        output.append(row)
    return output


def _filter_logists(rows: list[dict[str, Any]], filters: AnalyticsFilters) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        if filters.logist and _safe_lower(row.get("logist_name")) != _safe_lower(filters.logist):
            continue
        if filters.reys_number and _safe_lower(row.get("reys_number")) != _safe_lower(filters.reys_number):
            continue
        output.append(row)
    return output


def _filter_shipments(
    rows: list[dict[str, Any]],
    filters: AnalyticsFilters,
    latest_statuses: dict[str, dict[str, Any]],
    sales_rows: list[dict[str, Any]],
    logists_by_reys_map: dict[str, list[str]],
) -> list[dict[str, Any]]:
    sales_by_reys: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sales_rows:
        reys = _clean_text(row.get("reys_number"))
        if reys:
            sales_by_reys[reys].append(row)
    return [
        row
        for row in rows
        if _shipment_row_matches(
            row,
            filters,
            latest_statuses.get(_clean_text(row.get("reys_number"))),
            sales_by_reys,
            logists_by_reys_map,
        )
    ]


def _sum_sales(rows: list[dict[str, Any]]) -> float:
    return sum(_to_float(row.get("final_sale_amount")) for row in rows)


def _sum_cashflow_usd(rows: list[dict[str, Any]], flow_type: str) -> float:
    return sum(_to_float(row.get("amount_usd")) for row in rows if _safe_lower(row.get("flow_type")) == flow_type)


def _previous_period_range(start: date | None, end: date | None) -> tuple[date | None, date | None]:
    if not start or not end:
        return None, None
    days = max((end - start).days + 1, 1)
    previous_end = start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=days - 1)
    return previous_start, previous_end


def _group_month(
    rows: list[dict[str, Any]],
    value_fn: Callable[[dict[str, Any]], float],
    date_fn: Callable[[dict[str, Any]], Any],
) -> list[dict[str, Any]]:
    totals: dict[str, float] = defaultdict(float)
    for row in rows:
        month = _month_key(date_fn(row))
        if not month:
            continue
        totals[month] += value_fn(row)
    return [
        {
            "month": month,
            "label": _month_label(month),
            "value": _round(value),
        }
        for month, value in sorted(totals.items())
    ]


def _metric_value(row: dict[str, Any], metric: str) -> float:
    if metric == "cbm":
        return _to_float(row.get("cbm"))
    if metric == "bl_count":
        return 1.0 if _clean_text(row.get("shipping_mark")) else 0.0
    return _to_float(row.get("final_sale_amount"))


def _build_filter_options(dataset: dict[str, list[dict[str, Any]]]) -> dict[str, list[str]]:
    latest_statuses = _latest_status_map(dataset["statuses"])
    furas = set()
    warehouses = set()
    for row in dataset["shipments"]:
        for key in ("china_truck_number", "kazakh_truck_number"):
            value = _clean_text(row.get(key))
            if value:
                furas.add(value)
        warehouse = _clean_text(row.get("agent"))
        if warehouse:
            warehouses.add(warehouse)
    for row in latest_statuses.values():
        truck = _clean_text(row.get("truck_number"))
        if truck:
            furas.add(truck)
    statuses = {
        _normalize_status_bucket(_clean_text(row.get("status")))
        for row in latest_statuses.values()
        if _clean_text(row.get("status"))
    }
    return {
        "managers": sorted({_clean_text(row.get("salesperson")) for row in dataset["sales"] if _clean_text(row.get("salesperson"))}),
        "logists": sorted({_clean_text(row.get("logist_name")) for row in dataset["logists"] if _clean_text(row.get("logist_name"))}),
        "clients": sorted({_clean_text(row.get("client_name")) for row in dataset["sales"] if _clean_text(row.get("client_name"))}),
        "bl_codes": sorted({_clean_text(row.get("shipping_mark")).upper() for row in dataset["sales"] if _clean_text(row.get("shipping_mark"))}),
        "reys_numbers": sorted(
            {_clean_text(row.get("reys_number")) for row in dataset["sales"] if _clean_text(row.get("reys_number"))}
            | {_clean_text(row.get("reys_number")) for row in dataset["shipments"] if _clean_text(row.get("reys_number"))}
        ),
        "furas": sorted(item for item in furas if item),
        "statuses": sorted(item for item in statuses if item),
        "currencies": sorted({_clean_text(row.get("currency")).upper() for row in dataset["cashflow"] if _clean_text(row.get("currency"))}),
        "bank_or_cash": sorted({_clean_text(row.get("wallet")) for row in dataset["cashflow"] if _clean_text(row.get("wallet"))}),
        "categories": sorted({_clean_text(row.get("category")) for row in dataset["cashflow"] if _clean_text(row.get("category"))}),
        "warehouses": sorted(warehouses),
    }


def _selected_filters_payload(filters: AnalyticsFilters) -> dict[str, Any]:
    return {
        "period": filters.preset,
        "date_from": _date_to_str(filters.date_from),
        "date_to": _date_to_str(filters.date_to),
        "sales_plan_id": filters.sales_plan_id,
        "manager": filters.manager,
        "logist": filters.logist,
        "client": filters.client,
        "bl_code": filters.bl_code,
        "reys_number": filters.reys_number,
        "fura": filters.fura_number,
        "status": filters.status,
        "currency": filters.currency,
        "bank_or_cash": filters.bank_or_cash,
        "category": filters.category,
        "warehouse": filters.warehouse,
    }


def _sales_by_manager(rows: list[dict[str, Any]], metric: str) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"value": 0.0, "bl_codes": set(), "clients": set(), "cbm": 0.0, "gross_weight": 0.0}
    )
    for row in rows:
        manager = _clean_text(row.get("salesperson"))
        if not manager:
            continue
        bucket = grouped[manager]
        bucket["value"] += _metric_value(row, metric)
        bl = _clean_text(row.get("shipping_mark")).upper()
        client = _clean_text(row.get("client_name"))
        if bl:
            bucket["bl_codes"].add(bl)
        if client:
            bucket["clients"].add(client)
        bucket["cbm"] += _to_float(row.get("cbm"))
        bucket["gross_weight"] += _to_float(row.get("gross_weight"))

    total_value = sum(bucket["value"] for bucket in grouped.values()) or 1.0
    output = []
    for manager, bucket in grouped.items():
        value = bucket["value"]
        output.append(
            {
                "manager_name": manager,
                "value": _round(value),
                "display_value": _format_money(value) if metric == "amount_usd" else f"{_format_number(value)} {PLAN_METRIC_LABELS.get(metric, '')}".strip(),
                "bl_count": len(bucket["bl_codes"]),
                "client_count": len(bucket["clients"]),
                "average_deal_value": _round(value / max(len(bucket["bl_codes"]), 1)),
                "average_deal": _format_money(value / max(len(bucket["bl_codes"]), 1)) if metric == "amount_usd" else f"{_format_number(value / max(len(bucket['bl_codes']), 1))} {PLAN_METRIC_LABELS.get(metric, '')}".strip(),
                "cbm": _round(bucket["cbm"]),
                "gross_weight": _round(bucket["gross_weight"]),
                "share_percent": _round((value / total_value) * 100),
            }
        )
    output.sort(key=lambda item: item["value"], reverse=True)
    return output


def _sales_by_logist(sales_rows: list[dict[str, Any]], logist_rows: list[dict[str, Any]], metric: str) -> list[dict[str, Any]]:
    sales_by_reys: dict[str, dict[str, Any]] = defaultdict(lambda: {"value": 0.0, "bl_codes": set()})
    for row in sales_rows:
        reys = _clean_text(row.get("reys_number"))
        if not reys:
            continue
        sales_by_reys[reys]["value"] += _metric_value(row, metric)
        bl = _clean_text(row.get("shipping_mark")).upper()
        if bl:
            sales_by_reys[reys]["bl_codes"].add(bl)

    assignments_by_reys: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in logist_rows:
        reys = _clean_text(row.get("reys_number"))
        name = _clean_text(row.get("logist_name"))
        if reys and name:
            assignments_by_reys[reys].append(row)

    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "value": 0.0,
            "reys_numbers": set(),
            "bl_count": 0.0,
            "warehouse_ok": 0,
            "damage_ok": 0,
            "assignments": 0,
        }
    )
    for reys, sales_info in sales_by_reys.items():
        assignments = assignments_by_reys.get(reys, [])
        if not assignments:
            continue
        share_value = sales_info["value"] / len(assignments)
        share_bl = len(sales_info["bl_codes"]) / len(assignments)
        for assignment in assignments:
            name = _clean_text(assignment.get("logist_name"))
            bucket = grouped[name]
            bucket["value"] += share_value
            bucket["bl_count"] += share_bl
            bucket["reys_numbers"].add(reys)
            bucket["warehouse_ok"] += _to_int(assignment.get("warehouse_no_extra_days"))
            bucket["damage_ok"] += _to_int(assignment.get("no_damage_or_missing"))
            bucket["assignments"] += 1

    total_value = sum(bucket["value"] for bucket in grouped.values()) or 1.0
    output = []
    for name, bucket in grouped.items():
        assigned_reys = len(bucket["reys_numbers"])
        value = bucket["value"]
        output.append(
            {
                "logist_name": name,
                "assigned_reys_count": assigned_reys,
                "closed_amount": _round(value),
                "display_value": _format_money(value) if metric == "amount_usd" else f"{_format_number(value)} {PLAN_METRIC_LABELS.get(metric, '')}".strip(),
                "share_percent": _round((value / total_value) * 100),
                "average_per_reys_value": _round(value / max(assigned_reys, 1)),
                "average_per_reys": _format_money(value / max(assigned_reys, 1)) if metric == "amount_usd" else f"{_format_number(value / max(assigned_reys, 1))} {PLAN_METRIC_LABELS.get(metric, '')}".strip(),
                "warehouse_kpi": f"{bucket['warehouse_ok']}/{max(bucket['assignments'], 1)}",
                "damage_kpi": f"{bucket['damage_ok']}/{max(bucket['assignments'], 1)}",
                "bl_count": _round(bucket["bl_count"], 2),
            }
        )
    output.sort(key=lambda item: item["closed_amount"], reverse=True)
    return output


def _shipment_status_counts(
    shipment_rows: list[dict[str, Any]],
    latest_statuses: dict[str, dict[str, Any]],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in shipment_rows:
        reys = _clean_text(row.get("reys_number"))
        label = _normalize_status_bucket(_resolve_shipment_status(row, latest_statuses.get(reys)))
        if label:
            counts[label] += 1
    return dict(counts)


def _delayed_shipments(shipment_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    delayed: list[dict[str, Any]] = []
    today = datetime.now().date()
    for row in shipment_rows:
        loaded = _parse_date(row.get("loaded_date"))
        distributed = _parse_date(row.get("distributed_date"))
        if loaded and not distributed and (today - loaded).days > DELAY_THRESHOLD_DAYS:
            delayed.append(row)
            continue
        if _to_float(row.get("zhongshan_tashkent_days")) > DELAY_THRESHOLD_DAYS:
            delayed.append(row)
    return delayed


def _shipment_table_rows(
    sales_rows: list[dict[str, Any]],
    shipment_rows: list[dict[str, Any]],
    latest_statuses: dict[str, dict[str, Any]],
    logist_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    shipment_by_reys = _shipment_map(shipment_rows)
    logists_by_reys_map = _logists_by_reys(logist_rows)
    today = datetime.now().date()
    output = []
    for row in sales_rows:
        reys = _clean_text(row.get("reys_number"))
        shipment = shipment_by_reys.get(reys, {})
        latest_status = latest_statuses.get(reys, {})
        loaded = _parse_date(shipment.get("loaded_date"))
        arrived = _parse_date(shipment.get("distributed_date") or shipment.get("tashkent_date"))
        if loaded and arrived:
            days = (arrived - loaded).days
        elif loaded:
            days = (today - loaded).days
        else:
            days = None
        output.append(
            {
                "bl_code": _clean_text(row.get("shipping_mark")).upper(),
                "client_name": _clean_text(row.get("client_name")),
                "reys_number": reys,
                "fura_number": _clean_text(shipment.get("kazakh_truck_number") or shipment.get("china_truck_number") or latest_status.get("truck_number")),
                "status": _resolve_shipment_status(shipment, latest_status),
                "loaded_date": _clean_text(shipment.get("loaded_date")),
                "arrived_date": _clean_text(shipment.get("distributed_date") or shipment.get("tashkent_date")),
                "days": days,
                "manager_name": _clean_text(row.get("salesperson")),
                "logist_name": ", ".join(logists_by_reys_map.get(reys, [])),
            }
        )
    output.sort(key=lambda item: (item["loaded_date"], item["reys_number"], item["bl_code"]), reverse=True)
    return output


def _sales_period_total(rows: list[dict[str, Any]], start: date | None, end: date | None) -> float:
    subset = []
    for row in rows:
        row_date = _parse_date(row.get("sale_date") or row.get("invoice_date"))
        if start and (row_date is None or row_date < start):
            continue
        if end and (row_date is None or row_date > end):
            continue
        subset.append(row)
    return _sum_sales(subset)


def _smart_insights(
    sales_rows: list[dict[str, Any]],
    cashflow_rows: list[dict[str, Any]],
    debt_rows: list[dict[str, Any]],
    shipment_rows: list[dict[str, Any]],
    manager_rows: list[dict[str, Any]],
) -> list[str]:
    insights: list[str] = []
    total_sales = _sum_sales(sales_rows)
    previous_start, previous_end = _previous_period_range(
        min((_parse_date(row.get("sale_date") or row.get("invoice_date")) for row in sales_rows if _parse_date(row.get("sale_date") or row.get("invoice_date"))), default=None),
        max((_parse_date(row.get("sale_date") or row.get("invoice_date")) for row in sales_rows if _parse_date(row.get("sale_date") or row.get("invoice_date"))), default=None),
    )
    previous_sales = _sales_period_total(sales_rows, previous_start, previous_end) if previous_start and previous_end else 0
    growth = _percent_change(total_sales, previous_sales)
    if growth is not None:
        direction = "oshdi" if growth >= 0 else "kamaydi"
        insights.append(f"📈 Savdo o‘tgan davrga nisbatan {abs(growth):.1f}% ga {direction}.")

    if debt_rows:
        biggest = max(debt_rows, key=lambda item: _to_float(item.get("debt_amount")))
        if _to_float(biggest.get("debt_amount")) > 0:
            insights.append(
                f"⚠️ Eng katta qarz: {biggest.get('client_name') or biggest.get('shipping_mark')} — {_format_money(_to_float(biggest.get('debt_amount')))}."
            )

    if manager_rows:
        top_manager = max(manager_rows, key=lambda item: _to_float(item.get("value") or item.get("sales_amount")))
        insights.append(f"🏆 Oy bo‘yicha eng yaxshi menejer: {top_manager.get('manager_name')}.")

    if shipment_rows:
        status_counts = Counter()
        for row in shipment_rows:
            label = _normalize_status_bucket(_resolve_shipment_status(row, None))
            if label:
                status_counts[label] += 1
        if status_counts:
            label, value = max(status_counts.items(), key=lambda item: item[1])
            insights.append(f"🚛 Eng ko‘p aktiv BL hozir {label} bosqichida.")

    income = _sum_cashflow_usd(cashflow_rows, "income")
    if income < total_sales and total_sales > 0:
        insights.append("📉 Kirim kamaygan, lekin BL soni oshgan.")

    return insights[:5]


def _debts_raw(sales_rows: list[dict[str, Any]], cashflow_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sales_by_mark: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "shipping_mark": "",
            "client_name": "",
            "salesperson": "",
            "sales_amount": 0.0,
            "sale_date": None,
        }
    )
    for row in sales_rows:
        mark = _clean_text(row.get("shipping_mark")).upper()
        if not mark:
            continue
        bucket = sales_by_mark[mark]
        bucket["shipping_mark"] = mark
        bucket["client_name"] = _clean_text(row.get("client_name"))
        bucket["salesperson"] = _clean_text(row.get("salesperson"))
        bucket["sales_amount"] += _to_float(row.get("final_sale_amount"))
        sale_date = _parse_date(row.get("sale_date") or row.get("invoice_date"))
        if sale_date and (bucket["sale_date"] is None or sale_date > bucket["sale_date"]):
            bucket["sale_date"] = sale_date

    payments_by_counterparty: dict[str, dict[str, Any]] = defaultdict(lambda: {"paid": 0.0, "last_date": None})
    for row in cashflow_rows:
        if _safe_lower(row.get("flow_type")) != "income":
            continue
        key = _clean_text(row.get("counterparty")).upper()
        if not key:
            continue
        payments_by_counterparty[key]["paid"] += _to_float(row.get("amount_usd"))
        op_date = _parse_date(row.get("operation_date"))
        if op_date and (
            payments_by_counterparty[key]["last_date"] is None
            or op_date > payments_by_counterparty[key]["last_date"]
        ):
            payments_by_counterparty[key]["last_date"] = op_date

    debts = []
    for mark, row in sales_by_mark.items():
        payment_info = payments_by_counterparty.get(mark) or payments_by_counterparty.get(_clean_text(row.get("client_name")).upper()) or {"paid": 0.0, "last_date": None}
        paid_amount = _to_float(payment_info.get("paid"))
        sales_amount = _to_float(row.get("sales_amount"))
        debts.append(
            {
                **row,
                "paid_amount": paid_amount,
                "debt_amount": sales_amount - paid_amount,
                "last_payment_date": payment_info.get("last_date"),
            }
        )
    debts.sort(key=lambda item: _to_float(item.get("debt_amount")), reverse=True)
    return debts


def _missing_currencies(cashflow_rows: list[dict[str, Any]]) -> list[str]:
    missing = set()
    for row in cashflow_rows:
        currency = _clean_text(row.get("currency")).upper()
        amount = _to_float(row.get("amount"))
        amount_usd = _to_float(row.get("amount_usd"))
        if currency and currency != BASE_CURRENCY and amount > 0 and amount_usd == 0:
            missing.add(currency)
    return sorted(missing)


def _month_label(month_key: str) -> str:
    if not month_key:
        return "—"
    year, month = month_key.split("-")
    return f"{MONTH_NAMES.get(month, month)} {year}"


def _smart_insights(
    sales_rows: list[dict[str, Any]],
    cashflow_rows: list[dict[str, Any]],
    debt_rows: list[dict[str, Any]],
    shipment_rows: list[dict[str, Any]],
    manager_rows: list[dict[str, Any]],
) -> list[str]:
    insights: list[str] = []
    total_sales = _sum_sales(sales_rows)
    previous_start, previous_end = _previous_period_range(
        min((_parse_date(row.get("sale_date") or row.get("invoice_date")) for row in sales_rows if _parse_date(row.get("sale_date") or row.get("invoice_date"))), default=None),
        max((_parse_date(row.get("sale_date") or row.get("invoice_date")) for row in sales_rows if _parse_date(row.get("sale_date") or row.get("invoice_date"))), default=None),
    )
    previous_sales = _sales_period_total(sales_rows, previous_start, previous_end) if previous_start and previous_end else 0
    growth = _percent_change(total_sales, previous_sales)
    if growth is not None:
        direction = "oshdi" if growth >= 0 else "kamaydi"
        insights.append(f"📈 Savdo o‘tgan davrga nisbatan {abs(growth):.1f}% ga {direction}.")

    if debt_rows:
        biggest = max(debt_rows, key=lambda item: _to_float(item.get("debt_amount")))
        if _to_float(biggest.get("debt_amount")) > 0:
            insights.append(
                f"⚠️ Eng katta qarz: {biggest.get('client_name') or biggest.get('shipping_mark')} — {_format_money(_to_float(biggest.get('debt_amount')))}."
            )

    if manager_rows:
        top_manager = max(manager_rows, key=lambda item: _to_float(item.get("value") or item.get("sales_amount")))
        insights.append(f"🏆 Oy bo‘yicha eng yaxshi menejer: {top_manager.get('manager_name')}.")

    if shipment_rows:
        status_counts = Counter()
        for row in shipment_rows:
            label = _normalize_status_bucket(_resolve_shipment_status(row, None))
            if label:
                status_counts[label] += 1
        if status_counts:
            label, _value = max(status_counts.items(), key=lambda item: item[1])
            insights.append(f"🚛 Eng ko‘p aktiv BL hozir {label} bosqichida.")

    income = _sum_cashflow_usd(cashflow_rows, "income")
    if income < total_sales and total_sales > 0:
        insights.append("📉 Kirim kamaygan, lekin BL soni oshgan.")

    return insights[:5]


def get_overview(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, dataset["plans"])
    filters = _apply_plan_dates(initial_filters, selected_plan)
    latest_statuses = _latest_status_map(dataset["statuses"])
    shipment_by_reys = _shipment_map(dataset["shipments"])
    logists_map = _logists_by_reys(dataset["logists"])

    sales_rows = _filter_sales(dataset["sales"], filters, latest_statuses, shipment_by_reys, logists_map)
    cashflow_rows = _filter_cashflow(dataset["cashflow"], filters)
    shipment_rows = _filter_shipments(dataset["shipments"], filters, latest_statuses, sales_rows, logists_map)
    debts_rows = _debts_raw(sales_rows, cashflow_rows)

    total_sales = _sum_sales(sales_rows)
    previous_start, previous_end = _previous_period_range(filters.date_from, filters.date_to)
    previous_sales = _sales_period_total(dataset["sales"], previous_start, previous_end)
    growth = _percent_change(total_sales, previous_sales)
    income = _sum_cashflow_usd(cashflow_rows, "income")
    expense = _sum_cashflow_usd(cashflow_rows, "expense")
    profit = income - expense
    margin = (profit / income * 100.0) if income else 0.0
    total_debt = sum(max(_to_float(item.get("debt_amount")), 0.0) for item in debts_rows)
    distinct_bl = {_clean_text(row.get("shipping_mark")).upper() for row in sales_rows if _clean_text(row.get("shipping_mark"))}
    arrived_shipments = [row for row in shipment_rows if _clean_text(row.get("distributed_date")) or _clean_text(row.get("tashkent_date"))]
    delayed_shipments = _delayed_shipments(shipment_rows)
    average_deal = total_sales / max(len(distinct_bl), 1) if distinct_bl else 0.0
    managers_rows = _sales_by_manager(sales_rows, "amount_usd")
    sync_status = analytics_importer.get_sync_status()

    return {
        "filters": _build_filter_options(dataset),
        "selected_filters": _selected_filters_payload(filters),
        "plans": list_sales_plans(),
        "selected_plan": selected_plan,
        "kpis": {
            "total_sales": {"value": total_sales, "display": _format_money(total_sales)},
            "monthly_growth": {"value": growth or 0, "display": "—" if growth is None else f"{growth:.1f}%"},
            "income": {"value": income, "display": _format_money(income)},
            "expense": {"value": expense, "display": _format_money(expense)},
            "profit": {"value": profit, "display": _format_money(profit), "note": f"Margin {margin:.1f}%"},
            "debt": {"value": total_debt, "display": _format_money(total_debt)},
            "active_bl_count": {"value": len(distinct_bl), "display": str(len(distinct_bl))},
            "arrived_shipments_count": {"value": len(arrived_shipments), "display": str(len(arrived_shipments))},
            "delayed_shipments_count": {"value": len(delayed_shipments), "display": str(len(delayed_shipments))},
            "average_deal": {"value": average_deal, "display": _format_money(average_deal)},
        },
        "meta": {
            "has_data": bool(dataset["sales"] or dataset["cashflow"] or dataset["shipments"]),
            "base_currency": BASE_CURRENCY,
            "last_sync_at": sync_status.get("last_sync_at", ""),
            "source_name": sync_status.get("source_name", ""),
            "missing_currencies": _missing_currencies(cashflow_rows),
        },
        "smart_insights": _smart_insights(sales_rows, cashflow_rows, debts_rows, shipment_rows, managers_rows),
        "empty": not bool(dataset["sales"] or dataset["cashflow"]),
    }


def get_sales_growth(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, dataset["plans"])
    filters = _apply_plan_dates(initial_filters, selected_plan)
    latest_statuses = _latest_status_map(dataset["statuses"])
    shipment_by_reys = _shipment_map(dataset["shipments"])
    logists_map = _logists_by_reys(dataset["logists"])

    sales_rows = _filter_sales(dataset["sales"], filters, latest_statuses, shipment_by_reys, logists_map)
    cashflow_rows = _filter_cashflow(dataset["cashflow"], filters)

    current_total = _sum_sales(sales_rows)
    previous_start, previous_end = _previous_period_range(filters.date_from, filters.date_to)
    previous_total = _sales_period_total(dataset["sales"], previous_start, previous_end)
    growth = _percent_change(current_total, previous_total)

    sales_series = _group_month(sales_rows, lambda row: _to_float(row.get("final_sale_amount")), lambda row: row.get("sale_date") or row.get("invoice_date"))
    income_series = _group_month(
        [row for row in cashflow_rows if _safe_lower(row.get("flow_type")) == "income"],
        lambda row: _to_float(row.get("amount_usd")),
        lambda row: row.get("operation_date"),
    )
    expense_series = _group_month(
        [row for row in cashflow_rows if _safe_lower(row.get("flow_type")) == "expense"],
        lambda row: _to_float(row.get("amount_usd")),
        lambda row: row.get("operation_date"),
    )

    expense_map = {row["month"]: row["value"] for row in expense_series}
    profit_series = []
    for row in income_series:
        profit_series.append(
            {
                "month": row["month"],
                "label": row["label"],
                "value": _round(row["value"] - expense_map.get(row["month"], 0.0)),
            }
        )

    monthly_table = []
    previous_value = None
    grouped_bl: dict[str, set[str]] = defaultdict(set)
    grouped_cbm: dict[str, float] = defaultdict(float)
    grouped_weight: dict[str, float] = defaultdict(float)
    for row in sales_rows:
        month = _month_key(row.get("sale_date") or row.get("invoice_date"))
        if not month:
            continue
        bl = _clean_text(row.get("shipping_mark")).upper()
        if bl:
            grouped_bl[month].add(bl)
        grouped_cbm[month] += _to_float(row.get("cbm"))
        grouped_weight[month] += _to_float(row.get("gross_weight"))

    for row in sales_series:
        month = row["month"]
        current_value = _to_float(row.get("value"))
        bl_count = len(grouped_bl.get(month, set()))
        monthly_growth = _percent_change(current_value, previous_value) if previous_value is not None else None
        monthly_table.append(
            {
                "month": row["label"],
                "total_sales": _format_money(current_value),
                "bl_count": bl_count,
                "average_deal": _format_money(current_value / max(bl_count, 1)) if bl_count else _format_money(0),
                "cbm": _round(grouped_cbm.get(month, 0.0)),
                "gross_weight": _round(grouped_weight.get(month, 0.0)),
                "growth_percent": None if monthly_growth is None else _round(monthly_growth, 1),
            }
        )
        previous_value = current_value

    growth_series = []
    previous_month_value = None
    for row in sales_series:
        current_value = _to_float(row.get("value"))
        growth_series.append(
            {
                "month": row["month"],
                "label": row["label"],
                "value": 0 if previous_month_value is None else _round(_percent_change(current_value, previous_month_value) or 0, 1),
            }
        )
        previous_month_value = current_value

    return {
        "summary": {
            "current_month": _format_number(current_total),
            "previous_month": _format_number(previous_total),
            "difference": _format_number(current_total - previous_total),
            "growth_percent": None if growth is None else _round(growth, 1),
            "base_currency": BASE_CURRENCY,
        },
        "series": {
            "sales": sales_series,
            "income": income_series,
            "profit": profit_series,
            "growth": growth_series,
        },
        "table": monthly_table,
        "empty": not bool(sales_rows),
    }


def get_cashflow(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, dataset["plans"])
    filters = _apply_plan_dates(initial_filters, selected_plan)
    rows = _filter_cashflow(dataset["cashflow"], filters)

    income_rows = [row for row in rows if _safe_lower(row.get("flow_type")) == "income"]
    expense_rows = [row for row in rows if _safe_lower(row.get("flow_type")) == "expense"]
    income = _sum_cashflow_usd(income_rows, "income")
    expense = _sum_cashflow_usd(expense_rows, "expense")
    net_profit = income - expense

    wallet_balances: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    currency_balances: dict[str, float] = defaultdict(float)
    for row in rows:
        amount = _to_float(row.get("amount"))
        if _safe_lower(row.get("flow_type")) == "expense":
            amount *= -1
        currency = _clean_text(row.get("currency")).upper() or BASE_CURRENCY
        wallet = _clean_text(row.get("wallet")) or "Noma'lum"
        wallet_balances[wallet][currency] += amount
        currency_balances[currency] += amount

    expense_categories = Counter()
    income_categories = Counter()
    for row in expense_rows:
        expense_categories[_clean_text(row.get("category")) or "Boshqa"] += _to_float(row.get("amount_usd"))
    for row in income_rows:
        income_categories[_clean_text(row.get("category")) or "Boshqa"] += _to_float(row.get("amount_usd"))

    table = [
        {
            "date": _clean_text(row.get("operation_date")),
            "category": _clean_text(row.get("category")),
            "type": _clean_text(row.get("flow_type")),
            "amount": _format_number(_to_float(row.get("amount"))),
            "currency": _clean_text(row.get("currency")).upper() or BASE_CURRENCY,
            "bank_or_cash": _clean_text(row.get("wallet")),
            "comment": _clean_text(row.get("comment")),
            "bl_or_reys": _clean_text(row.get("reys_number")) or _clean_text(row.get("counterparty")),
        }
        for row in sorted(rows, key=lambda item: (_parse_date(item.get("operation_date")) or date.min), reverse=True)
    ]

    return {
        "kpis": {
            "income": _format_money(income),
            "expense": _format_money(expense),
            "net_profit": _format_money(net_profit),
            "balance_by_currency": {currency: _format_money(amount, currency) for currency, amount in sorted(currency_balances.items())},
            "wallet_balances": {
                wallet: {currency: _format_money(amount, currency) for currency, amount in sorted(values.items())}
                for wallet, values in wallet_balances.items()
            },
        },
        "charts": {
            "income_vs_expense": {
                "income": _group_month(income_rows, lambda row: _to_float(row.get("amount_usd")), lambda row: row.get("operation_date")),
                "expense": _group_month(expense_rows, lambda row: _to_float(row.get("amount_usd")), lambda row: row.get("operation_date")),
            },
            "expense_by_category": [{"label": key, "value": _round(value)} for key, value in expense_categories.most_common(10)],
            "income_by_category": [{"label": key, "value": _round(value)} for key, value in income_categories.most_common(10)],
        },
        "table": table,
        "empty": not bool(rows),
    }


def get_managers(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, dataset["plans"])
    filters = _apply_plan_dates(initial_filters, selected_plan)
    latest_statuses = _latest_status_map(dataset["statuses"])
    shipment_by_reys = _shipment_map(dataset["shipments"])
    logists_map = _logists_by_reys(dataset["logists"])

    sales_rows = _filter_sales(dataset["sales"], filters, latest_statuses, shipment_by_reys, logists_map)
    cashflow_rows = _filter_cashflow(dataset["cashflow"], filters)
    shipment_rows = _filter_shipments(dataset["shipments"], filters, latest_statuses, sales_rows, logists_map)

    base_rows = _sales_by_manager(sales_rows, "amount_usd")
    debts_rows = _debts_raw(sales_rows, cashflow_rows)
    debt_by_manager: dict[str, dict[str, float]] = defaultdict(lambda: {"debt": 0.0, "paid": 0.0})
    for row in debts_rows:
        manager = _clean_text(row.get("salesperson")) or "Belgilanmagan"
        debt_by_manager[manager]["debt"] += _to_float(row.get("debt_amount"))
        debt_by_manager[manager]["paid"] += _to_float(row.get("paid_amount"))

    delayed_reys = {_clean_text(item.get("reys_number")) for item in _delayed_shipments(shipment_rows)}
    delayed_by_manager = Counter()
    for row in sales_rows:
        manager = _clean_text(row.get("salesperson"))
        if manager and _clean_text(row.get("reys_number")) in delayed_reys:
            delayed_by_manager[manager] += 1

    table = []
    for row in base_rows:
        manager = row["manager_name"]
        sale_amount = _to_float(row.get("value"))
        paid_amount = debt_by_manager[manager]["paid"]
        debt_amount = debt_by_manager[manager]["debt"]
        related_sales = [item for item in sales_rows if _clean_text(item.get("salesperson")) == manager]
        profit_value = sum(
            _to_float(item.get("final_sale_amount"))
            - _to_float(item.get("customs_payment"))
            - _to_float(item.get("company_expense"))
            - _to_float(item.get("certificate_expense"))
            for item in related_sales
        )
        if debt_amount <= 0:
            status = "To'liq yopilgan"
        elif paid_amount > 0:
            status = "Qisman to'langan"
        else:
            status = "Qarzdor"
        table.append(
            {
                "manager_name": manager,
                "sales_total": row["display_value"],
                "sales_amount": sale_amount,
                "bl_count": row["bl_count"],
                "client_count": row["client_count"],
                "paid_amount": _format_money(paid_amount),
                "debt_amount": _format_money(debt_amount),
                "average_check": row["average_deal"],
                "profit": _format_money(profit_value),
                "profit_value": _round(profit_value),
                "status": status,
                "late_count": delayed_by_manager.get(manager, 0),
                "cbm": _round(row["cbm"]),
                "gross_weight": _round(row["gross_weight"]),
                "share_percent": row["share_percent"],
                "debt_amount_value": _round(debt_amount),
                "paid_amount_value": _round(paid_amount),
            }
        )
    table.sort(key=lambda item: item["sales_amount"], reverse=True)

    ranking = []
    if table:
        ranking.append(f"🏆 Eng ko‘p savdo: {table[0]['manager_name']} — {table[0]['sales_total']}")
        ranking.append(f"📦 Eng ko‘p BL: {max(table, key=lambda item: item['bl_count'])['manager_name']}")
        ranking.append(f"⚠️ Eng ko‘p qarz: {max(table, key=lambda item: item['debt_amount_value'])['manager_name']}")
        ranking.append(f"🚛 Eng ko‘p kechikish: {max(table, key=lambda item: item['late_count'])['manager_name']}")

    return {
        "ranking": ranking,
        "leaders": table[:5],
        "table": table,
        "empty": not bool(table),
    }


def get_logists(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, dataset["plans"])
    filters = _apply_plan_dates(initial_filters, selected_plan)
    latest_statuses = _latest_status_map(dataset["statuses"])
    shipment_by_reys = _shipment_map(dataset["shipments"])
    logists_map = _logists_by_reys(dataset["logists"])

    sales_rows = _filter_sales(dataset["sales"], filters, latest_statuses, shipment_by_reys, logists_map)
    logist_rows = _filter_logists(dataset["logists"], filters)
    rows = _sales_by_logist(sales_rows, logist_rows, "amount_usd")

    summary = {
        "total_closed": _format_money(sum(item["closed_amount"] for item in rows)),
        "total_reys": sum(item["assigned_reys_count"] for item in rows),
        "avg_per_reys": _format_money(
            (sum(item["closed_amount"] for item in rows) / max(sum(item["assigned_reys_count"] for item in rows), 1))
            if rows
            else 0
        ),
    }

    return {
        "leaders": rows[:5],
        "summary": summary,
        "table": rows,
        "empty": not bool(rows),
    }


def get_shipments(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, dataset["plans"])
    filters = _apply_plan_dates(initial_filters, selected_plan)
    latest_statuses = _latest_status_map(dataset["statuses"])
    shipment_by_reys = _shipment_map(dataset["shipments"])
    logists_map = _logists_by_reys(dataset["logists"])

    sales_rows = _filter_sales(dataset["sales"], filters, latest_statuses, shipment_by_reys, logists_map)
    shipment_rows = _filter_shipments(dataset["shipments"], filters, latest_statuses, sales_rows, logists_map)
    logist_rows = _filter_logists(dataset["logists"], filters)

    status_counts = _shipment_status_counts(shipment_rows, latest_statuses)
    delayed = _delayed_shipments(shipment_rows)
    arrived = [row for row in shipment_rows if _clean_text(row.get("tashkent_date")) or _clean_text(row.get("distributed_date"))]
    active_bl = {_clean_text(row.get("shipping_mark")).upper() for row in sales_rows if _clean_text(row.get("shipping_mark"))}
    delivered_days = [_to_float(item.get("zhongshan_tashkent_days")) for item in arrived if _to_float(item.get("zhongshan_tashkent_days")) > 0]
    average_delivery_days = _round(sum(delivered_days) / len(delivered_days), 1) if delivered_days else 0.0

    return {
        "kpis": {
            "sent_furas": len(shipment_rows),
            "arrived_furas": len(arrived),
            "active_bl": len(active_bl),
            "in_transit_bl": max(len(active_bl) - len(arrived), 0),
            "china_count": status_counts.get("Xitoy", 0),
            "horgos_count": status_counts.get("Horgos", 0),
            "yallama_count": status_counts.get("Yallama", 0),
            "toshkent_count": status_counts.get("Toshkent", 0),
            "chuqursoy_count": status_counts.get("Chuqursoy / bojxona", 0),
            "average_delivery_days": average_delivery_days,
            "delayed_shipments": len(delayed),
        },
        "series": {
            "sent_by_month": _group_month(shipment_rows, lambda row: 1, lambda row: row.get("loaded_date")),
            "arrived_by_month": _group_month(arrived, lambda row: 1, lambda row: row.get("distributed_date") or row.get("tashkent_date")),
            "status_counts": [{"label": key, "value": value} for key, value in status_counts.items()],
        },
        "table": _shipment_table_rows(sales_rows, shipment_rows, latest_statuses, logist_rows),
        "empty": not bool(shipment_rows),
    }


def get_debts(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, dataset["plans"])
    filters = _apply_plan_dates(initial_filters, selected_plan)
    latest_statuses = _latest_status_map(dataset["statuses"])
    shipment_by_reys = _shipment_map(dataset["shipments"])
    logists_map = _logists_by_reys(dataset["logists"])

    sales_rows = _filter_sales(dataset["sales"], filters, latest_statuses, shipment_by_reys, logists_map)
    cashflow_rows = _filter_cashflow(dataset["cashflow"], filters)
    raw_rows = _debts_raw(sales_rows, cashflow_rows)

    table = []
    for row in raw_rows:
        sales_amount = _to_float(row.get("sales_amount"))
        debt_amount = _to_float(row.get("debt_amount"))
        sale_date = row.get("sale_date")
        days = (datetime.now().date() - sale_date).days if sale_date else 0
        if debt_amount <= 0:
            state = "green"
            comment = "To'liq yopilgan"
        elif debt_amount <= sales_amount * 0.3:
            state = "yellow"
            comment = "Qisman to'langan"
        else:
            state = "red"
            comment = "Katta qarz"
        table.append(
            {
                "bl_code": row["shipping_mark"],
                "shipping_mark": row["shipping_mark"],
                "client_name": row["client_name"],
                "amount": _format_money(sales_amount),
                "amount_value": _round(sales_amount),
                "paid_amount": _format_money(_to_float(row.get("paid_amount"))),
                "paid_amount_value": _round(_to_float(row.get("paid_amount"))),
                "debt_amount": _format_money(debt_amount),
                "debt_amount_value": _round(debt_amount),
                "currency": BASE_CURRENCY,
                "days": days,
                "manager_name": row["salesperson"],
                "last_payment_date": _date_to_str(row.get("last_payment_date")),
                "state": state,
                "comment": comment,
            }
        )

    total_debt = sum(max(_to_float(row.get("debt_amount")), 0.0) for row in raw_rows)
    debt_leader = ""
    if table:
        debt_leader = max(table, key=lambda item: item["debt_amount_value"])["manager_name"]

    return {
        "summary": {
            "total_debt": _format_money(total_debt),
            "overdue_count": len([row for row in table if row["state"] == "red"]),
            "partial_paid_count": len([row for row in table if row["state"] == "yellow"]),
            "manager_debt_leader": debt_leader or "—",
        },
        "table": table,
        "empty": not bool(table),
    }


def _plan_metric_value(metric: str, sales_rows: list[dict[str, Any]]) -> float:
    if metric == "cbm":
        return sum(_to_float(row.get("cbm")) for row in sales_rows)
    if metric == "bl_count":
        return float(len({_clean_text(row.get("shipping_mark")).upper() for row in sales_rows if _clean_text(row.get("shipping_mark"))}))
    return _sum_sales(sales_rows)


def _monitor_sales_leaders(sales_rows: list[dict[str, Any]], metric: str, target_value: float) -> tuple[float, list[dict[str, Any]], int]:
    grouped = _sales_by_manager(sales_rows, metric)
    closed_value = sum(item["value"] for item in grouped)
    leaders = []
    for item in grouped[:5]:
        leaders.append(
            {
                "name": item["manager_name"],
                "initials": "".join(part[:1].upper() for part in item["manager_name"].split()[:2]) or "SM",
                "value": _round(item["value"]),
                "bl_count": item["bl_count"],
                "share_percent": _round((item["value"] / target_value) * 100 if target_value else 0),
            }
        )
    return closed_value, leaders, sum(item["bl_count"] for item in grouped)


def _monitor_logist_leaders(
    sales_rows: list[dict[str, Any]],
    logist_rows: list[dict[str, Any]],
    metric: str,
    target_value: float,
) -> tuple[float, list[dict[str, Any]], int]:
    grouped = _sales_by_logist(sales_rows, logist_rows, metric)
    closed_value = sum(item["closed_amount"] for item in grouped)
    leaders = []
    for item in grouped[:5]:
        leaders.append(
            {
                "name": item["logist_name"],
                "initials": "".join(part[:1].upper() for part in item["logist_name"].split()[:2]) or "LG",
                "value": _round(item["closed_amount"]),
                "bl_count": int(round(item["bl_count"])),
                "share_percent": _round((item["closed_amount"] / target_value) * 100 if target_value else 0),
            }
        )
    return closed_value, leaders, int(round(sum(item["bl_count"] for item in grouped)))


def _monitor_monthly(sales_rows: list[dict[str, Any]], metric: str) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {"value": 0.0, "bl_codes": set()})
    for row in sales_rows:
        month = _month_key(row.get("sale_date") or row.get("invoice_date"))
        if not month:
            continue
        grouped[month]["value"] += _metric_value(row, metric)
        bl = _clean_text(row.get("shipping_mark")).upper()
        if bl:
            grouped[month]["bl_codes"].add(bl)
    output = []
    for month in sorted(grouped):
        output.append(
            {
                "month": month,
                "label": _month_label(month),
                "value": _round(grouped[month]["value"]),
                "bl_count": len(grouped[month]["bl_codes"]),
            }
        )
    return output[-12:]


def get_monitor(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    plans = list_sales_plans()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, plans)
    if not selected_plan:
        return {"empty": True, "message": "Avval sales plan tanlang yoki yarating."}
    if not dataset["sales"]:
        return {"empty": True, "message": "Google Sheets ma’lumotlari hali import qilinmagan."}

    metric = _clean_text(args.get("metric") or selected_plan.get("target_metric") or "amount_usd")
    if metric not in PLAN_METRIC_LABELS:
        metric = "amount_usd"

    latest_statuses = _latest_status_map(dataset["statuses"])
    shipment_by_reys = _shipment_map(dataset["shipments"])
    logists_map = _logists_by_reys(dataset["logists"])

    plan_filters = _apply_plan_dates(initial_filters, selected_plan)
    plan_sales_rows = _filter_sales(dataset["sales"], plan_filters, latest_statuses, shipment_by_reys, logists_map)
    all_time_sales_rows = _filter_sales(
        dataset["sales"],
        _filters_without_dates(plan_filters),
        latest_statuses,
        shipment_by_reys,
        logists_map,
    )
    logist_rows = _filter_logists(dataset["logists"], _filters_without_dates(plan_filters))

    target_value = _to_float(selected_plan.get("target_value") or selected_plan.get("target_amount_usd"))
    closed_value = _plan_metric_value(metric, plan_sales_rows)
    remaining_value = max(target_value - closed_value, 0.0)
    progress_percent = _round((closed_value / target_value) * 100 if target_value else 0.0, 2)
    total_bl = len({_clean_text(row.get("shipping_mark")).upper() for row in plan_sales_rows if _clean_text(row.get("shipping_mark"))})
    logists_closed, logist_leaders, logist_bl_count = _monitor_logist_leaders(plan_sales_rows, logist_rows, metric, target_value or 1.0)
    sales_closed, sales_leaders, sales_bl_count = _monitor_sales_leaders(plan_sales_rows, metric, target_value or 1.0)
    sync_status = analytics_importer.get_sync_status()

    return {
        "empty": False,
        "plan": {
            "id": _to_int(selected_plan.get("id")),
            "name": _clean_text(selected_plan.get("name")),
            "period_start": _clean_text(selected_plan.get("period_start")),
            "period_end": _clean_text(selected_plan.get("period_end")),
            "target_value": _round(target_value),
            "metric": metric,
            "metric_label": PLAN_METRIC_LABELS.get(metric, ""),
        },
        "overall": {
            "closed_value": _round(closed_value),
            "remaining_value": _round(remaining_value),
            "progress_percent": progress_percent,
            "total_bl": total_bl,
            "plan_completed": bool(target_value and closed_value >= target_value),
            "overshoot_value": _round(max(closed_value - target_value, 0.0)),
        },
        "monthly": _monitor_monthly(all_time_sales_rows, metric),
        "departments": {
            "logists": {
                "closed_value": _round(logists_closed),
                "plan_share_percent": _round((logists_closed / target_value) * 100 if target_value else 0.0),
                "bl_count": logist_bl_count,
                "leaders": logist_leaders,
            },
            "sales": {
                "closed_value": _round(sales_closed),
                "plan_share_percent": _round((sales_closed / target_value) * 100 if target_value else 0.0),
                "bl_count": sales_bl_count,
                "leaders": sales_leaders,
            },
        },
        "last_updated": sync_status.get("last_sync_at", ""),
        "source_name": sync_status.get("source_name", ""),
    }


def get_sync_settings_payload() -> dict[str, Any]:
    payload = analytics_importer.get_sync_status()
    payload["plans"] = list_sales_plans()
    return payload


def list_sales_plans() -> list[dict[str, Any]]:
    plans = _fetch_table("analytics_sales_plans")
    plans.sort(
        key=lambda item: (
            _to_int(item.get("is_active")),
            _parse_date(item.get("period_start")) or date.min,
            _to_int(item.get("id")),
        ),
        reverse=True,
    )
    return plans


def save_sales_plan(payload: dict[str, Any]) -> dict[str, Any]:
    plan_id = _to_int(payload.get("id"))
    name = _clean_text(payload.get("name"))
    period_start = _clean_text(payload.get("period_start"))
    period_end = _clean_text(payload.get("period_end"))
    target_metric = _clean_text(payload.get("target_metric") or "amount_usd")
    if target_metric not in PLAN_METRIC_LABELS:
        target_metric = "amount_usd"
    target_value = _to_float(payload.get("target_value"))
    is_active = 1 if payload.get("is_active") else 0
    # Ombor sheet config — defaults match the real BURAQ Ombor sheet layout
    ombor_sheet_id = _clean_text(payload.get("ombor_sheet_id") or "")
    ombor_sheet_name = _clean_text(payload.get("ombor_sheet_name") or "Ombor")
    ombor_cbm_col = _clean_text(payload.get("ombor_cbm_col") or "V").upper()
    ombor_date_col = _clean_text(payload.get("ombor_date_col") or "Z").upper()
    ombor_seller_col = _clean_text(payload.get("ombor_seller_col") or "AG").upper()
    ombor_header_rows = max(0, _to_int(payload.get("ombor_header_rows") if payload.get("ombor_header_rows") is not None else 2))
    # FTL (full-truckload) second sheet config
    ftl_sheet_id     = _clean_text(payload.get("ftl_sheet_id") or "")
    ftl_sheet_gid    = _clean_text(payload.get("ftl_sheet_gid") or "")
    ftl_type_col     = _clean_text(payload.get("ftl_type_col")   or "J").upper()
    ftl_date_col     = _clean_text(payload.get("ftl_date_col")   or "L").upper()
    ftl_seller_col   = _clean_text(payload.get("ftl_seller_col") or "AB").upper()
    ftl_header_rows  = max(0, _to_int(payload.get("ftl_header_rows") if payload.get("ftl_header_rows") is not None else 1))
    ftl_cbm_per_truck = _to_float(payload.get("ftl_cbm_per_truck")) or 10.0
    if not name:
        raise ValueError("Plan nomi kiritilmagan")
    if not period_start or not period_end:
        raise ValueError("Plan davri kiritilmagan")

    conn = db.get_conn()
    try:
        if is_active:
            conn.execute("UPDATE analytics_sales_plans SET is_active = 0")
        if plan_id:
            conn.execute(
                """
                UPDATE analytics_sales_plans
                SET name = ?, period_start = ?, period_end = ?, target_amount_usd = ?, target_metric = ?, target_value = ?, is_active = ?,
                    ombor_sheet_id = ?, ombor_sheet_name = ?, ombor_cbm_col = ?, ombor_date_col = ?, ombor_seller_col = ?, ombor_header_rows = ?,
                    ftl_sheet_id = ?, ftl_sheet_gid = ?, ftl_type_col = ?, ftl_date_col = ?, ftl_seller_col = ?,
                    ftl_header_rows = ?, ftl_cbm_per_truck = ?,
                    updated_at = datetime('now','localtime')
                WHERE id = ?
                """,
                (
                    name, period_start, period_end,
                    target_value if target_metric == "amount_usd" else 0,
                    target_metric, target_value, is_active,
                    ombor_sheet_id, ombor_sheet_name, ombor_cbm_col, ombor_date_col, ombor_seller_col, ombor_header_rows,
                    ftl_sheet_id, ftl_sheet_gid, ftl_type_col, ftl_date_col, ftl_seller_col, ftl_header_rows, ftl_cbm_per_truck,
                    plan_id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO analytics_sales_plans(
                    name, period_start, period_end, target_amount_usd, target_metric, target_value, is_active,
                    ombor_sheet_id, ombor_sheet_name, ombor_cbm_col, ombor_date_col, ombor_seller_col, ombor_header_rows,
                    ftl_sheet_id, ftl_sheet_gid, ftl_type_col, ftl_date_col, ftl_seller_col, ftl_header_rows, ftl_cbm_per_truck,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))
                """,
                (
                    name, period_start, period_end,
                    target_value if target_metric == "amount_usd" else 0,
                    target_metric, target_value, is_active,
                    ombor_sheet_id, ombor_sheet_name, ombor_cbm_col, ombor_date_col, ombor_seller_col, ombor_header_rows,
                    ftl_sheet_id, ftl_sheet_gid, ftl_type_col, ftl_date_col, ftl_seller_col, ftl_header_rows, ftl_cbm_per_truck,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "plans": list_sales_plans()}


def activate_sales_plan(plan_id: int) -> dict[str, Any]:
    conn = db.get_conn()
    try:
        conn.execute("UPDATE analytics_sales_plans SET is_active = 0")
        conn.execute(
            "UPDATE analytics_sales_plans SET is_active = 1, updated_at = datetime('now','localtime') WHERE id = ?",
            (plan_id,),
        )
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "plans": list_sales_plans()}


def delete_sales_plan(plan_id: int) -> dict[str, Any]:
    conn = db.get_conn()
    try:
        conn.execute("DELETE FROM analytics_sales_plans WHERE id = ?", (plan_id,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "plans": list_sales_plans()}


def get_export_dataset(report_type: str, args: Any) -> tuple[str, list[dict[str, Any]]]:
    dataset = _load_dataset()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, dataset["plans"])
    filters = _apply_plan_dates(initial_filters, selected_plan)
    latest_statuses = _latest_status_map(dataset["statuses"])
    shipment_by_reys = _shipment_map(dataset["shipments"])
    logists_map = _logists_by_reys(dataset["logists"])

    report = _clean_text(report_type).lower()
    if report == "sales":
        rows = _filter_sales(dataset["sales"], filters, latest_statuses, shipment_by_reys, logists_map)
        return "analytics_sales_report", rows
    if report == "cashflow":
        rows = _filter_cashflow(dataset["cashflow"], filters)
        return "analytics_cashflow_report", rows
    if report == "managers":
        return "analytics_manager_kpi_report", get_managers(args)["table"]
    if report == "logists":
        return "analytics_logist_kpi_report", get_logists(args)["table"]
    if report == "debts":
        return "analytics_debts_report", get_debts(args)["table"]
    if report == "shipments":
        return "analytics_shipments_report", get_shipments(args)["table"]
    raise ValueError("Unknown report type")


PLAN_METRIC_LABELS["cbm"] = "m³"
STATUS_BUCKETS["nurjo‘li"] = "Qozog'iston"
STATUS_BUCKETS.pop("nurjoвЂli", None)


def _month_label(month_key: str) -> str:
    if not month_key:
        return "—"
    year, month = month_key.split("-")
    return f"{MONTH_NAMES.get(month, month)} {year}"


def _smart_insights(
    sales_rows: list[dict[str, Any]],
    cashflow_rows: list[dict[str, Any]],
    debt_rows: list[dict[str, Any]],
    shipment_rows: list[dict[str, Any]],
    manager_rows: list[dict[str, Any]],
) -> list[str]:
    insights: list[str] = []
    total_sales = _sum_sales(sales_rows)
    previous_start, previous_end = _previous_period_range(
        min((_parse_date(row.get("sale_date") or row.get("invoice_date")) for row in sales_rows if _parse_date(row.get("sale_date") or row.get("invoice_date"))), default=None),
        max((_parse_date(row.get("sale_date") or row.get("invoice_date")) for row in sales_rows if _parse_date(row.get("sale_date") or row.get("invoice_date"))), default=None),
    )
    previous_sales = _sales_period_total(sales_rows, previous_start, previous_end) if previous_start and previous_end else 0
    growth = _percent_change(total_sales, previous_sales)
    if growth is not None:
        direction = "oshdi" if growth >= 0 else "kamaydi"
        insights.append(f"📈 Savdo o‘tgan davrga nisbatan {abs(growth):.1f}% ga {direction}.")

    if debt_rows:
        biggest = max(debt_rows, key=lambda item: _to_float(item.get("debt_amount")))
        if _to_float(biggest.get("debt_amount")) > 0:
            insights.append(
                f"⚠️ Eng katta qarz: {biggest.get('client_name') or biggest.get('shipping_mark')} — {_format_money(_to_float(biggest.get('debt_amount')))}."
            )

    if manager_rows:
        top_manager = max(manager_rows, key=lambda item: _to_float(item.get("value") or item.get("sales_amount")))
        insights.append(f"🏆 Oy bo‘yicha eng yaxshi menejer: {top_manager.get('manager_name')}.")

    if shipment_rows:
        status_counts = Counter()
        for row in shipment_rows:
            label = _normalize_status_bucket(_resolve_shipment_status(row, None))
            if label:
                status_counts[label] += 1
        if status_counts:
            label, _value = max(status_counts.items(), key=lambda item: item[1])
            insights.append(f"🚛 Eng ko‘p aktiv BL hozir {label} bosqichida.")

    income = _sum_cashflow_usd(cashflow_rows, "income")
    if income < total_sales and total_sales > 0:
        insights.append("📉 Kirim kamaygan, lekin BL soni oshgan.")

    return insights[:5]


def get_overview(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, dataset["plans"])
    filters = _apply_plan_dates(initial_filters, selected_plan)
    latest_statuses = _latest_status_map(dataset["statuses"])
    shipment_by_reys = _shipment_map(dataset["shipments"])
    logists_map = _logists_by_reys(dataset["logists"])

    sales_rows = _filter_sales(dataset["sales"], filters, latest_statuses, shipment_by_reys, logists_map)
    cashflow_rows = _filter_cashflow(dataset["cashflow"], filters)
    shipment_rows = _filter_shipments(dataset["shipments"], filters, latest_statuses, sales_rows, logists_map)
    debts_rows = _debts_raw(sales_rows, cashflow_rows)

    total_sales = _sum_sales(sales_rows)
    previous_start, previous_end = _previous_period_range(filters.date_from, filters.date_to)
    previous_sales = _sales_period_total(dataset["sales"], previous_start, previous_end)
    growth = _percent_change(total_sales, previous_sales)
    income = _sum_cashflow_usd(cashflow_rows, "income")
    expense = _sum_cashflow_usd(cashflow_rows, "expense")
    profit = income - expense
    margin = (profit / income * 100.0) if income else 0.0
    total_debt = sum(max(_to_float(item.get("debt_amount")), 0.0) for item in debts_rows)
    distinct_bl = {_clean_text(row.get("shipping_mark")).upper() for row in sales_rows if _clean_text(row.get("shipping_mark"))}
    arrived_shipments = [row for row in shipment_rows if _clean_text(row.get("distributed_date")) or _clean_text(row.get("tashkent_date"))]
    delayed_shipments = _delayed_shipments(shipment_rows)
    average_deal = total_sales / max(len(distinct_bl), 1) if distinct_bl else 0.0
    managers_rows = _sales_by_manager(sales_rows, "amount_usd")
    sync_status = analytics_importer.get_sync_status()

    return {
        "filters": _build_filter_options(dataset),
        "selected_filters": _selected_filters_payload(filters),
        "plans": list_sales_plans(),
        "selected_plan": selected_plan,
        "kpis": {
            "total_sales": {"value": total_sales, "display": _format_money(total_sales)},
            "monthly_growth": {"value": growth or 0, "display": "—" if growth is None else f"{growth:.1f}%"},
            "income": {"value": income, "display": _format_money(income)},
            "expense": {"value": expense, "display": _format_money(expense)},
            "profit": {"value": profit, "display": _format_money(profit), "note": f"Margin {margin:.1f}%"},
            "debt": {"value": total_debt, "display": _format_money(total_debt)},
            "active_bl_count": {"value": len(distinct_bl), "display": str(len(distinct_bl))},
            "arrived_shipments_count": {"value": len(arrived_shipments), "display": str(len(arrived_shipments))},
            "delayed_shipments_count": {"value": len(delayed_shipments), "display": str(len(delayed_shipments))},
            "average_deal": {"value": average_deal, "display": _format_money(average_deal)},
        },
        "meta": {
            "has_data": bool(dataset["sales"] or dataset["cashflow"] or dataset["shipments"]),
            "base_currency": BASE_CURRENCY,
            "last_sync_at": sync_status.get("last_sync_at", ""),
            "source_name": sync_status.get("source_name", ""),
            "missing_currencies": _missing_currencies(cashflow_rows),
        },
        "smart_insights": _smart_insights(sales_rows, cashflow_rows, debts_rows, shipment_rows, managers_rows),
        "empty": not bool(dataset["sales"] or dataset["cashflow"]),
    }


def get_managers(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, dataset["plans"])
    filters = _apply_plan_dates(initial_filters, selected_plan)
    latest_statuses = _latest_status_map(dataset["statuses"])
    shipment_by_reys = _shipment_map(dataset["shipments"])
    logists_map = _logists_by_reys(dataset["logists"])

    sales_rows = _filter_sales(dataset["sales"], filters, latest_statuses, shipment_by_reys, logists_map)
    cashflow_rows = _filter_cashflow(dataset["cashflow"], filters)
    shipment_rows = _filter_shipments(dataset["shipments"], filters, latest_statuses, sales_rows, logists_map)

    base_rows = _sales_by_manager(sales_rows, "amount_usd")
    debts_rows = _debts_raw(sales_rows, cashflow_rows)
    debt_by_manager: dict[str, dict[str, float]] = defaultdict(lambda: {"debt": 0.0, "paid": 0.0})
    for row in debts_rows:
        manager = _clean_text(row.get("salesperson")) or "Belgilanmagan"
        debt_by_manager[manager]["debt"] += _to_float(row.get("debt_amount"))
        debt_by_manager[manager]["paid"] += _to_float(row.get("paid_amount"))

    delayed_reys = {_clean_text(item.get("reys_number")) for item in _delayed_shipments(shipment_rows)}
    delayed_by_manager = Counter()
    for row in sales_rows:
        manager = _clean_text(row.get("salesperson"))
        if manager and _clean_text(row.get("reys_number")) in delayed_reys:
            delayed_by_manager[manager] += 1

    table = []
    for row in base_rows:
        manager = row["manager_name"]
        sale_amount = _to_float(row.get("value"))
        paid_amount = debt_by_manager[manager]["paid"]
        debt_amount = debt_by_manager[manager]["debt"]
        related_sales = [item for item in sales_rows if _clean_text(item.get("salesperson")) == manager]
        profit_value = sum(
            _to_float(item.get("final_sale_amount"))
            - _to_float(item.get("customs_payment"))
            - _to_float(item.get("company_expense"))
            - _to_float(item.get("certificate_expense"))
            for item in related_sales
        )
        if debt_amount <= 0:
            status = "To'liq yopilgan"
        elif paid_amount > 0:
            status = "Qisman to'langan"
        else:
            status = "Qarzdor"
        table.append(
            {
                "manager_name": manager,
                "sales_total": row["display_value"],
                "sales_amount": sale_amount,
                "bl_count": row["bl_count"],
                "client_count": row["client_count"],
                "paid_amount": _format_money(paid_amount),
                "debt_amount": _format_money(debt_amount),
                "average_check": row["average_deal"],
                "profit": _format_money(profit_value),
                "profit_value": _round(profit_value),
                "status": status,
                "late_count": delayed_by_manager.get(manager, 0),
                "cbm": _round(row["cbm"]),
                "gross_weight": _round(row["gross_weight"]),
                "share_percent": row["share_percent"],
                "debt_amount_value": _round(debt_amount),
                "paid_amount_value": _round(paid_amount),
            }
        )
    table.sort(key=lambda item: item["sales_amount"], reverse=True)

    ranking = []
    if table:
        ranking.append(f"🏆 Eng ko‘p savdo: {table[0]['manager_name']} — {table[0]['sales_total']}")
        ranking.append(f"📦 Eng ko‘p BL: {max(table, key=lambda item: item['bl_count'])['manager_name']}")
        ranking.append(f"⚠️ Eng ko‘p qarz: {max(table, key=lambda item: item['debt_amount_value'])['manager_name']}")
        ranking.append(f"🚛 Eng ko‘p kechikish: {max(table, key=lambda item: item['late_count'])['manager_name']}")

    return {
        "ranking": ranking,
        "leaders": table[:5],
        "table": table,
        "empty": not bool(table),
    }


def get_monitor(args: Any) -> dict[str, Any]:
    from services import ombor_service

    plans = list_sales_plans()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, plans)
    if not selected_plan:
        return {"empty": True, "message": "Avval sales plan tanlang yoki yarating."}

    metric = _clean_text(args.get("metric") or selected_plan.get("target_metric") or "cbm")
    if metric not in PLAN_METRIC_LABELS:
        metric = "cbm"

    plan_filters = _apply_plan_dates(initial_filters, selected_plan)
    target_value = _to_float(selected_plan.get("target_value") or selected_plan.get("target_amount_usd"))
    ombor_sheet_id = _clean_text(selected_plan.get("ombor_sheet_id") or "")

    # --- Live Ombor sheet mode ---
    if ombor_sheet_id:
        force = bool(args.get("force"))
        try:
            # Auto-correct: plans saved before the column-letter fix used the old
            # (wrong-by-one) defaults W/AA/AH. Map those to the real V/Z/AG.
            cbm_col_stored    = _clean_text(selected_plan.get("ombor_cbm_col") or "")
            date_col_stored   = _clean_text(selected_plan.get("ombor_date_col") or "")
            seller_col_stored = _clean_text(selected_plan.get("ombor_seller_col") or "")
            if cbm_col_stored.upper() == "W" and date_col_stored.upper() == "AA" and seller_col_stored.upper() == "AH":
                cbm_col_stored, date_col_stored, seller_col_stored = "V", "Z", "AG"

            # Read all 3 LTL pipeline stages (Ombor → Ortilgan furalar → Yetib keldi)
            # and merge by seller name. Stage-1 (Ombor) columns come from the plan
            # config; the other two stages use a hard-coded layout (see LTL_PIPELINE_STAGES).
            ombor = ombor_service.fetch_combined_ltl_data(
                sheet_id=ombor_sheet_id,
                primary_sheet_name=_clean_text(selected_plan.get("ombor_sheet_name") or "Ombor"),
                primary_cbm_col=cbm_col_stored or "V",
                primary_date_col=date_col_stored or "Z",
                primary_seller_col=seller_col_stored or "AG",
                primary_logist_col=_clean_text(selected_plan.get("ombor_logist_col") or "AH"),
                primary_bl_col=_clean_text(selected_plan.get("ombor_bl_col") or "E"),
                primary_header_rows=max(0, _to_int(selected_plan.get("ombor_header_rows") if selected_plan.get("ombor_header_rows") is not None else 2)),
                date_from=plan_filters.date_from,
                date_to=plan_filters.date_to,
                force=force,
            )
        except Exception as exc:
            return {"empty": True, "message": f"Ombor sheet xatosi: {exc}"}

        closed_value = ombor["total_cbm"]
        total_bl = ombor["total_bl"]

        # ─────────────────────────────────────────────────────────────────
        # FTL (full-truckload) sales — second Google Sheet.
        # Classification rule:
        #   • If the seller-column name matches one of LOGIST_NAMES (set below)
        #     → LOGISTIKA BO'LIMI panel  (display in truck COUNT only,
        #                                  does NOT contribute to plan progress)
        #   • Otherwise → SAVDO BO'LIMI FTL sub-view
        #     (truck count displayed; trucks × m³/truck DOES contribute to plan)
        # ─────────────────────────────────────────────────────────────────
        LOGIST_NAMES = (
            "SAYFULLAYEV ABDULLOH ORIFJON O'G'LI",
            "O'KTAMOV MAQSUDXO'JA MAXAMAT O'G'LI",
            "ABDULLAYEV IBROHIM ABDUSATTOR O'G'LI",
        )
        def _norm_person(name: str) -> str:
            """Fuzzy match: lowercase, strip all apostrophe variants, collapse whitespace."""
            if not name:
                return ""
            n = name.casefold()
            for ch in ("ʻ", "ʼ", "'", "`", "‘", "’"):
                n = n.replace(ch, "")
            return " ".join(n.split()).strip()
        LOGIST_SET = {_norm_person(n) for n in LOGIST_NAMES}

        ftl_diag = None
        ftl_savdo_leaders: list[dict[str, Any]] = []   # SAVDO FTL sub-view
        ftl_logist_leaders: list[dict[str, Any]] = []  # LOGISTIKA panel
        ftl_savdo_total_trucks = 0.0
        ftl_savdo_total_bl = 0
        ftl_savdo_total_cbm = 0.0
        ftl_logist_total_trucks = 0.0
        ftl_logist_total_bl = 0

        ftl_sheet_id = _clean_text(selected_plan.get("ftl_sheet_id") or "1Ud46UlcezyxnO-PHbx60K0wzLgxGPVleOmPBDQdV9-I")
        ftl_gid      = _clean_text(selected_plan.get("ftl_sheet_gid") or "619267330")
        ftl_cbm_per_truck = _to_float(selected_plan.get("ftl_cbm_per_truck")) or 10.0

        if ftl_sheet_id:
            try:
                ftl = ombor_service.fetch_ftl_data(
                    sheet_id=ftl_sheet_id,
                    gid=ftl_gid,
                    type_col=_clean_text(selected_plan.get("ftl_type_col")   or "J"),
                    date_col=_clean_text(selected_plan.get("ftl_date_col")   or "L"),
                    seller_col=_clean_text(selected_plan.get("ftl_seller_col") or "AB"),
                    header_rows=max(0, _to_int(selected_plan.get("ftl_header_rows") if selected_plan.get("ftl_header_rows") is not None else 1)),
                    date_from=plan_filters.date_from,
                    date_to=plan_filters.date_to,
                    force=force,
                )
                ftl_diag = ftl.get("diagnostics")

                seller_rows = []
                logist_rows = []
                for ftl_name, ftl_info in ftl.get("by_seller", {}).items():
                    norm = _norm_person(ftl_name)
                    trucks = float(ftl_info["trucks"])
                    bl = int(ftl_info["bl"])
                    row = {"name": ftl_name, "trucks": trucks, "bl": bl}
                    if norm in LOGIST_SET:
                        logist_rows.append(row)
                        ftl_logist_total_trucks += trucks
                        ftl_logist_total_bl += bl
                    else:
                        seller_rows.append(row)
                        ftl_savdo_total_trucks += trucks
                        ftl_savdo_total_bl += bl

                seller_rows.sort(key=lambda x: x["trucks"], reverse=True)
                logist_rows.sort(key=lambda x: x["trucks"], reverse=True)

                def _build_ftl_leaders(rows: list[dict[str, Any]], total_trucks: float) -> list[dict[str, Any]]:
                    out: list[dict[str, Any]] = []
                    for r in rows:
                        share = (r["trucks"] / total_trucks * 100) if total_trucks else 0
                        name = r["name"]
                        out.append({
                            "name": name,
                            "initials": "".join(p[0].upper() for p in name.split()[:2] if p) or "?",
                            "value": round(r["trucks"], 2),
                            "bl_count": r["bl"],
                            "share_percent": round(share, 1),
                            "unit": "fura",
                        })
                    return out

                ftl_savdo_leaders  = _build_ftl_leaders(seller_rows,  ftl_savdo_total_trucks)
                ftl_logist_leaders = _build_ftl_leaders(logist_rows, ftl_logist_total_trucks)

                # Plan progress: ONLY SAVDO FTL m³ adds. Logist FTL is shown but doesn't count.
                ftl_savdo_total_cbm = ftl_savdo_total_trucks * ftl_cbm_per_truck
                closed_value += ftl_savdo_total_cbm

                if ftl_diag is not None:
                    ftl_diag["savdo_trucks"]  = round(ftl_savdo_total_trucks, 2)
                    ftl_diag["savdo_bl"]      = ftl_savdo_total_bl
                    ftl_diag["savdo_cbm_added"] = round(ftl_savdo_total_cbm, 2)
                    ftl_diag["logist_trucks"] = round(ftl_logist_total_trucks, 2)
                    ftl_diag["logist_bl"]     = ftl_logist_total_bl
                    ftl_diag["logist_names_recognized"] = [r["name"] for r in logist_rows]
            except Exception as exc:
                # Graceful: FTL is optional, fall back to Ombor-only data
                ftl_diag = {"error": str(exc)}

        remaining_value = max(target_value - closed_value, 0.0)
        progress_percent = _round((closed_value / target_value) * 100 if target_value else 0.0, 2)

        def _to_leader(person: dict[str, Any]) -> dict[str, Any]:
            name = _clean_text(person["name"])
            return {
                "name": name,
                "initials": "".join(p[0].upper() for p in name.split()[:2] if p) or "?",
                "value": _round(person["cbm"]),
                "bl_count": _to_int(person["bl_count"]),
                "share_percent": _round(person["share_percent"]),
            }

        seller_leaders = [_to_leader(s) for s in ombor.get("sellers", [])]
        logist_leaders = [_to_leader(l) for l in ombor.get("logists", [])]

        # ─────────────────────────────────────────────────────────────────
        # Oylik dinamika ("Monthly dynamics" panel) — independent of the
        # active plan's date range. The active plan only filters the totals
        # / leaders. The monthly history bar chart shows the last ~24 months
        # of CBM regardless of which plan happens to be active right now,
        # so a plan that closed last month (or finished at >100%) still
        # appears in the historical bars.
        #
        # IMPORTANT: use Tashkent-local "today" — Railway containers run in
        # UTC, so `date.today()` shifts forward/back by up to 5 hours at
        # midnight local. For a monitor that's expected to reflect the
        # current month accurately the moment a plan period rolls over,
        # we need the calendar that the operators are looking at.
        # Default values BEFORE the try block so the diagnostics block at
        # the bottom can always reference them even if the fetch raises.
        history_error: str | None = None
        ombor_history: dict[str, Any] | None = None
        try:
            today = ombor_service.datetime.now(ombor_service.TASHKENT_TZ).date()
        except Exception:
            today = date.today()
        month_start = today.replace(day=1)
        wide_from = date(month_start.year - 2, month_start.month, 1)
        wide_to = today

        try:
            # Span 24 months back from the first of THIS month. Generous
            # so that no boundary issue can lose a recent month — the
            # frontend only renders months where there's actual data.
            ombor_history = ombor_service.fetch_combined_ltl_data(
                sheet_id=ombor_sheet_id,
                primary_sheet_name=_clean_text(selected_plan.get("ombor_sheet_name") or "Ombor"),
                primary_cbm_col=cbm_col_stored or "V",
                primary_date_col=date_col_stored or "Z",
                primary_seller_col=seller_col_stored or "AG",
                primary_logist_col=_clean_text(selected_plan.get("ombor_logist_col") or "AH"),
                primary_bl_col=_clean_text(selected_plan.get("ombor_bl_col") or "E"),
                primary_header_rows=max(0, _to_int(selected_plan.get("ombor_header_rows") if selected_plan.get("ombor_header_rows") is not None else 2)),
                date_from=wide_from,
                date_to=wide_to,
                force=force,
            )
            monthly_source = ombor_history.get("monthly") or []
        except Exception as exc:
            # If the history fetch fails, fall back to the active-plan
            # monthly so we don't blank the panel entirely.
            history_error = str(exc)
            monthly_source = ombor.get("monthly", []) or []

        # ─────────────────────────────────────────────────────────────────
        # FTL history — 24-month window of full-truckload sales so the
        # Oylik dinamika bars can show SAVDO trucks for previous months
        # (not just the active plan's month). Same caching as the LTL
        # history fetch — 30s TTL keyed on the wide range.
        ftl_history_by_month: dict[str, dict[str, dict[str, Any]]] = {}
        if ftl_sheet_id:
            try:
                ftl_history = ombor_service.fetch_ftl_data(
                    sheet_id=ftl_sheet_id,
                    gid=ftl_gid,
                    type_col=_clean_text(selected_plan.get("ftl_type_col")   or "J"),
                    date_col=_clean_text(selected_plan.get("ftl_date_col")   or "L"),
                    seller_col=_clean_text(selected_plan.get("ftl_seller_col") or "AB"),
                    header_rows=max(0, _to_int(selected_plan.get("ftl_header_rows") if selected_plan.get("ftl_header_rows") is not None else 1)),
                    date_from=wide_from,
                    date_to=wide_to,
                    force=force,
                )
                ftl_history_by_month = ftl_history.get("by_month_seller") or {}
            except Exception:
                # FTL is optional — silently fall back to LTL-only monthly bars.
                ftl_history_by_month = {}

        # For each month, split FTL trucks into:
        #   SAVDO  — counts toward the bar's m³ (1 fura = ftl_cbm_per_truck)
        #   LOGISTIKA — counts only as a separate truck number on the bar
        #               (per user request: shown but NOT multiplied into m³)
        def _split_ftl_month(month_sellers: dict[str, dict[str, Any]]) -> tuple[float, float, int, float, int]:
            """Return (savdo_trucks, savdo_cbm, savdo_bl, log_trucks, log_bl)."""
            sv_trucks = 0.0
            sv_bl = 0
            lg_trucks = 0.0
            lg_bl = 0
            for raw_name, info in (month_sellers or {}).items():
                trucks = float(info.get("trucks") or 0)
                bl = int(info.get("bl") or 0)
                if _norm_person(raw_name) in LOGIST_SET:
                    lg_trucks += trucks
                    lg_bl     += bl
                else:
                    sv_trucks += trucks
                    sv_bl     += bl
            return sv_trucks, sv_trucks * ftl_cbm_per_truck, sv_bl, lg_trucks, lg_bl

        # Build the monthly array — LTL m³ + SAVDO FTL m³, plus separate
        # SAVDO and LOGISTIKA truck counts for the bar.
        monthly = []
        for m in monthly_source:
            ym = m["month"]
            ltl_cbm = float(m.get("cbm") or 0)
            sv_trucks, sv_cbm, _sv_bl, lg_trucks, _lg_bl = _split_ftl_month(
                ftl_history_by_month.get(ym, {})
            )
            monthly.append({
                "month": ym,
                "label": m.get("label") or ym,
                "value":           _round(ltl_cbm + sv_cbm),  # LTL + SAVDO FTL m³ only
                "ltl_cbm":         _round(ltl_cbm),
                "savdo_ftl_cbm":   _round(sv_cbm),
                "savdo_trucks":    _round(sv_trucks),         # contributes to m³
                "logistika_trucks": _round(lg_trucks),        # display-only, NOT in m³
                "bl_count":        _to_int(m.get("bl_count")),  # unique Ombor BLs only
            })

        # ─────────────────────────────────────────────────────────────────
        # Plan-vs-sheet "why is my plan empty?" hint.
        # When the active plan resolves to 0 rows in the configured period
        # but the wide-range fetch DID find rows, the operator is usually
        # confused about which window the plan covers vs where their
        # sheet data actually lives. We compute a one-shot status string
        # the UI can render as a yellow banner so they don't have to
        # inspect the diagnostics blob.
        plan_data_status: dict[str, Any] = {"state": "ok"}
        try:
            plan_period_start = _clean_text(selected_plan.get("period_start"))
            plan_period_end   = _clean_text(selected_plan.get("period_end"))
            active_diag = ombor.get("diagnostics") or {}
            per_stage_active = active_diag.get("per_stage") or {}
            # rows_used across the active-plan stages (already filtered)
            active_rows_used = sum(
                int((s or {}).get("rows_used") or 0)
                for s in per_stage_active.values()
                if isinstance(s, dict)
            )
            history_total_cbm = 0.0
            history_latest_month = ""
            if ombor_history:
                history_total_cbm = float(ombor_history.get("total_cbm") or 0)
                hist_monthly = ombor_history.get("monthly") or []
                if hist_monthly:
                    last = hist_monthly[-1]
                    history_latest_month = last.get("label") or last.get("month") or ""
            # Active plan saw zero rows? Was the wide-range fetch empty too?
            if active_rows_used == 0 and history_total_cbm > 0:
                plan_data_status = {
                    "state": "empty_in_period",
                    "plan_period_start": plan_period_start,
                    "plan_period_end":   plan_period_end,
                    "history_latest_month": history_latest_month,
                    "message": (
                        f"План «{_clean_text(selected_plan.get('name'))}» "
                        f"({plan_period_start} → {plan_period_end}) пуст: "
                        f"в Google Sheet нет строк с датами в этом периоде. "
                        f"Последние данные — {history_latest_month}."
                    ),
                }
            elif active_rows_used == 0 and history_total_cbm == 0:
                plan_data_status = {
                    "state": "sheet_empty",
                    "plan_period_start": plan_period_start,
                    "plan_period_end":   plan_period_end,
                    "message": (
                        "Google Sheet не вернул ни одной строки за последние 24 месяца. "
                        "Проверь настройки колонок (CBM=V, SANA=Z, SOTUVCHI=AG) "
                        "и доступ к таблице по ссылке."
                    ),
                }
        except Exception:
            plan_data_status = {"state": "ok"}

        return {
            "empty": False,
            "data_source": "ombor_live",
            "plan": {
                "id": _to_int(selected_plan.get("id")),
                "name": _clean_text(selected_plan.get("name")),
                "period_start": _clean_text(selected_plan.get("period_start")),
                "period_end": _clean_text(selected_plan.get("period_end")),
                "target_value": _round(target_value),
                "metric": "cbm",
                "metric_label": "m³",
            },
            "overall": {
                "closed_value": _round(closed_value),
                "remaining_value": _round(remaining_value),
                "progress_percent": progress_percent,
                "total_bl": total_bl,
                "plan_completed": bool(target_value and closed_value >= target_value),
                "overshoot_value": _round(max(closed_value - target_value, 0.0)),
            },
            "monthly": monthly,
            # Two panels rotate on the monitor:
            #   "logists" key  → SAVDO BO'LIMI    (Ombor sellers + FTL non-logist names)
            #     · leaders         = LTL view (Ombor m³)
            #     · ftl.leaders     = FTL view (truck count, sellers only)
            #   "sales"   key  → LOGISTIKA BO'LIMI (the 3 hardcoded logist names from FTL.AB)
            #                    DOES NOT contribute to plan progress. Display in truck count only.
            "departments": {
                "logists": {
                    "closed_value": _round(ombor["total_cbm"]),       # LTL total (pure Ombor m³)
                    "plan_share_percent": _round((ombor["total_cbm"] / target_value * 100) if target_value else 0, 2),
                    "bl_count": ombor["total_bl"],
                    "leaders": seller_leaders,
                    "ftl": {
                        "total_trucks": _round(ftl_savdo_total_trucks),
                        "total_bl": ftl_savdo_total_bl,
                        "total_cbm": _round(ftl_savdo_total_cbm),
                        "leaders": ftl_savdo_leaders,
                    },
                },
                "sales": {
                    "display_mode": "ftl_only",                 # frontend signal: render trucks not m³
                    "total_trucks": _round(ftl_logist_total_trucks),
                    "total_bl": ftl_logist_total_bl,
                    "leaders": ftl_logist_leaders,
                    # Legacy fields kept for compat (will be ignored in ftl_only mode)
                    "closed_value": 0,
                    "plan_share_percent": 0,
                    "bl_count": ftl_logist_total_bl,
                },
            },
            "last_updated": ombor["fetched_at"],
            "source_name": "Google Sheets - " + str(selected_plan.get("ombor_sheet_name") or "Ombor"),
            "plan_data_status": plan_data_status,
            "diagnostics": ombor.get("diagnostics"),
            "ftl_diagnostics": ftl_diag,
            "history_diagnostics": {
                # What date window did the history fetch actually cover?
                "wide_from": wide_from.isoformat(),
                "wide_to":   wide_to.isoformat(),
                "today":     today.isoformat(),
                # Which months came back (with non-zero CBM)?
                "months_present": [m.get("month") for m in monthly_source if (m.get("cbm") or 0) > 0],
                "month_count":    len(monthly_source),
                "error":          history_error or "",
                # Per-stage row counts so the user can see which tab
                # contributed to the history aggregate.
                "stages":         (
                    (ombor_history.get("diagnostics") or {}).get("per_stage")
                    if ombor_history else None
                ),
            },
            "ombor_config": {
                "cbm_col": cbm_col_stored or "V",
                "date_col": date_col_stored or "Z",
                "seller_col": seller_col_stored or "AG",
                "sheet_name": _clean_text(selected_plan.get("ombor_sheet_name") or "Ombor"),
                "header_rows": max(0, _to_int(selected_plan.get("ombor_header_rows") if selected_plan.get("ombor_header_rows") is not None else 2)),
            },
        }

    # --- Fallback: existing analytics DB ---
    dataset = _load_dataset()
    if not dataset["sales"]:
        return {"empty": True, "message": "Google Sheets ma’lumotlari hali import qilinmagan."}

    latest_statuses = _latest_status_map(dataset["statuses"])
    shipment_by_reys = _shipment_map(dataset["shipments"])
    logists_map = _logists_by_reys(dataset["logists"])

    plan_sales_rows = _filter_sales(dataset["sales"], plan_filters, latest_statuses, shipment_by_reys, logists_map)
    all_time_sales_rows = _filter_sales(
        dataset["sales"],
        _filters_without_dates(plan_filters),
        latest_statuses,
        shipment_by_reys,
        logists_map,
    )
    logist_rows = _filter_logists(dataset["logists"], _filters_without_dates(plan_filters))

    closed_value = _plan_metric_value(metric, plan_sales_rows)
    remaining_value = max(target_value - closed_value, 0.0)
    progress_percent = _round((closed_value / target_value) * 100 if target_value else 0.0, 2)
    total_bl = len({_clean_text(row.get("shipping_mark")).upper() for row in plan_sales_rows if _clean_text(row.get("shipping_mark"))})
    logists_closed, logist_leaders, logist_bl_count = _monitor_logist_leaders(plan_sales_rows, logist_rows, metric, target_value or 1.0)
    sales_closed, sales_leaders, sales_bl_count = _monitor_sales_leaders(plan_sales_rows, metric, target_value or 1.0)
    sync_status = analytics_importer.get_sync_status()

    return {
        "empty": False,
        "data_source": "analytics_db",
        "plan": {
            "id": _to_int(selected_plan.get("id")),
            "name": _clean_text(selected_plan.get("name")),
            "period_start": _clean_text(selected_plan.get("period_start")),
            "period_end": _clean_text(selected_plan.get("period_end")),
            "target_value": _round(target_value),
            "metric": metric,
            "metric_label": PLAN_METRIC_LABELS.get(metric, ""),
        },
        "overall": {
            "closed_value": _round(closed_value),
            "remaining_value": _round(remaining_value),
            "progress_percent": progress_percent,
            "total_bl": total_bl,
            "plan_completed": bool(target_value and closed_value >= target_value),
            "overshoot_value": _round(max(closed_value - target_value, 0.0)),
        },
        "monthly": _monitor_monthly(all_time_sales_rows, metric),
        "departments": {
            "logists": {
                "closed_value": _round(logists_closed),
                "plan_share_percent": _round((logists_closed / target_value) * 100 if target_value else 0.0),
                "bl_count": logist_bl_count,
                "leaders": logist_leaders,
            },
            "sales": {
                "closed_value": _round(sales_closed),
                "plan_share_percent": _round((sales_closed / target_value) * 100 if target_value else 0.0),
                "bl_count": sales_bl_count,
                "leaders": sales_leaders,
            },
        },
        "last_updated": sync_status.get("last_sync_at", ""),
        "source_name": sync_status.get("source_name", ""),
    }


# ──────────────────────────────────────────────────────────────────────────
# /analytics/api/monitor/month/<YYYY-MM> click-popup endpoint
# Returns SAVDO + LOGISTIKA leaderboards for one specific calendar month,
# using the wide-range LTL + FTL fetches as data sources (so it almost
# always hits the same 30-sec cache the main monitor warmed).
# ──────────────────────────────────────────────────────────────────────────
def get_monitor_month_breakdown(ym: str, args: Any) -> dict[str, Any]:
    from services import ombor_service

    ym_clean = str(ym or "").strip()
    if not re.match(r"^\d{4}-\d{2}$", ym_clean):
        raise ValueError("ym должен быть в формате YYYY-MM")

    plans = list_sales_plans()
    initial_filters = parse_filters(args)
    selected_plan = _get_selected_plan(initial_filters, plans)
    if not selected_plan:
        return {"empty": True, "message": "Avval sales plan tanlang yoki yarating."}

    ombor_sheet_id = _clean_text(selected_plan.get("ombor_sheet_id") or "")
    if not ombor_sheet_id:
        return {"empty": True, "message": "Ombor sheet не настроен у текущего плана."}

    cbm_col_stored    = _clean_text(selected_plan.get("ombor_cbm_col") or "")
    date_col_stored   = _clean_text(selected_plan.get("ombor_date_col") or "")
    seller_col_stored = _clean_text(selected_plan.get("ombor_seller_col") or "")
    if cbm_col_stored.upper() == "W" and date_col_stored.upper() == "AA" and seller_col_stored.upper() == "AH":
        cbm_col_stored, date_col_stored, seller_col_stored = "V", "Z", "AG"

    try:
        today = ombor_service.datetime.now(ombor_service.TASHKENT_TZ).date()
    except Exception:
        today = date.today()
    month_start = today.replace(day=1)
    wide_from = date(month_start.year - 2, month_start.month, 1)
    wide_to = today

    try:
        ombor_history = ombor_service.fetch_combined_ltl_data(
            sheet_id=ombor_sheet_id,
            primary_sheet_name=_clean_text(selected_plan.get("ombor_sheet_name") or "Ombor"),
            primary_cbm_col=cbm_col_stored or "V",
            primary_date_col=date_col_stored or "Z",
            primary_seller_col=seller_col_stored or "AG",
            primary_logist_col=_clean_text(selected_plan.get("ombor_logist_col") or "AH"),
            primary_bl_col=_clean_text(selected_plan.get("ombor_bl_col") or "E"),
            primary_header_rows=max(0, _to_int(selected_plan.get("ombor_header_rows") if selected_plan.get("ombor_header_rows") is not None else 2)),
            date_from=wide_from,
            date_to=wide_to,
            force=False,
        )
    except Exception as exc:
        return {"empty": True, "message": f"Ombor sheet xatosi: {exc}"}

    LOGIST_NAMES = (
        "SAYFULLAYEV ABDULLOH ORIFJON O'G'LI",
        "O'KTAMOV MAQSUDXO'JA MAXAMAT O'G'LI",
        "ABDULLAYEV IBROHIM ABDUSATTOR O'G'LI",
    )
    def _norm_person(name: str) -> str:
        if not name:
            return ""
        n = name.casefold()
        for ch in ("ʻ", "ʼ", "'", "`", "‘", "’"):
            n = n.replace(ch, "")
        return " ".join(n.split()).strip()
    LOGIST_SET = {_norm_person(n) for n in LOGIST_NAMES}

    ltl_month_map = (ombor_history.get("monthly_sellers") or {}).get(ym_clean, {})

    ftl_month_map: dict[str, dict[str, Any]] = {}
    ftl_sheet_id = _clean_text(selected_plan.get("ftl_sheet_id") or "1Ud46UlcezyxnO-PHbx60K0wzLgxGPVleOmPBDQdV9-I")
    ftl_gid      = _clean_text(selected_plan.get("ftl_sheet_gid") or "619267330")
    ftl_cbm_per_truck = _to_float(selected_plan.get("ftl_cbm_per_truck")) or 10.0
    if ftl_sheet_id:
        try:
            ftl_history = ombor_service.fetch_ftl_data(
                sheet_id=ftl_sheet_id,
                gid=ftl_gid,
                type_col=_clean_text(selected_plan.get("ftl_type_col")   or "J"),
                date_col=_clean_text(selected_plan.get("ftl_date_col")   or "L"),
                seller_col=_clean_text(selected_plan.get("ftl_seller_col") or "AB"),
                header_rows=max(0, _to_int(selected_plan.get("ftl_header_rows") if selected_plan.get("ftl_header_rows") is not None else 1)),
                date_from=wide_from,
                date_to=wide_to,
                force=False,
            )
            ftl_month_map = (ftl_history.get("by_month_seller") or {}).get(ym_clean, {})
        except Exception:
            ftl_month_map = {}

    ltl_by_key: dict[str, dict[str, Any]] = {}
    for skey, sval in ltl_month_map.items():
        ltl_by_key[skey] = {
            "name": sval.get("name") or "",
            "ltl_cbm": float(sval.get("cbm") or 0),
            "ltl_bl": int(sval.get("bl_count") or 0),
            "ftl_trucks": 0.0,
            "ftl_bl": 0,
        }

    logistika_rows: list[dict[str, Any]] = []
    for raw_ftl_name, info in (ftl_month_map or {}).items():
        norm_key = _norm_person(raw_ftl_name)
        if not norm_key:
            continue
        trucks = float(info.get("trucks") or 0)
        bl = int(info.get("bl") or 0)
        if norm_key in LOGIST_SET:
            logistika_rows.append({
                "name": raw_ftl_name,
                "ftl_trucks": round(trucks, 2),
                "ftl_bl": bl,
            })
            continue
        seller = ltl_by_key.get(norm_key)
        if seller is None:
            seller = {
                "name": raw_ftl_name,
                "ltl_cbm": 0.0,
                "ltl_bl": 0,
                "ftl_trucks": 0.0,
                "ftl_bl": 0,
            }
            ltl_by_key[norm_key] = seller
        elif len(raw_ftl_name) > len(seller["name"]):
            seller["name"] = raw_ftl_name
        seller["ftl_trucks"] += trucks
        seller["ftl_bl"]     += bl

    savdo_sellers: list[dict[str, Any]] = []
    savdo_total_ltl = 0.0
    savdo_total_ftl_trucks = 0.0
    savdo_total_ftl_cbm = 0.0
    savdo_total_bl = 0
    for sval in ltl_by_key.values():
        ftl_cbm = sval["ftl_trucks"] * ftl_cbm_per_truck
        total_cbm = sval["ltl_cbm"] + ftl_cbm
        savdo_sellers.append({
            "name":       sval["name"],
            "initials":   "".join(p[0].upper() for p in sval["name"].split()[:2] if p) or "?",
            "ltl_cbm":    _round(sval["ltl_cbm"]),
            "ftl_trucks": _round(sval["ftl_trucks"]),
            "ftl_cbm":    _round(ftl_cbm),
            "total_cbm":  _round(total_cbm),
            "bl_count":   sval["ltl_bl"],
        })
        savdo_total_ltl        += sval["ltl_cbm"]
        savdo_total_ftl_trucks += sval["ftl_trucks"]
        savdo_total_ftl_cbm    += ftl_cbm
        savdo_total_bl         += sval["ltl_bl"]
    savdo_total_cbm = savdo_total_ltl + savdo_total_ftl_cbm
    for row in savdo_sellers:
        share = (float(row["total_cbm"]) / savdo_total_cbm * 100) if savdo_total_cbm else 0
        row["share_percent"] = round(share, 1)
    savdo_sellers.sort(key=lambda x: x["total_cbm"], reverse=True)

    logistika_total_trucks = sum(r["ftl_trucks"] for r in logistika_rows)
    logistika_total_bl     = sum(r["ftl_bl"]     for r in logistika_rows)
    for row in logistika_rows:
        row["initials"] = "".join(p[0].upper() for p in row["name"].split()[:2] if p) or "?"
        row["share_percent"] = (
            round(row["ftl_trucks"] / logistika_total_trucks * 100, 1)
            if logistika_total_trucks else 0
        )
    logistika_rows.sort(key=lambda x: x["ftl_trucks"], reverse=True)

    try:
        year_part, month_part = ym_clean.split("-")
        month_label = f"{ombor_service.MONTH_NAMES.get(month_part, month_part)} {year_part}"
    except ValueError:
        month_label = ym_clean

    return {
        "empty": False,
        "month": ym_clean,
        "label": month_label,
        "savdo": {
            "total_ltl_cbm":    _round(savdo_total_ltl),
            "total_ftl_trucks": _round(savdo_total_ftl_trucks),
            "total_ftl_cbm":    _round(savdo_total_ftl_cbm),
            "total_cbm":        _round(savdo_total_cbm),
            "total_bl":         savdo_total_bl,
            "sellers":          savdo_sellers,
        },
        "logistika": {
            "total_trucks": _round(logistika_total_trucks),
            "total_bl":     logistika_total_bl,
            "logists":      logistika_rows,
        },
        "ftl_cbm_per_truck": ftl_cbm_per_truck,
    }


# ──────────────────────────────────────────────────────────────────────────
# Director paneli — Savdo · Seliy aggregation
# Pulls FTL rows from the director's configured Google Sheet (independent
# of Sales Monitor's sheet) and applies the same Savdo/Logistika split
# logic the monitor uses: hardcoded 3 LOGIST names + optional department
# column override.
# ──────────────────────────────────────────────────────────────────────────
DIRECTOR_LOGIST_NAMES = (
    "SAYFULLAYEV ABDULLOH ORIFJON O'G'LI",
    "O'KTAMOV MAQSUDXO'JA MAXAMAT O'G'LI",
    "ABDULLAYEV IBROHIM ABDUSATTOR O'G'LI",
)


def _norm_person_director(name: str) -> str:
    if not name:
        return ""
    n = name.casefold()
    for ch in ("ʻ", "ʼ", "'", "`", "‘", "’"):
        n = n.replace(ch, "")
    return " ".join(n.split()).strip()


_DIRECTOR_LOGIST_SET = {_norm_person_director(n) for n in DIRECTOR_LOGIST_NAMES}


def _parse_iso_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except (ValueError, AttributeError):
        return None


def get_director_seliy(cfg: dict, date_from_str: str, date_to_str: str) -> dict:
    """Return department-split + agent/client leaderboards for the director's
    Savdo · Seliy view. cfg comes from db.get_director_config('savdo_seliy').
    """
    from services import ombor_service

    sheet_id = (cfg.get("sheet_id") or "").strip()
    if not sheet_id:
        return {"configured": False, "message": "Sheet manbasini sozlang (⚙)."}

    gid_or_name = (cfg.get("sheet_gid") or "").strip() or (cfg.get("sheet_name") or "").strip()
    if not gid_or_name:
        return {"configured": False, "message": "gid yoki varaq nomini ko'rsating."}

    cols = cfg.get("columns") or {}
    header_rows = max(0, int(cfg.get("header_rows") or 1))

    def _maybe_idx(letter: str) -> int | None:
        """Returns column index OR None if the user left this column empty
        in settings. This avoids reading unrelated cells when the source
        sheet doesn't have e.g. an explicit department/client/agent column."""
        s = (letter or "").strip().upper()
        if not s:
            return None
        return ombor_service._col_to_index(s)

    # Required columns: date, sotuvchi (seller name), trucks (type or count)
    date_idx   = _maybe_idx(cols.get("date_col")   or "A")
    logist_idx = _maybe_idx(cols.get("logist_col") or "C")
    trucks_idx = _maybe_idx(cols.get("trucks_col") or "D")
    # Optional columns: if user left them blank, skip reading them
    client_idx = _maybe_idx(cols.get("client_col") or "")
    agent_idx  = _maybe_idx(cols.get("agent_col")  or "")
    # Department detection is name-based (exactly like Sales Monitor):
    # always use the 3 LOGIST_NAMES check, never read a department column.
    # Any stale department_col stored from old config is intentionally
    # ignored here.
    dept_idx = None

    date_from = _parse_iso_date(date_from_str)
    date_to   = _parse_iso_date(date_to_str)

    try:
        rows = ombor_service._fetch_csv_by_gid(sheet_id, gid_or_name)
    except Exception as exc:
        return {"configured": True, "error": f"Sheet o'qish xatosi: {exc}",
                "departments": {"savdo": {}, "logistika": {}},
                "agents": {"rows": []}, "clients": {"rows": []}}

    data_rows = rows[header_rows:]

    daily_savdo: dict[str, float] = {}
    daily_logistika: dict[str, float] = {}
    by_agent: dict[str, dict[str, Any]] = {}
    by_client: dict[str, dict[str, Any]] = {}
    by_savdo_seller: dict[str, dict[str, Any]] = {}
    by_log_seller:   dict[str, dict[str, Any]] = {}
    # Per-seller client breakdown: seller_key -> client_key -> {name, trucks, bl}
    # Used for the drill-down modal that opens when a seller is clicked.
    savdo_seller_clients: dict[str, dict[str, dict[str, Any]]] = {}
    log_seller_clients:   dict[str, dict[str, dict[str, Any]]] = {}
    savdo_total = 0.0
    log_total   = 0.0
    savdo_bl    = 0
    log_bl      = 0

    diag = {
        "rows_total": len(data_rows),
        "rows_used":  0,
        "rows_bad_date": 0,
        "rows_outside_period": 0,
        "rows_no_trucks": 0,
        "sample_used": [],     # first 5 rows that passed filter (raw_date → parsed)
        "sample_outside": [],  # first 5 rows rejected as outside period
        "by_month": {},        # "YYYY-MM" → {trucks: X, rows: Y}
        "sheet_id": sheet_id[:12] + "…",
        "sheet_ref": gid_or_name,
        "filter_from": date_from_str or "",
        "filter_to":   date_to_str or "",
    }

    def safe_cell(row: list[str], idx: int | None) -> str:
        if idx is None or idx < 0:
            return ""
        return row[idx].strip() if idx < len(row) else ""

    for row in data_rows:
        # Trucks counting — identical to Sales Monitor's fetch_ftl_data:
        # use _truck_count on the cell content (20GP/20HQ → 0.5, anything
        # else with a digit or 'FURA' → 1.0, header/junk → 0). This way
        # Director · Seliy matches Sales Monitor numbers on the same sheet.
        trucks_raw = safe_cell(row, trucks_idx)
        trucks = ombor_service._truck_count(trucks_raw)
        if trucks <= 0:
            diag["rows_no_trucks"] += 1
            continue

        raw_date_text = safe_cell(row, date_idx)
        row_date = ombor_service._parse_date(raw_date_text)
        if row_date is None:
            diag["rows_bad_date"] += 1
            continue
        if (date_from and row_date < date_from) or (date_to and row_date > date_to):
            diag["rows_outside_period"] += 1
            if len(diag["sample_outside"]) < 5:
                diag["sample_outside"].append({
                    "raw": raw_date_text[:30],
                    "parsed": row_date.isoformat(),
                    "trucks": trucks,
                })
            continue
        if len(diag["sample_used"]) < 5:
            diag["sample_used"].append({
                "raw": raw_date_text[:30],
                "parsed": row_date.isoformat(),
                "trucks": trucks,
            })
        ym_bucket = row_date.strftime("%Y-%m")
        m_stats = diag["by_month"].setdefault(ym_bucket, {"trucks": 0.0, "rows": 0})
        m_stats["trucks"] += trucks
        m_stats["rows"] += 1

        # Department: prefer explicit label IF the user pointed at a
        # department column; otherwise use the same 3-name LOGIST_NAMES
        # check that Sales Monitor uses (Sayfullayev / O'ktamov / Abdullayev).
        logist_name = safe_cell(row, logist_idx)
        is_logistika = False
        if dept_idx is not None:
            dept_label = safe_cell(row, dept_idx).strip().casefold()
            if dept_label:
                if "logist" in dept_label or "лог" in dept_label:
                    is_logistika = True
                elif "savdo" in dept_label or "торг" in dept_label or "сав" in dept_label:
                    is_logistika = False
                else:
                    is_logistika = _norm_person_director(logist_name) in _DIRECTOR_LOGIST_SET
            else:
                is_logistika = _norm_person_director(logist_name) in _DIRECTOR_LOGIST_SET
        else:
            is_logistika = _norm_person_director(logist_name) in _DIRECTOR_LOGIST_SET

        date_key = row_date.strftime("%Y-%m-%d")
        raw_seller = (logist_name or "").strip()
        # Use normalized key (case-fold + apostrophe-strip) so "Olimov ..."
        # and "OLIMOV ..." merge into one bucket — same dedupe rule the
        # Sales Monitor uses in get_monitor_month_breakdown.
        seller_key = _norm_person_director(raw_seller) if raw_seller else ""
        raw_client_for_drill = safe_cell(row, client_idx) if client_idx is not None else ""
        client_key_for_drill = (raw_client_for_drill or "").strip()

        def _track_seller_client(bucket_map: dict, s_key: str, c_raw: str, c_key: str, t: float) -> None:
            if not s_key or not c_key:
                return
            per_seller = bucket_map.setdefault(s_key, {})
            cb = per_seller.setdefault(c_key, {"name": c_raw, "trucks": 0.0, "bl": 0})
            if len(c_raw) > len(cb["name"]):
                cb["name"] = c_raw
            cb["trucks"] += t
            cb["bl"] += 1

        if is_logistika:
            daily_logistika[date_key] = daily_logistika.get(date_key, 0.0) + trucks
            log_total += trucks
            log_bl += 1
            if seller_key:
                bucket = by_log_seller.setdefault(seller_key, {"name": raw_seller, "trucks": 0.0, "bl": 0})
                # Keep the longest variant as display name (matches Sales Monitor)
                if len(raw_seller) > len(bucket["name"]):
                    bucket["name"] = raw_seller
                bucket["trucks"] += trucks
                bucket["bl"] += 1
                _track_seller_client(log_seller_clients, seller_key, raw_client_for_drill, client_key_for_drill, trucks)
        else:
            daily_savdo[date_key] = daily_savdo.get(date_key, 0.0) + trucks
            savdo_total += trucks
            savdo_bl += 1
            if seller_key:
                bucket = by_savdo_seller.setdefault(seller_key, {"name": raw_seller, "trucks": 0.0, "bl": 0})
                if len(raw_seller) > len(bucket["name"]):
                    bucket["name"] = raw_seller
                bucket["trucks"] += trucks
                bucket["bl"] += 1
                _track_seller_client(savdo_seller_clients, seller_key, raw_client_for_drill, client_key_for_drill, trucks)

        if agent_idx is not None:
            agent_name = safe_cell(row, agent_idx)
            if agent_name:
                bucket = by_agent.setdefault(agent_name, {"name": agent_name, "trucks": 0.0, "bl": 0})
                bucket["trucks"] += trucks
                bucket["bl"] += 1

        if client_idx is not None:
            client_name = safe_cell(row, client_idx)
            if client_name:
                bucket = by_client.setdefault(client_name, {"name": client_name, "trucks": 0.0, "bl": 0})
                bucket["trucks"] += trucks
                bucket["bl"] += 1

        diag["rows_used"] += 1

    all_dates = sorted(set(list(daily_savdo.keys()) + list(daily_logistika.keys())))

    def _r(v: float) -> float:
        return round(float(v or 0), 2)

    savdo_chart = {
        "type": "line",
        "title": "Kunlik furalar",
        "labels": all_dates,
        "datasets": [{
            "label": "Furalar",
            "data": [_r(daily_savdo.get(d, 0)) for d in all_dates],
            "borderColor": "#4aa8ff",
            "backgroundColor": "rgba(74,168,255,.15)",
        }],
    }
    log_chart = {
        "type": "line",
        "title": "Kunlik furalar",
        "labels": all_dates,
        "datasets": [{
            "label": "Furalar",
            "data": [_r(daily_logistika.get(d, 0)) for d in all_dates],
            "borderColor": "#ff5b7f",
            "backgroundColor": "rgba(255,91,127,.15)",
        }],
    }

    agents_rows = sorted(
        [{"name": v["name"], "trucks": _r(v["trucks"]), "bl": v["bl"]} for v in by_agent.values()],
        key=lambda r: r["trucks"], reverse=True,
    )
    clients_rows = sorted(
        [{"name": v["name"], "trucks": _r(v["trucks"]), "bl": v["bl"]} for v in by_client.values()],
        key=lambda r: r["trucks"], reverse=True,
    )

    def _bar_chart(rows: list[dict], color: str) -> dict:
        top = rows[:10]
        return {
            "type": "bar",
            "labels": [(r["name"] or "")[:18] for r in top],
            "datasets": [{
                "label": "Furalar",
                "data": [r["trucks"] for r in top],
                "backgroundColor": color,
                "borderColor": color,
            }],
        }

    def _clients_for_seller(per_seller: dict, seller_key: str) -> list[dict]:
        clients_map = per_seller.get(seller_key) or {}
        return sorted(
            [{"name": c["name"], "trucks": _r(c["trucks"]), "bl": c["bl"]} for c in clients_map.values()],
            key=lambda r: r["trucks"], reverse=True,
        )

    savdo_sellers = sorted(
        [
            {
                "name": v["name"],
                "trucks": _r(v["trucks"]),
                "bl": v["bl"],
                "clients": _clients_for_seller(savdo_seller_clients, sk),
            }
            for sk, v in by_savdo_seller.items()
        ],
        key=lambda r: r["trucks"], reverse=True,
    )
    log_sellers = sorted(
        [
            {
                "name": v["name"],
                "trucks": _r(v["trucks"]),
                "bl": v["bl"],
                "clients": _clients_for_seller(log_seller_clients, sk),
            }
            for sk, v in by_log_seller.items()
        ],
        key=lambda r: r["trucks"], reverse=True,
    )

    return {
        "configured": True,
        "departments": {
            "savdo": {
                "summary": f"{_r(savdo_total)} fura",
                "kpis": [
                    {"label": "Furalar", "value": f"{_r(savdo_total)}"},
                    {"label": "Sotuvchilar", "value": str(len(savdo_sellers))},
                ],
                "chart": savdo_chart,
                "sellers": savdo_sellers,
            },
            "logistika": {
                "summary": f"{_r(log_total)} fura",
                "kpis": [
                    {"label": "Furalar", "value": f"{_r(log_total)}"},
                    {"label": "Logistlar", "value": str(len(log_sellers))},
                ],
                "chart": log_chart,
                "sellers": log_sellers,
            },
        },
        "agents":  {
            "rows": agents_rows,
            "chart": _bar_chart(agents_rows, "#2ad09b"),
            "total_trucks": _r(sum(r["trucks"] for r in agents_rows)),
        },
        "clients": {
            "rows": clients_rows,
            "chart": _bar_chart(clients_rows, "#a78bfa"),
            "total_trucks": _r(sum(r["trucks"] for r in clients_rows)),
        },
        "diagnostics": diag,
        "message": _build_seliy_diagnostic_message(diag, date_from, date_to, sheet_id, gid_or_name),
    }


def _director_sborniy_agents(
    sheet_id: str,
    agent_col: str,
    date_col: str,
    date_from,
    date_to,
    sheet_name: str = "Fura statuslari",
    header_rows: int = 1,
) -> list[dict]:
    """Build agent ranking from a separate 'Fura statuslari' tab.

    Each row in that sheet represents one fura; we group by the agent
    name in column `agent_col` and count rows. If `date_col` is set
    AND parses, we also apply the same date filter as the rest of
    Sborniy so the leaderboard reflects the chosen period.
    """
    from services import ombor_service

    if not sheet_id or not agent_col:
        return []
    try:
        rows = ombor_service._fetch_csv(sheet_id, sheet_name)
    except Exception:
        return []

    agent_idx = ombor_service._col_to_index(agent_col)
    date_idx  = ombor_service._col_to_index(date_col) if date_col else None
    data_rows = rows[max(0, int(header_rows)):]

    by_agent: dict[str, dict] = {}
    for row in data_rows:
        agent = (row[agent_idx].strip() if agent_idx < len(row) else "")
        if not agent:
            continue
        if date_idx is not None:
            raw_date = row[date_idx].strip() if date_idx < len(row) else ""
            row_date = ombor_service._parse_date(raw_date)
            if row_date is None:
                continue
            if (date_from and row_date < date_from) or (date_to and row_date > date_to):
                continue
        # Normalize key (case-fold + apostrophe-strip) so 'Aliyev' /
        # 'ALIYEV' / 'aliyev' merge into one row, like everywhere else.
        key = _norm_person_director(agent)
        bucket = by_agent.setdefault(key, {"name": agent, "trucks": 0, "bl": 0})
        if len(agent) > len(bucket["name"]):
            bucket["name"] = agent
        bucket["trucks"] += 1
        bucket["bl"] += 1

    return sorted(
        [{"name": v["name"], "trucks": v["trucks"], "bl": v["bl"]} for v in by_agent.values()],
        key=lambda r: r["trucks"], reverse=True,
    )


# Weight categories for the Sborniy per-weight seller leaderboard.
# (min_kg inclusive, max_kg exclusive; None = unbounded)
DIRECTOR_WEIGHT_CATEGORIES = (
    {"key": "eng_yengil", "label": "Eng yengil", "min": 0,   "max": 100},
    {"key": "yengil",     "label": "Yengil",     "min": 100, "max": 250},
    {"key": "orta",       "label": "O'rta",      "min": 250, "max": 400},
    {"key": "ogir",       "label": "Og'ir",      "min": 400, "max": 500},
    {"key": "eng_ogir",   "label": "Eng og'ir",  "min": 500, "max": None},
)


def _director_sborniy_weight_categories(
    main_sheet_id: str,
    override_url: str,
    stages: list[dict],
    date_from,
    date_to,
) -> dict:
    """Group sales rows into 5 weight brackets, ranking sellers inside each.

    `stages` is a list of tab configs ({sheet_name, seller_col, weight_col,
    date_col, header_rows}) — sales rows physically move between the 3 tabs
    (Ombor → Ortilgan furalar → Yetib keldi) during their lifecycle, so all
    tabs are read and merged into shared buckets. A row lives in exactly one
    tab at any moment, so summing does not double-count.

    Returns {"configured": bool, "categories": [...], "stages": [{sheet,
    rows} per tab]} — the per-stage row counts feed the diagnostic line.
    """
    from services import ombor_service
    import database as _db

    if override_url:
        sheet_id, _gid = _db._parse_sheet_url(override_url.strip())
        if not sheet_id:
            sheet_id = main_sheet_id
    else:
        sheet_id = main_sheet_id

    usable = [
        st for st in stages
        if (st.get("sheet_name") or "").strip()
        and (st.get("seller_col") or "").strip()
        and (st.get("weight_col") or "").strip()
    ]
    if not sheet_id or not usable:
        return {"configured": False, "categories": [], "stages": []}

    # cat_key -> seller_key -> {name, count, weight, bl_set}
    buckets: dict[str, dict[str, dict]] = {c["key"]: {} for c in DIRECTOR_WEIGHT_CATEGORIES}
    # Overall per-seller totals with a per-category breakdown — feeds the
    # summary leaderboard under the 5 category columns.
    overall: dict[str, dict] = {}
    stage_stats: list[dict] = []

    for st in usable:
        sheet_name = st["sheet_name"].strip()
        try:
            rows = ombor_service._fetch_csv(sheet_id, sheet_name)
        except Exception:
            stage_stats.append({"sheet": sheet_name, "rows": 0, "error": "fetch failed"})
            continue
        try:
            header_n = max(0, int(st.get("header_rows") or 1))
        except (TypeError, ValueError):
            header_n = 1
        data_rows = rows[header_n:]

        seller_idx = ombor_service._col_to_index(st["seller_col"].strip())
        weight_idx = ombor_service._col_to_index(st["weight_col"].strip())
        date_letter = (st.get("date_col") or "").strip()
        date_idx = ombor_service._col_to_index(date_letter) if date_letter else None
        bl_letter = (st.get("bl_col") or "").strip()
        bl_idx = ombor_service._col_to_index(bl_letter) if bl_letter else None

        stat = {
            "sheet": sheet_name,
            "rows_total": len(data_rows),
            "rows": 0,             # used
            "no_seller": 0,
            "no_weight": 0,
            "bad_date": 0,
            "outside": 0,
            "no_date": 0,          # empty date cell — row still counted
        }
        for row in data_rows:
            seller = (row[seller_idx].strip() if seller_idx < len(row) else "")
            if not seller:
                stat["no_seller"] += 1
                continue
            weight_cell = row[weight_idx].strip() if weight_idx < len(row) else ""
            weight = ombor_service._parse_float(weight_cell)
            if weight <= 0:
                stat["no_weight"] += 1
                continue
            if date_idx is not None:
                raw_date = row[date_idx].strip() if date_idx < len(row) else ""
                if raw_date:
                    row_date = ombor_service._parse_date(raw_date)
                    if row_date is None:
                        # Non-empty but unparseable date — treat as data error,
                        # skip so garbage rows don't pollute the ranking.
                        stat["bad_date"] += 1
                        continue
                    if (date_from and row_date < date_from) or (date_to and row_date > date_to):
                        stat["outside"] += 1
                        continue
                else:
                    # Empty date cell = cargo not yet scheduled (typical for
                    # rows still sitting in Ombor). Count it as current so
                    # in-progress sales don't vanish from the rating.
                    stat["no_date"] += 1

            cat = None
            for c in DIRECTOR_WEIGHT_CATEGORIES:
                lo, hi = c["min"], c["max"]
                if weight >= lo and (hi is None or weight < hi):
                    cat = c
                    break
            if cat is None:
                continue

            key = _norm_person_director(seller)
            bucket = buckets[cat["key"]].setdefault(
                key, {"name": seller, "count": 0, "weight": 0.0, "bl_set": set()}
            )
            if len(seller) > len(bucket["name"]):
                bucket["name"] = seller
            bucket["count"] += 1
            bucket["weight"] += weight
            bl_code = ""
            if bl_idx is not None:
                bl_code = row[bl_idx].strip() if bl_idx < len(row) else ""
                if bl_code:
                    bucket["bl_set"].add(bl_code.upper())

            ov = overall.setdefault(
                key,
                {"name": seller, "count": 0, "weight": 0.0, "bl_set": set(),
                 "per_cat": {c["key"]: {"count": 0, "weight": 0.0} for c in DIRECTOR_WEIGHT_CATEGORIES}},
            )
            if len(seller) > len(ov["name"]):
                ov["name"] = seller
            ov["count"] += 1
            ov["weight"] += weight
            if bl_code:
                ov["bl_set"].add(bl_code.upper())
            ov["per_cat"][cat["key"]]["count"] += 1
            ov["per_cat"][cat["key"]]["weight"] += weight
            stat["rows"] += 1

        stage_stats.append(stat)

    categories = []
    for c in DIRECTOR_WEIGHT_CATEGORIES:
        sellers = sorted(
            [
                {
                    "name":   v["name"],
                    "count":  v["count"],
                    "weight": round(v["weight"], 1),
                    "bl":     len(v["bl_set"]),
                    "bl_codes": sorted(v["bl_set"])[:20],
                }
                for v in buckets[c["key"]].values()
            ],
            key=lambda r: (r["count"], r["weight"]), reverse=True,
        )
        lo, hi = c["min"], c["max"]
        range_label = f"{lo}–{hi} kg" if hi is not None else f"{lo}+ kg"
        categories.append({
            "key":          c["key"],
            "label":        c["label"],
            "range_label":  range_label,
            "total_count":  sum(s["count"] for s in sellers),
            "total_weight": round(sum(s["weight"] for s in sellers), 1),
            "total_bl":     sum(s["bl"] for s in sellers),
            "sellers":      sellers,
        })

    # Overall seller leaderboard with per-category breakdown
    sellers_overall = sorted(
        [
            {
                "name":   v["name"],
                "count":  v["count"],
                "weight": round(v["weight"], 1),
                "bl":     len(v["bl_set"]),
                "categories": {
                    ck: {"count": cv["count"], "weight": round(cv["weight"], 1)}
                    for ck, cv in v["per_cat"].items() if cv["count"] > 0
                },
            }
            for v in overall.values()
        ],
        key=lambda r: (r["count"], r["weight"]), reverse=True,
    )

    return {"configured": True, "categories": categories, "sellers": sellers_overall, "stages": stage_stats}


# Per-stage daily aggregator for the Sborniy 'Kunlik dinamika' chart.
# Reuses ombor_service's cached _fetch_csv and parsing helpers so the
# 3 sheets aren't re-downloaded after fetch_combined_ltl_data already
# warmed the cache.
def _director_sborniy_daily(
    sheet_id: str,
    primary_sheet_name: str,
    primary_cbm_col: str,
    primary_date_col: str,
    primary_header_rows: int,
    date_from,
    date_to,
) -> dict:
    from services import ombor_service

    stages = [
        {
            "sheet_name":  primary_sheet_name,
            "cbm_col":     primary_cbm_col,
            "date_col":    primary_date_col,
            "header_rows": primary_header_rows,
        },
        # Stages 2 & 3 use the same fixed layout LTL_PIPELINE_STAGES does.
        {"sheet_name": "Ortilgan furalar", "cbm_col": "U", "date_col": "Y", "header_rows": 1},
        {"sheet_name": "Yetib keldi",      "cbm_col": "T", "date_col": "X", "header_rows": 1},
    ]

    daily: dict[str, float] = {}
    for st in stages:
        try:
            rows = ombor_service._fetch_csv(sheet_id, st["sheet_name"])
        except Exception:
            continue
        cbm_idx  = ombor_service._col_to_index(st["cbm_col"])
        date_idx = ombor_service._col_to_index(st["date_col"])
        data_rows = rows[max(0, int(st["header_rows"])):]
        for row in data_rows:
            cbm_cell = row[cbm_idx].strip() if cbm_idx < len(row) else ""
            cbm = ombor_service._parse_float(cbm_cell)
            if cbm <= 0:
                continue
            raw_date = row[date_idx].strip() if date_idx < len(row) else ""
            row_date = ombor_service._parse_date(raw_date)
            if row_date is None:
                continue
            if (date_from and row_date < date_from) or (date_to and row_date > date_to):
                continue
            day_key = row_date.strftime("%Y-%m-%d")
            daily[day_key] = daily.get(day_key, 0.0) + cbm
    # Return as ordered dict sorted by date asc
    return dict(sorted(daily.items()))


# ──────────────────────────────────────────────────────────────────────────
# Director paneli — Savdo · Sborniy (consolidated / LTL)
# Mirrors Sales Monitor's Savdo bo'limi LTL leaderboard. Reads the Ombor
# sheet via the same fetch_ombor_data() helper Sales Monitor uses, so
# CBM totals and seller buckets are identical. FTL trucks are NOT
# included here — Seliy is the separate page for whole-truck data.
# ──────────────────────────────────────────────────────────────────────────
def get_director_sborniy(cfg: dict, date_from_str: str, date_to_str: str) -> dict:
    from services import ombor_service

    sheet_id = (cfg.get("sheet_id") or "").strip()
    if not sheet_id:
        return {"configured": False, "message": "Sheet manbasini sozlang (⚙)."}

    sheet_name = (cfg.get("sheet_name") or "").strip() or "Ombor"
    cols = cfg.get("columns") or {}
    header_rows = max(0, int(cfg.get("header_rows") or 2))

    cbm_col    = (cols.get("cbm_col")    or "V").strip().upper() or "V"
    date_col   = (cols.get("date_col")   or "Z").strip().upper() or "Z"
    seller_col = (cols.get("seller_col") or "AG").strip().upper() or "AG"
    bl_col     = (cols.get("bl_col")     or "E").strip().upper() or "E"
    # Logist col on the primary (Ombor) stage — Sales Monitor uses AH by
    # default. Not exposed in the director UI (we don't show a separate
    # logist leaderboard for Sborniy), but the combined fetcher needs it.
    logist_col = (cols.get("logist_col") or "AH").strip().upper() or "AH"
    # Fura statuslari tab — for agent ranking
    fura_agent_col = (cols.get("fura_agent_col") or "B").strip().upper() or "B"
    fura_date_col  = (cols.get("fura_date_col")  or "").strip().upper()

    date_from = _parse_iso_date(date_from_str)
    date_to   = _parse_iso_date(date_to_str)

    # Use the same combined fetcher Sales Monitor uses: it walks the
    # 3-stage LTL pipeline (Ombor + Ortilgan furalar + Yetib keldi) and
    # merges sellers/monthly across stages with apostrophe-normalized
    # name keys. The user only configures the primary (Ombor) columns —
    # stages 2 and 3 have fixed column layouts baked into ombor_service.
    try:
        ombor = ombor_service.fetch_combined_ltl_data(
            sheet_id=sheet_id,
            primary_sheet_name=sheet_name,
            primary_cbm_col=cbm_col,
            primary_date_col=date_col,
            primary_seller_col=seller_col,
            primary_logist_col=logist_col,
            primary_bl_col=bl_col,
            primary_header_rows=header_rows,
            date_from=date_from,
            date_to=date_to,
            force=False,
        )
    except Exception as exc:
        return {
            "configured": True,
            "error": f"Sheet o'qish xatosi: {exc}",
            "kpis": [], "charts": {},
            "message": f"Xato: {exc}",
        }

    sellers_raw = ombor.get("sellers") or []
    diag = ombor.get("diagnostics") or {}

    def _r(v: float) -> float:
        return round(float(v or 0), 2)

    # fetch_ombor_data returns sellers as a list (already sorted by CBM desc)
    sellers_sorted = [
        {
            "name":  v.get("name") or "—",
            "cbm":   _r(v.get("cbm") or 0),
            "bl":    int(v.get("bl_count") or 0),
            "share": float(v.get("share_percent") or 0),
        }
        for v in (sellers_raw if isinstance(sellers_raw, list) else sellers_raw.values())
    ]

    total_cbm = _r(ombor.get("total_cbm") or sum(s["cbm"] for s in sellers_sorted))
    total_bl  = int(ombor.get("total_bl")  or sum(s["bl"]  for s in sellers_sorted))

    # Top-N bar chart
    top_n = sellers_sorted[:10]
    bar_chart = {
        "type": "bar",
        "title": "Top sotuvchilar (m³)",
        "labels": [(s["name"] or "")[:18] for s in top_n],
        "datasets": [{
            "label": "m³",
            "data": [s["cbm"] for s in top_n],
            "backgroundColor": "#4aa8ff",
            "borderColor": "#4aa8ff",
        }],
    }

    # Daily CBM breakdown across all 3 stages. fetch_combined_ltl_data
    # only returns month-level aggregates, so we walk the raw CSVs here
    # (cached by ombor_service) and bucket by day.
    daily_totals = _director_sborniy_daily(
        sheet_id=sheet_id,
        primary_sheet_name=sheet_name,
        primary_cbm_col=cbm_col,
        primary_date_col=date_col,
        primary_header_rows=header_rows,
        date_from=date_from,
        date_to=date_to,
    )
    daily_axis  = list(daily_totals.keys())  # already sorted
    daily_values = [_r(daily_totals[d]) for d in daily_axis]
    line_chart = {
        "type": "line",
        "title": "Kunlik dinamika (m³)",
        "labels": daily_axis,
        "datasets": [{
            "label": "m³",
            "data": daily_values,
            "borderColor": "#2ad09b",
            "backgroundColor": "rgba(42,208,155,.15)",
        }],
    }

    # Combined-fetcher diagnostics: per-stage rows + cache state
    per_stage = (diag.get("per_stage") or {}) if isinstance(diag, dict) else {}
    stage_summaries: list[str] = []
    total_rows_used  = 0
    total_rows_total = 0
    for stage_name, sd in per_stage.items():
        if not isinstance(sd, dict):
            continue
        if sd.get("error"):
            stage_summaries.append(f"{stage_name}: XATO ({sd['error']})")
            continue
        used  = int(sd.get("rows_used")  or 0)
        total = int(sd.get("rows_total") or 0)
        total_rows_used  += used
        total_rows_total += total
        stage_summaries.append(f"{stage_name}: {used}/{total}")
    stages_summary = " | ".join(stage_summaries) or "—"

    # Agent ranking from the 'Fura statuslari' tab in the same spreadsheet
    agents_rows = _director_sborniy_agents(
        sheet_id=sheet_id,
        agent_col=fura_agent_col,
        date_col=fura_date_col,
        date_from=date_from,
        date_to=date_to,
    )

    # Weight-category seller leaderboard (Eng yengil … Eng og'ir).
    # Reads all 3 lifecycle tabs — sales rows migrate Ombor → Ortilgan
    # furalar → Yetib keldi, so a single-tab read misses everything that
    # has already moved on.
    def _vazn_stage(prefix: str) -> dict:
        return {
            "sheet_name":  (cols.get(f"{prefix}_sheet_name") or "").strip(),
            "seller_col":  (cols.get(f"{prefix}_seller_col") or "").strip().upper(),
            "weight_col":  (cols.get(f"{prefix}_weight_col") or "").strip().upper(),
            "bl_col":      (cols.get(f"{prefix}_bl_col") or "").strip().upper(),
            "date_col":    (cols.get(f"{prefix}_date_col") or "").strip().upper(),
            "header_rows": cols.get(f"{prefix}_header_rows"),
        }

    vazn_stages = [_vazn_stage("vazn1"), _vazn_stage("vazn2"), _vazn_stage("vazn3")]
    # Backward compat: configs saved before the 3-tab split used vazn_* keys
    if not any(s["sheet_name"] and s["seller_col"] and s["weight_col"] for s in vazn_stages):
        legacy = _vazn_stage("vazn")
        if legacy["sheet_name"]:
            vazn_stages = [legacy]

    weight_data = _director_sborniy_weight_categories(
        main_sheet_id=sheet_id,
        override_url=(cols.get("vazn_sheet_url") or "").strip(),
        stages=vazn_stages,
        date_from=date_from,
        date_to=date_to,
    )

    return {
        "configured": True,
        "kpis": [
            {"label": "Jami m³",     "value": f"{_r(total_cbm)}"},
            {"label": "Jami BL",     "value": str(total_bl)},
            {"label": "Sotuvchilar", "value": str(len(sellers_sorted))},
            {"label": "Agentlar",    "value": str(len(agents_rows))},
        ],
        "charts": {
            "chart1": line_chart,
            "chart2": bar_chart,
        },
        "sellers": sellers_sorted,
        "agents":  agents_rows,
        "weight_categories": weight_data,
        "diagnostics": diag,
        "message": (
            f"3 ta bosqich birlashtirildi (Ombor + Ortilgan furalar + Yetib keldi). "
            f"Jami: {total_rows_used}/{total_rows_total} qator "
            f"({date_from or '∞'} → {date_to or '∞'}). "
            f"Bosqichlar bo'yicha: {stages_summary}. "
            f"Agentlar (Fura statuslari): {len(agents_rows)} ta. "
            f"Vazn reytingi: "
            + (
                " | ".join(
                    (
                        f"{s['sheet']}: XATO (o'qib bo'lmadi)"
                        if s.get("error")
                        else (
                            f"{s['sheet']}: {s.get('rows', 0)}/{s.get('rows_total', 0)} qator"
                            + (f", sotuvchisiz={s['no_seller']}" if s.get("no_seller") else "")
                            + (f", vaznsiz={s['no_weight']}" if s.get("no_weight") else "")
                            + (f", sanasi buzuq={s['bad_date']}" if s.get("bad_date") else "")
                            + (f", davrdan tashqari={s['outside']}" if s.get("outside") else "")
                            + (f", sanasiz={s['no_date']}" if s.get("no_date") else "")
                        )
                    )
                    for s in (weight_data.get("stages") or [])
                )
                if weight_data.get("configured") and weight_data.get("stages")
                else "sozlanmagan (har bir varaq uchun vazn ustunini kiriting)"
            )
        ),
    }


# ──────────────────────────────────────────────────────────────────────────
# Director paneli — Ombor va yuklar statusi
# Warehouse fill gauges: aggregates CBM by warehouse name and shows
# fill % against per-warehouse capacity. The 3 hardcoded warehouses
# are YIWU / ZHONGSHAN / HORGOS — matched case-insensitively against
# the configured warehouse column (substring match handles cells like
# 'YIWU склад', 'horgos warehouse', etc.).
# ──────────────────────────────────────────────────────────────────────────
DIRECTOR_WAREHOUSES = ("YIWU", "ZHONGSHAN", "HORGOS")


def _safe_float(value, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(str(value).replace(",", ".").replace(" ", ""))
    except (ValueError, TypeError):
        return default


def _director_ombor_metric(
    main_sheet_id: str,
    override_url: str,
    sheet_name: str,
    date_col: str,
    cbm_col: str,
    header_rows,
    date_from,
    date_to,
) -> dict:
    """Compute one sub-metric (sum of m³ on a tab, optionally date-filtered).

    Returns {"cbm": float, "rows": int, "configured": bool}.
    If override_url is non-empty, parses sheet_id from it; otherwise uses
    main_sheet_id. Returns configured=False if essentials are missing.
    """
    from services import ombor_service
    import database as _db

    if override_url:
        sheet_id, _gid = _db._parse_sheet_url(override_url.strip())
        if not sheet_id:
            sheet_id = main_sheet_id
    else:
        sheet_id = main_sheet_id

    if not sheet_id or not sheet_name or not cbm_col:
        return {"cbm": 0.0, "rows": 0, "configured": False}

    try:
        rows = ombor_service._fetch_csv(sheet_id, sheet_name)
    except Exception:
        return {"cbm": 0.0, "rows": 0, "configured": True, "error": "fetch failed"}

    try:
        header_n = max(0, int(header_rows or 1))
    except (TypeError, ValueError):
        header_n = 1
    data_rows = rows[header_n:]

    cbm_idx  = ombor_service._col_to_index(cbm_col)
    date_idx = ombor_service._col_to_index(date_col) if date_col else None

    total = 0.0
    used  = 0
    for row in data_rows:
        cbm_cell = row[cbm_idx].strip() if cbm_idx < len(row) else ""
        cbm = ombor_service._parse_float(cbm_cell)
        if cbm <= 0:
            continue
        if date_idx is not None:
            raw_date = row[date_idx].strip() if date_idx < len(row) else ""
            row_date = ombor_service._parse_date(raw_date)
            if row_date is None:
                continue
            if (date_from and row_date < date_from) or (date_to and row_date > date_to):
                continue
        total += cbm
        used += 1

    return {"cbm": round(total, 2), "rows": used, "configured": True}


def _director_ombor_transit_metric(
    main_sheet_id: str,
    override_url: str,
    sheet_name: str,
    departure_col: str,
    arrival_col: str,
    cbm_col: str,
    header_rows,
    date_from,
    date_to,
) -> dict:
    """Count cargos currently 'in transit' on a tab.

    A row is in transit if:
      - departure_date is set AND <= today (cargo has departed), AND
      - arrival_date is empty OR > today (cargo hasn't arrived yet)

    Period filter (date_from / date_to) applies to departure_date so
    the KPI reflects 'in-transit cargos that departed within the period'.

    Returns {"count": int, "cbm": float, "configured": bool}.
    """
    from services import ombor_service
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import database as _db

    if override_url:
        sheet_id, _gid = _db._parse_sheet_url(override_url.strip())
        if not sheet_id:
            sheet_id = main_sheet_id
    else:
        sheet_id = main_sheet_id

    if not sheet_id or not sheet_name or not departure_col:
        return {"count": 0, "cbm": 0.0, "configured": False}

    try:
        rows = ombor_service._fetch_csv(sheet_id, sheet_name)
    except Exception:
        return {"count": 0, "cbm": 0.0, "configured": True, "error": "fetch failed"}

    try:
        header_n = max(0, int(header_rows or 1))
    except (TypeError, ValueError):
        header_n = 1
    data_rows = rows[header_n:]

    dep_idx = ombor_service._col_to_index(departure_col)
    arr_idx = ombor_service._col_to_index(arrival_col) if arrival_col else None
    cbm_idx = ombor_service._col_to_index(cbm_col)     if cbm_col else None

    today = datetime.now(ZoneInfo("Asia/Tashkent")).date()

    count = 0
    total_cbm = 0.0
    for row in data_rows:
        dep_cell = row[dep_idx].strip() if dep_idx < len(row) else ""
        dep_date = ombor_service._parse_date(dep_cell)
        if dep_date is None or dep_date > today:
            continue  # not yet departed
        # Arrival check: if set AND already passed, the cargo has arrived
        if arr_idx is not None:
            arr_cell = row[arr_idx].strip() if arr_idx < len(row) else ""
            arr_date = ombor_service._parse_date(arr_cell)
            if arr_date is not None and arr_date <= today:
                continue
        # Apply period filter on departure date
        if date_from and dep_date < date_from:
            continue
        if date_to and dep_date > date_to:
            continue
        count += 1
        if cbm_idx is not None:
            cbm_cell = row[cbm_idx].strip() if cbm_idx < len(row) else ""
            cbm = ombor_service._parse_float(cbm_cell)
            if cbm > 0:
                total_cbm += cbm

    return {"count": count, "cbm": round(total_cbm, 2), "configured": True}


def get_director_ombor(cfg: dict, date_from_str: str, date_to_str: str) -> dict:
    from services import ombor_service

    sheet_id = (cfg.get("sheet_id") or "").strip()
    if not sheet_id:
        return {"configured": False, "message": "Sheet manbasini sozlang (⚙)."}

    sheet_name = (cfg.get("sheet_name") or "Ombor").strip() or "Ombor"
    cols = cfg.get("columns") or {}
    header_rows = max(0, int(cfg.get("header_rows") or 2))

    date_idx = ombor_service._col_to_index((cols.get("date_col") or "Z"))
    cbm_idx  = ombor_service._col_to_index((cols.get("cbm_col")  or "V"))
    bl_letter = (cols.get("bl_col") or "").strip().upper()
    bl_idx   = ombor_service._col_to_index(bl_letter) if bl_letter else None
    wh_letter = (cols.get("warehouse_col") or "").strip().upper()
    wh_idx   = ombor_service._col_to_index(wh_letter) if wh_letter else None

    capacities = {
        "YIWU":      _safe_float(cols.get("capacity_yiwu")),
        "ZHONGSHAN": _safe_float(cols.get("capacity_zhongshan")),
        "HORGOS":    _safe_float(cols.get("capacity_horgos")),
    }

    date_from = _parse_iso_date(date_from_str)
    date_to   = _parse_iso_date(date_to_str)

    try:
        rows = ombor_service._fetch_csv(sheet_id, sheet_name)
    except Exception as exc:
        return {
            "configured": True,
            "error": f"Sheet o'qish xatosi: {exc}",
            "kpis": [], "charts": {}, "warehouses": [],
            "message": f"Xato: {exc}",
        }

    data_rows = rows[max(0, header_rows):]

    by_wh: dict[str, dict] = {n: {"cbm": 0.0, "bl_set": set(), "rows": 0} for n in DIRECTOR_WAREHOUSES}
    daily: dict[str, float] = {}
    diag = {
        "rows_total": len(data_rows),
        "rows_used": 0,
        "rows_no_cbm": 0,
        "rows_bad_date": 0,
        "rows_outside_period": 0,
        "rows_no_warehouse": 0,
    }

    def safe_cell(row: list[str], idx) -> str:
        if idx is None or idx < 0:
            return ""
        return row[idx].strip() if idx < len(row) else ""

    for row in data_rows:
        cbm = ombor_service._parse_float(safe_cell(row, cbm_idx))
        if cbm <= 0:
            diag["rows_no_cbm"] += 1
            continue
        row_date = ombor_service._parse_date(safe_cell(row, date_idx))
        if row_date is None:
            diag["rows_bad_date"] += 1
            continue
        if (date_from and row_date < date_from) or (date_to and row_date > date_to):
            diag["rows_outside_period"] += 1
            continue

        wh_raw = safe_cell(row, wh_idx).upper()
        matched: str | None = None
        if wh_raw:
            for wh in DIRECTOR_WAREHOUSES:
                if wh in wh_raw:
                    matched = wh
                    break
        if matched:
            bucket = by_wh[matched]
            bucket["cbm"] += cbm
            bucket["rows"] += 1
            bl_id = safe_cell(row, bl_idx) if bl_idx is not None else f"__row_{diag['rows_used']}__"
            if bl_id:
                bucket["bl_set"].add(bl_id)
        else:
            diag["rows_no_warehouse"] += 1

        day_key = row_date.strftime("%Y-%m-%d")
        daily[day_key] = daily.get(day_key, 0.0) + cbm
        diag["rows_used"] += 1

    def _r(v: float) -> float:
        return round(float(v or 0), 2)

    warehouses = []
    for wh in DIRECTOR_WAREHOUSES:
        b = by_wh[wh]
        cap = capacities[wh]
        cbm = _r(b["cbm"])
        fill = round((cbm / cap * 100) if cap > 0 else 0, 1)
        warehouses.append({
            "name": wh,
            "cbm": cbm,
            "bl":  len(b["bl_set"]),
            "rows": b["rows"],
            "capacity": _r(cap),
            "fill_percent": fill,
        })

    daily_sorted = dict(sorted(daily.items()))
    daily_chart = {
        "type": "line",
        "title": "Kunlik harakat (m³)",
        "labels": list(daily_sorted.keys()),
        "datasets": [{
            "label": "m³",
            "data": [_r(v) for v in daily_sorted.values()],
            "borderColor": "#4aa8ff",
            "backgroundColor": "rgba(74,168,255,.15)",
        }],
    }

    active_warehouses = [w for w in warehouses if w["cbm"] > 0]
    dist_chart = {
        "type": "doughnut",
        "title": "Ombor bo'yicha taqsimot",
        "labels": [w["name"] for w in active_warehouses],
        "datasets": [{
            "label": "m³",
            "data":  [w["cbm"] for w in active_warehouses],
            "backgroundColor": ["#4aa8ff", "#2ad09b", "#ffb739"],
        }],
    }

    total_cbm = sum(w["cbm"] for w in warehouses)
    total_bl  = sum(w["bl"]  for w in warehouses)

    # 4 configurable sub-metrics — each points at its own sheet/tab.
    # ortilgan / hajm → sum m³ by single date column
    # yulda / bojxona → count rows currently in transit (departure passed,
    #                   arrival empty/future), with m³ as subtitle.
    main_sheet_id = sheet_id
    extra_kpis = []
    for prefix, label in [("ortilgan", "Umumiy ortilgan yuklar"),
                          ("hajm",     "Umumiy hajm")]:
        result = _director_ombor_metric(
            main_sheet_id=main_sheet_id,
            override_url=(cols.get(f"{prefix}_sheet_url") or "").strip(),
            sheet_name=(cols.get(f"{prefix}_sheet_name") or "").strip(),
            date_col=(cols.get(f"{prefix}_date_col") or "").strip().upper(),
            cbm_col=(cols.get(f"{prefix}_cbm_col") or "").strip().upper(),
            header_rows=cols.get(f"{prefix}_header_rows"),
            date_from=date_from,
            date_to=date_to,
        )
        extra_kpis.append({
            "key":   prefix,
            "label": label,
            "mode":  "cbm",
            "cbm":   result["cbm"],
            "count": result["rows"],
            "configured": result["configured"],
        })
    for prefix, label in [("yulda",   "Yo'ldagi yuklar"),
                          ("bojxona", "Bojxonadagi yuklar")]:
        result = _director_ombor_transit_metric(
            main_sheet_id=main_sheet_id,
            override_url=(cols.get(f"{prefix}_sheet_url") or "").strip(),
            sheet_name=(cols.get(f"{prefix}_sheet_name") or "").strip(),
            departure_col=(cols.get(f"{prefix}_departure_col") or "").strip().upper(),
            arrival_col=(cols.get(f"{prefix}_arrival_col") or "").strip().upper(),
            cbm_col=(cols.get(f"{prefix}_cbm_col") or "").strip().upper(),
            header_rows=cols.get(f"{prefix}_header_rows"),
            date_from=date_from,
            date_to=date_to,
        )
        extra_kpis.append({
            "key":   prefix,
            "label": label,
            "mode":  "transit",
            "cbm":   result["cbm"],
            "count": result["count"],
            "configured": result["configured"],
        })

    return {
        "configured": True,
        "kpis": [
            {"label": "Jami m³", "value": f"{_r(total_cbm)}"},
            {"label": "Jami BL", "value": str(total_bl)},
            {"label": "Faol omborlar", "value": str(len(active_warehouses))},
        ],
        "warehouses": warehouses,
        "extra_kpis": extra_kpis,
        "charts": {"chart1": daily_chart, "chart2": dist_chart},
        "diagnostics": diag,
        "message": (
            f"Yangilangan: {diag['rows_used']} / {diag['rows_total']} qator "
            f"({date_from or '∞'} → {date_to or '∞'}). "
            f"Ombor topilmadi: {diag['rows_no_warehouse']}, "
            f"CBM={diag['rows_no_cbm']}, sana={diag['rows_bad_date']}, "
            f"davrdan tashqari={diag['rows_outside_period']}"
        ),
    }


def _build_seliy_diagnostic_message(diag: dict, date_from, date_to, sheet_id: str, gid_or_name: str) -> str:
    # Round trucks in by_month for display
    by_month_sorted = sorted(diag.get("by_month", {}).items())
    by_month_str = ", ".join(
        f"{ym}: {round(m['trucks'], 1)} fura ({m['rows']} qator)"
        for ym, m in by_month_sorted
    ) or "—"
    return (
        f"Manba: {sheet_id[:14]}… · gid/varaq {gid_or_name} | "
        f"Davr: {date_from or '∞'} → {date_to or '∞'} | "
        f"Olingan: {diag['rows_used']} / {diag['rows_total']} qator. "
        f"Tashlangan: trucks={diag['rows_no_trucks']}, sana={diag['rows_bad_date']}, "
        f"davrdan tashqari={diag['rows_outside_period']}. "
        f"Oylar: {by_month_str}"
    )
