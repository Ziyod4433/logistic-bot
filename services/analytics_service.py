from __future__ import annotations

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
                SET name = ?, period_start = ?, period_end = ?, target_amount_usd = ?, target_metric = ?, target_value = ?, is_active = ?, updated_at = datetime('now','localtime')
                WHERE id = ?
                """,
                (
                    name,
                    period_start,
                    period_end,
                    target_value if target_metric == "amount_usd" else 0,
                    target_metric,
                    target_value,
                    is_active,
                    plan_id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO analytics_sales_plans(name, period_start, period_end, target_amount_usd, target_metric, target_value, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))
                """,
                (
                    name,
                    period_start,
                    period_end,
                    target_value if target_metric == "amount_usd" else 0,
                    target_metric,
                    target_value,
                    is_active,
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
