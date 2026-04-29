from __future__ import annotations

import json
import os
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import database as db

BASE_CURRENCY = "UZS"
DELAY_THRESHOLD_DAYS = 25


def _now() -> datetime:
    return datetime.now()


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_date(value: Any) -> date | None:
    text = _clean_text(value)
    if not text:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _date_to_str(value: date | None) -> str:
    return value.strftime("%d.%m.%Y") if value else ""


def _to_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _month_key(value: Any) -> str:
    parsed = _parse_date(value)
    return parsed.strftime("%Y-%m") if parsed else ""


def _format_number(value: float) -> str:
    value = float(value or 0)
    if abs(value - round(value)) < 0.00001:
        return f"{int(round(value)):,}".replace(",", " ")
    return f"{value:,.2f}".replace(",", " ").rstrip("0").rstrip(".")


def _format_money(value: float, currency: str) -> str:
    return f"{_format_number(value)} {currency}".strip()


def _combine_currency_totals(rows: list[dict[str, Any]], field: str) -> dict[str, float]:
    totals: dict[str, float] = defaultdict(float)
    for row in rows:
        currency = _clean_text(row.get("currency")) or BASE_CURRENCY
        totals[currency] += _to_float(row.get(field))
    return dict(totals)


def _format_currency_totals(totals: dict[str, float]) -> str:
    if not totals:
        return "0"
    parts = [_format_money(amount, currency) for currency, amount in sorted(totals.items()) if abs(amount) > 0.00001]
    return " · ".join(parts) if parts else "0"


def _load_rates() -> dict[str, float]:
    raw = db.get_setting("analytics_exchange_rates", '{"UZS": 1}')
    try:
        parsed = json.loads(raw or "{}")
    except json.JSONDecodeError:
        parsed = {}
    rates = {str(code).upper(): _to_float(value) for code, value in (parsed or {}).items() if _to_float(value) > 0}
    if "UZS" not in rates:
        rates["UZS"] = 1.0
    return rates


def _convert_to_base(amount: float, currency: str, rates: dict[str, float]) -> float | None:
    currency = (_clean_text(currency) or BASE_CURRENCY).upper()
    if currency == BASE_CURRENCY:
        return amount
    rate = rates.get(currency)
    if not rate:
        return None
    return amount * rate


def _sum_converted(rows: list[dict[str, Any]], field: str, rates: dict[str, float]) -> tuple[float, list[str]]:
    total = 0.0
    missing: list[str] = []
    for row in rows:
        converted = _convert_to_base(_to_float(row.get(field)), row.get("currency"), rates)
        if converted is None:
            currency = (_clean_text(row.get("currency")) or "?").upper()
            if currency not in missing:
                missing.append(currency)
            continue
        total += converted
    return total, missing


def _percent_change(current: float, previous: float) -> float | None:
    if abs(previous) < 0.00001:
        return None
    return ((current - previous) / previous) * 100.0


def _daterange_from_preset(preset: str, date_from_raw: str, date_to_raw: str) -> tuple[date | None, date | None]:
    today = _now().date()
    preset = _clean_text(preset or "month").lower()
    if preset == "today":
        return today, today
    if preset == "week":
        start = today - timedelta(days=today.weekday())
        return start, today
    if preset == "month":
        start = today.replace(day=1)
        return start, today
    if preset == "year":
        start = today.replace(month=1, day=1)
        return start, today
    if preset == "custom":
        return _parse_date(date_from_raw), _parse_date(date_to_raw)
    start = today.replace(day=1)
    return start, today


def _safe_lower(value: Any) -> str:
    return _clean_text(value).lower()


@dataclass
class AnalyticsFilters:
    preset: str
    date_from: date | None
    date_to: date | None
    manager: str = ""
    client: str = ""
    bl_code: str = ""
    reys_number: str = ""
    fura_number: str = ""
    status: str = ""
    currency: str = ""
    bank_or_cash: str = ""
    category: str = ""
    warehouse: str = ""


def parse_filters(args: Any) -> AnalyticsFilters:
    preset = _clean_text(args.get("period") or "month")
    date_from, date_to = _daterange_from_preset(preset, args.get("date_from"), args.get("date_to"))
    return AnalyticsFilters(
        preset=preset,
        date_from=date_from,
        date_to=date_to,
        manager=_clean_text(args.get("manager")),
        client=_clean_text(args.get("client")),
        bl_code=_clean_text(args.get("bl_code")),
        reys_number=_clean_text(args.get("reys_number")),
        fura_number=_clean_text(args.get("fura")),
        status=_clean_text(args.get("status")),
        currency=_clean_text(args.get("currency")),
        bank_or_cash=_clean_text(args.get("bank_or_cash")),
        category=_clean_text(args.get("category")),
        warehouse=_clean_text(args.get("warehouse")),
    )


def _ensure_demo_data_if_needed() -> None:
    if _safe_lower(os.getenv("FLASK_ENV")) == "production":
        return
    conn = db.get_conn()
    try:
        has_data = conn.execute(
            "SELECT EXISTS(SELECT 1 FROM sales_transactions) OR EXISTS(SELECT 1 FROM cashflow_transactions) OR EXISTS(SELECT 1 FROM shipments)"
        ).fetchone()[0]
        if has_data:
            return
        now_dt = datetime.now()
        created_at = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            """
            INSERT INTO sales_transactions(date, bl_code, client_name, manager_name, service_type, amount, cost, profit, currency, paid_amount, debt_amount, payment_status, source, source_sheet, raw_data_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("01.04.2026", "DEMO-BL-1", "Demo Client", "Demo Manager", "Logistics", 2500, 1700, 800, "USD", 1800, 700, "partial", "demo", "Demo", "{}", created_at, created_at),
        )
        conn.execute(
            """
            INSERT INTO cashflow_transactions(date, type, category, amount, currency, bank_or_cash, contractor, bl_code, reys_number, comment, source_sheet, raw_data_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("02.04.2026", "income", "Sales", 2500, "USD", "Bank", "Demo Client", "DEMO-BL-1", "DEMO-REYS", "Demo income", "Demo", "{}", created_at, created_at),
        )
        conn.execute(
            """
            INSERT INTO cashflow_transactions(date, type, category, amount, currency, bank_or_cash, contractor, bl_code, reys_number, comment, source_sheet, raw_data_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("03.04.2026", "expense", "Fuel", 500, "USD", "Cash", "Fuel Vendor", "DEMO-BL-1", "DEMO-REYS", "Demo expense", "Demo", "{}", created_at, created_at),
        )
        conn.execute(
            """
            INSERT INTO shipments(bl_code, client_name, reys_number, fura_number, container_type, station, agent, logist_name, sales_manager_name, warehouse, status, loaded_date, arrived_date, expected_date, cargo_type, weight_kg, volume_m3, places, places_breakdown, description, source_sheet, raw_data_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("DEMO-BL-1", "Demo Client", "DEMO-REYS", "FURA-1", "Demo", "Xitoy", "Demo Agent", "Demo Logist", "Demo Manager", "Yiwu", "Horgos (Qozoq)", "01.04.2026", "", "19.04.2026", "Electronics", 1200, 7.5, 32, "10 + 12 + 10", "Demo cargo", "Demo", "{}", created_at, created_at),
        )
        conn.commit()
    finally:
        conn.close()


def _fetch_table(table_name: str) -> list[dict[str, Any]]:
    conn = db.get_conn()
    try:
        rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _fetch_local_shipments() -> list[dict[str, Any]]:
    conn = db.get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                bl.id,
                bl.code AS bl_code,
                bl.client_name,
                b.name AS reys_number,
                '' AS fura_number,
                '' AS container_type,
                b.status AS station,
                '' AS agent,
                '' AS logist_name,
                '' AS sales_manager_name,
                '' AS warehouse,
                b.status AS status,
                b.name AS loaded_date,
                COALESCE(b.toshkent_arrived_at, '') AS arrived_date,
                COALESCE(b.eta_to_toshkent, '') AS expected_date,
                bl.cargo_type,
                bl.weight_kg,
                bl.volume_cbm AS volume_m3,
                bl.quantity_places AS places,
                COALESCE(bl.quantity_places_breakdown, '') AS places_breakdown,
                bl.cargo_description AS description,
                COALESCE(b.client_delivery_date, '') AS client_delivery_date,
                COALESCE(b.route_started_at, '') AS route_started_at,
                COALESCE(b.toshkent_arrived_at, '') AS toshkent_arrived_at,
                COALESCE(bl.chat_id, '') AS chat_id
            FROM bl_codes bl
            JOIN batches b ON b.id = bl.batch_id
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _load_dataset() -> dict[str, list[dict[str, Any]]]:
    _ensure_demo_data_if_needed()
    imported_shipments = _fetch_table("shipments")
    local_shipments = _fetch_local_shipments()
    sales = _fetch_table("sales_transactions")
    cashflow = _fetch_table("cashflow_transactions")

    local_map = {(_clean_text(item.get("bl_code")).upper()): item for item in local_shipments if _clean_text(item.get("bl_code"))}
    merged_shipments: list[dict[str, Any]] = []
    imported_seen: set[str] = set()

    for row in imported_shipments:
        code = _clean_text(row.get("bl_code")).upper()
        imported_seen.add(code)
        local = local_map.get(code)
        merged = dict(row)
        if local:
            for key in ("client_name", "reys_number", "status", "cargo_type", "description", "chat_id", "route_started_at", "toshkent_arrived_at", "client_delivery_date"):
                if local.get(key):
                    merged[key] = local.get(key)
        merged_shipments.append(merged)

    for code, local in local_map.items():
        if code not in imported_seen:
            merged_shipments.append(dict(local))

    return {"sales": sales, "cashflow": cashflow, "shipments": merged_shipments}


def _matches_filters(row: dict[str, Any], filters: AnalyticsFilters, row_type: str) -> bool:
    row_date = _parse_date(row.get("date") or row.get("loaded_date") or row.get("requested_at"))
    if filters.date_from and row_date and row_date < filters.date_from:
        return False
    if filters.date_to and row_date and row_date > filters.date_to:
        return False
    if filters.date_from and filters.date_to and row_date is None and row_type != "sync":
        return False

    comparisons = [
        (filters.manager, row.get("manager_name") or row.get("sales_manager_name") or row.get("logist_name")),
        (filters.client, row.get("client_name")),
        (filters.bl_code, row.get("bl_code")),
        (filters.reys_number, row.get("reys_number")),
        (filters.fura_number, row.get("fura_number")),
        (filters.status, row.get("status")),
        (filters.currency, row.get("currency")),
        (filters.bank_or_cash, row.get("bank_or_cash")),
        (filters.category, row.get("category")),
        (filters.warehouse, row.get("warehouse")),
    ]
    for needle, hay in comparisons:
        if needle and _safe_lower(needle) not in _safe_lower(hay):
            return False
    return True


def _filter_rows(rows: list[dict[str, Any]], filters: AnalyticsFilters, row_type: str) -> list[dict[str, Any]]:
    return [row for row in rows if _matches_filters(row, filters, row_type)]


def _filter_options(dataset: dict[str, list[dict[str, Any]]]) -> dict[str, list[str]]:
    options = {
        "managers": set(),
        "clients": set(),
        "bl_codes": set(),
        "reys_numbers": set(),
        "furas": set(),
        "statuses": set(),
        "currencies": set(),
        "bank_or_cash": set(),
        "categories": set(),
        "warehouses": set(),
    }
    for row in dataset["sales"]:
        if row.get("manager_name"):
            options["managers"].add(_clean_text(row["manager_name"]))
        if row.get("client_name"):
            options["clients"].add(_clean_text(row["client_name"]))
        if row.get("bl_code"):
            options["bl_codes"].add(_clean_text(row["bl_code"]).upper())
        if row.get("currency"):
            options["currencies"].add(_clean_text(row["currency"]).upper())
    for row in dataset["cashflow"]:
        if row.get("category"):
            options["categories"].add(_clean_text(row["category"]))
        if row.get("bank_or_cash"):
            options["bank_or_cash"].add(_clean_text(row["bank_or_cash"]))
        if row.get("currency"):
            options["currencies"].add(_clean_text(row["currency"]).upper())
        if row.get("bl_code"):
            options["bl_codes"].add(_clean_text(row["bl_code"]).upper())
        if row.get("reys_number"):
            options["reys_numbers"].add(_clean_text(row["reys_number"]))
    for row in dataset["shipments"]:
        if row.get("client_name"):
            options["clients"].add(_clean_text(row["client_name"]))
        if row.get("bl_code"):
            options["bl_codes"].add(_clean_text(row["bl_code"]).upper())
        if row.get("reys_number"):
            options["reys_numbers"].add(_clean_text(row["reys_number"]))
        if row.get("fura_number"):
            options["furas"].add(_clean_text(row["fura_number"]))
        if row.get("status"):
            options["statuses"].add(_clean_text(row["status"]))
        if row.get("warehouse"):
            options["warehouses"].add(_clean_text(row["warehouse"]))
        if row.get("sales_manager_name"):
            options["managers"].add(_clean_text(row["sales_manager_name"]))
        if row.get("logist_name"):
            options["managers"].add(_clean_text(row["logist_name"]))
    return {key: sorted(value) for key, value in options.items()}


def _group_month_series(rows: list[dict[str, Any]], value_getter, rates: dict[str, float] | None = None) -> list[dict[str, Any]]:
    buckets: dict[str, float] = defaultdict(float)
    for row in rows:
        key = _month_key(row.get("date") or row.get("loaded_date"))
        if not key:
            continue
        value = value_getter(row)
        if rates is not None:
            converted = _convert_to_base(value, row.get("currency"), rates)
            if converted is None:
                continue
            value = converted
        buckets[key] += value
    return [{"month": month, "value": round(value, 2)} for month, value in sorted(buckets.items())]


def _currency_balance(rows: list[dict[str, Any]]) -> dict[str, float]:
    result: dict[str, float] = defaultdict(float)
    for row in rows:
        currency = _clean_text(row.get("currency")).upper() or BASE_CURRENCY
        amount = _to_float(row.get("amount"))
        if _safe_lower(row.get("type")) == "expense":
            amount *= -1
        result[currency] += amount
    return dict(result)


def _delivery_days(row: dict[str, Any]) -> int | None:
    loaded = _parse_date(row.get("loaded_date") or row.get("route_started_at") or row.get("reys_number"))
    arrived = _parse_date(row.get("arrived_date") or row.get("client_delivery_date") or row.get("toshkent_arrived_at"))
    if not loaded:
        return None
    if not arrived:
        return (_now().date() - loaded).days
    return (arrived - loaded).days


def get_overview(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    filters = parse_filters(args)
    rates = _load_rates()
    sales = _filter_rows(dataset["sales"], filters, "sales")
    cashflow = _filter_rows(dataset["cashflow"], filters, "cashflow")
    shipments = _filter_rows(dataset["shipments"], filters, "shipments")

    total_sales_raw = _combine_currency_totals(sales, "amount")
    income_rows = [row for row in cashflow if _safe_lower(row.get("type")) == "income"]
    expense_rows = [row for row in cashflow if _safe_lower(row.get("type")) == "expense"]
    total_income_raw = _combine_currency_totals(income_rows, "amount")
    total_expense_raw = _combine_currency_totals(expense_rows, "amount")
    debt_raw = _combine_currency_totals(sales, "debt_amount")

    total_sales_base, missing_sales = _sum_converted(sales, "amount", rates)
    total_income_base, missing_income = _sum_converted(income_rows, "amount", rates)
    total_expense_base, missing_expense = _sum_converted(expense_rows, "amount", rates)
    debt_base, missing_debt = _sum_converted(sales, "debt_amount", rates)
    profit_base = total_income_base - total_expense_base
    margin = (profit_base / total_income_base * 100.0) if abs(total_income_base) > 0.00001 else None

    active_shipments = [row for row in shipments if not _clean_text(row.get("client_delivery_date"))]
    arrived_shipments = [row for row in shipments if _clean_text(row.get("arrived_date"))]
    delayed_shipments = [row for row in shipments if (_delivery_days(row) or 0) > DELAY_THRESHOLD_DAYS and not _clean_text(row.get("client_delivery_date"))]

    current_start, current_end = filters.date_from, filters.date_to
    previous_sales_rows = []
    if current_start and current_end:
        prev_days = (current_end - current_start).days + 1
        prev_end = current_start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=prev_days - 1)
        prev_filters = AnalyticsFilters(**{**filters.__dict__, "date_from": prev_start, "date_to": prev_end})
        previous_sales_rows = _filter_rows(dataset["sales"], prev_filters, "sales")
    current_sales_value = total_sales_base
    previous_sales_value, missing_prev = _sum_converted(previous_sales_rows, "amount", rates) if previous_sales_rows else (0.0, [])
    growth_percent = _percent_change(current_sales_value, previous_sales_value)

    deals_count = len({(_clean_text(row.get("bl_code")) or f"sale:{idx}") for idx, row in enumerate(sales)})
    average_check_base = (total_sales_base / deals_count) if deals_count else 0.0

    status_counter = Counter(_clean_text(row.get("status")) or "Unknown" for row in active_shipments)
    smart_insights = []
    if growth_percent is not None:
        arrow = "📈" if growth_percent >= 0 else "📉"
        smart_insights.append(f"{arrow} Savdo o‘tgan davrga nisbatan {abs(growth_percent):.1f}% ga {'oshdi' if growth_percent >= 0 else 'kamaydi'}.")
    if debt_raw:
        top_debt = max(sales, key=lambda row: _to_float(row.get("debt_amount")), default=None)
        if top_debt and _to_float(top_debt.get("debt_amount")) > 0:
            smart_insights.append(
                f"⚠️ Eng katta qarz: {_clean_text(top_debt.get('client_name')) or 'Noma’lum'} — {_format_money(_to_float(top_debt.get('debt_amount')), _clean_text(top_debt.get('currency')) or BASE_CURRENCY)}."
            )
    if status_counter:
        common_status, _ = status_counter.most_common(1)[0]
        smart_insights.append(f"🚛 Eng ko‘p aktiv BL hozir {common_status} bosqichida.")
    if total_income_base and len(active_shipments) > len(arrived_shipments):
        smart_insights.append("📉 Kirim kamaygan, lekin BL soni oshgan.")

    missing_currencies = sorted(set(missing_sales + missing_income + missing_expense + missing_debt))
    return {
        "filters": _filter_options(dataset),
        "selected_filters": {
            "period": filters.preset,
            "date_from": _date_to_str(filters.date_from),
            "date_to": _date_to_str(filters.date_to),
        },
        "kpis": {
            "total_sales": {"display": _format_currency_totals(total_sales_raw), "base_value": round(total_sales_base, 2)},
            "monthly_growth": {"display": "—" if growth_percent is None else f"{growth_percent:.1f}%", "value": growth_percent},
            "income": {"display": _format_currency_totals(total_income_raw), "base_value": round(total_income_base, 2)},
            "expense": {"display": _format_currency_totals(total_expense_raw), "base_value": round(total_expense_base, 2)},
            "profit": {"display": _format_money(profit_base, BASE_CURRENCY), "value": round(profit_base, 2)},
            "margin": {"display": "—" if margin is None else f"{margin:.1f}%", "value": margin},
            "debt": {"display": _format_currency_totals(debt_raw), "base_value": round(debt_base, 2)},
            "active_bl_count": {"display": str(len(active_shipments)), "value": len(active_shipments)},
            "arrived_shipments_count": {"display": str(len(arrived_shipments)), "value": len(arrived_shipments)},
            "delayed_shipments_count": {"display": str(len(delayed_shipments)), "value": len(delayed_shipments)},
            "average_deal": {"display": _format_money(average_check_base, BASE_CURRENCY), "value": round(average_check_base, 2)},
        },
        "smart_insights": smart_insights,
        "meta": {
            "missing_currencies": missing_currencies,
            "base_currency": BASE_CURRENCY,
            "has_data": bool(dataset["sales"] or dataset["cashflow"] or dataset["shipments"]),
        },
    }


def get_sales_growth(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    filters = parse_filters(args)
    rates = _load_rates()
    sales = _filter_rows(dataset["sales"], filters, "sales")
    cashflow = _filter_rows(dataset["cashflow"], filters, "cashflow")
    income_rows = [row for row in cashflow if _safe_lower(row.get("type")) == "income"]
    expense_rows = [row for row in cashflow if _safe_lower(row.get("type")) == "expense"]

    sales_series = _group_month_series(sales, lambda row: _to_float(row.get("amount")), rates)
    income_series = _group_month_series(income_rows, lambda row: _to_float(row.get("amount")), rates)
    expense_series = _group_month_series(expense_rows, lambda row: _to_float(row.get("amount")), rates)

    expense_by_month = {item["month"]: item["value"] for item in expense_series}
    profit_series = [{"month": item["month"], "value": round(item["value"] - expense_by_month.get(item["month"], 0), 2)} for item in income_series]

    growth_series = []
    previous = None
    for item in sales_series:
        value = item["value"]
        growth_series.append({"month": item["month"], "value": _percent_change(value, previous) if previous not in (None, 0) else None})
        previous = value

    current_month = sales_series[-1]["value"] if sales_series else 0
    previous_month = sales_series[-2]["value"] if len(sales_series) > 1 else 0
    delta = current_month - previous_month
    growth = _percent_change(current_month, previous_month)
    return {
        "summary": {
            "current_month": round(current_month, 2),
            "previous_month": round(previous_month, 2),
            "difference": round(delta, 2),
            "growth_percent": growth,
            "base_currency": BASE_CURRENCY,
        },
        "series": {
            "sales": sales_series,
            "income": income_series,
            "profit": profit_series,
            "growth": growth_series,
        },
        "empty": not bool(sales_series or income_series),
    }


def get_cashflow(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    filters = parse_filters(args)
    rates = _load_rates()
    cashflow = _filter_rows(dataset["cashflow"], filters, "cashflow")
    income_rows = [row for row in cashflow if _safe_lower(row.get("type")) == "income"]
    expense_rows = [row for row in cashflow if _safe_lower(row.get("type")) == "expense"]

    income_display = _format_currency_totals(_combine_currency_totals(income_rows, "amount"))
    expense_display = _format_currency_totals(_combine_currency_totals(expense_rows, "amount"))
    income_base, _ = _sum_converted(income_rows, "amount", rates)
    expense_base, _ = _sum_converted(expense_rows, "amount", rates)
    net_base = income_base - expense_base

    bank_balances: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in cashflow:
        place = _clean_text(row.get("bank_or_cash")) or "Noma’lum"
        currency = _clean_text(row.get("currency")).upper() or BASE_CURRENCY
        amount = _to_float(row.get("amount"))
        if _safe_lower(row.get("type")) == "expense":
            amount *= -1
        bank_balances[place][currency] += amount

    expense_categories = Counter()
    income_categories = Counter()
    for row in expense_rows:
        expense_categories[_clean_text(row.get("category")) or "Boshqa"] += _to_float(row.get("amount"))
    for row in income_rows:
        income_categories[_clean_text(row.get("category")) or "Boshqa"] += _to_float(row.get("amount"))

    rows = [
        {
            "date": _clean_text(row.get("date")),
            "category": _clean_text(row.get("category")),
            "type": _clean_text(row.get("type")),
            "amount": round(_to_float(row.get("amount")), 2),
            "currency": _clean_text(row.get("currency")).upper() or BASE_CURRENCY,
            "bank_or_cash": _clean_text(row.get("bank_or_cash")),
            "comment": _clean_text(row.get("comment")),
            "bl_or_reys": _clean_text(row.get("bl_code")) or _clean_text(row.get("reys_number")),
        }
        for row in sorted(cashflow, key=lambda item: (_parse_date(item.get("date")) or date.min), reverse=True)
    ]

    return {
        "kpis": {
            "income": income_display,
            "expense": expense_display,
            "net_profit": _format_money(net_base, BASE_CURRENCY),
            "balance_by_currency": {currency: _format_money(amount, currency) for currency, amount in _currency_balance(cashflow).items()},
        },
        "charts": {
            "income_vs_expense": {
                "income": _group_month_series(income_rows, lambda row: _to_float(row.get("amount")), rates),
                "expense": _group_month_series(expense_rows, lambda row: _to_float(row.get("amount")), rates),
            },
            "expense_by_category": [{"label": label, "value": round(value, 2)} for label, value in expense_categories.items()],
            "income_by_category": [{"label": label, "value": round(value, 2)} for label, value in income_categories.items()],
            "balances": [{"label": label, "values": dict(values)} for label, values in bank_balances.items()],
        },
        "table": rows,
        "empty": not bool(cashflow),
    }


def get_managers(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    filters = parse_filters(args)
    rates = _load_rates()
    sales = _filter_rows(dataset["sales"], filters, "sales")
    shipments = _filter_rows(dataset["shipments"], filters, "shipments")

    grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "manager_name": "",
        "sales_total_raw": defaultdict(float),
        "bl_codes": set(),
        "paid_raw": defaultdict(float),
        "debt_raw": defaultdict(float),
        "profit_raw": defaultdict(float),
        "status_counter": Counter(),
        "late_count": 0,
    })

    for row in sales:
        manager = _clean_text(row.get("manager_name")) or "Belgilanmagan"
        bucket = grouped[manager]
        bucket["manager_name"] = manager
        currency = _clean_text(row.get("currency")).upper() or BASE_CURRENCY
        bucket["sales_total_raw"][currency] += _to_float(row.get("amount"))
        bucket["paid_raw"][currency] += _to_float(row.get("paid_amount"))
        bucket["debt_raw"][currency] += _to_float(row.get("debt_amount"))
        bucket["profit_raw"][currency] += _to_float(row.get("profit"))
        if row.get("bl_code"):
            bucket["bl_codes"].add(_clean_text(row.get("bl_code")).upper())
        bucket["status_counter"][_clean_text(row.get("payment_status")) or "—"] += 1

    for row in shipments:
        manager = _clean_text(row.get("sales_manager_name")) or _clean_text(row.get("logist_name")) or ""
        if not manager:
            continue
        bucket = grouped[manager]
        bucket["manager_name"] = manager
        if row.get("bl_code"):
            bucket["bl_codes"].add(_clean_text(row.get("bl_code")).upper())
        if (_delivery_days(row) or 0) > DELAY_THRESHOLD_DAYS:
            bucket["late_count"] += 1

    rows = []
    for manager, bucket in grouped.items():
        sales_display = _format_currency_totals(bucket["sales_total_raw"])
        paid_display = _format_currency_totals(bucket["paid_raw"])
        debt_display = _format_currency_totals(bucket["debt_raw"])
        profit_display = _format_currency_totals(bucket["profit_raw"])
        average_check = 0.0
        converted_sales, _ = _sum_converted([{"amount": amount, "currency": currency} for currency, amount in bucket["sales_total_raw"].items()], "amount", rates)
        if bucket["bl_codes"]:
            average_check = converted_sales / len(bucket["bl_codes"])
        rows.append(
            {
                "manager_name": manager,
                "sales_total": sales_display,
                "bl_count": len(bucket["bl_codes"]),
                "paid_amount": paid_display,
                "debt_amount": debt_display,
                "average_check": _format_money(average_check, BASE_CURRENCY),
                "profit": profit_display,
                "status": bucket["status_counter"].most_common(1)[0][0] if bucket["status_counter"] else "—",
                "late_count": bucket["late_count"],
                "sort_sales": converted_sales,
                "sort_debt": sum(bucket["debt_raw"].values()),
            }
        )

    rows.sort(key=lambda item: item["sort_sales"], reverse=True)
    return {
        "table": rows,
        "ranking": {
            "best_sales_manager": rows[0]["manager_name"] if rows else "",
            "most_sales": rows[0]["sales_total"] if rows else "",
            "most_bl_manager": max(rows, key=lambda item: item["bl_count"])["manager_name"] if rows else "",
            "most_unpaid_manager": max(rows, key=lambda item: item["sort_debt"])["manager_name"] if rows else "",
            "most_delays_manager": max(rows, key=lambda item: item["late_count"])["manager_name"] if rows else "",
        },
        "empty": not bool(rows),
    }


def get_shipments(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    filters = parse_filters(args)
    shipments = _filter_rows(dataset["shipments"], filters, "shipments")
    local_status_counts = Counter(_clean_text(item.get("status")) or "Unknown" for item in shipments)

    active = [row for row in shipments if not _clean_text(row.get("client_delivery_date"))]
    arrived = [row for row in shipments if _clean_text(row.get("arrived_date"))]
    delayed = [row for row in shipments if (_delivery_days(row) or 0) > DELAY_THRESHOLD_DAYS and not _clean_text(row.get("client_delivery_date"))]
    sent_by_month = _group_month_series(shipments, lambda row: 1)
    arrived_by_month = _group_month_series(arrived, lambda row: 1)
    delivery_day_values = [value for value in (_delivery_days(row) for row in arrived) if value is not None]
    average_delivery = sum(delivery_day_values) / len(delivery_day_values) if delivery_day_values else 0

    table = []
    for row in sorted(shipments, key=lambda item: (_parse_date(item.get("loaded_date")) or date.min), reverse=True):
        loaded = _parse_date(row.get("loaded_date") or row.get("reys_number"))
        arrived_dt = _parse_date(row.get("arrived_date") or row.get("client_delivery_date") or row.get("toshkent_arrived_at"))
        table.append(
            {
                "bl_code": _clean_text(row.get("bl_code")).upper(),
                "client_name": _clean_text(row.get("client_name")),
                "reys_number": _clean_text(row.get("reys_number")),
                "fura_number": _clean_text(row.get("fura_number")),
                "status": _clean_text(row.get("status")),
                "loaded_date": _date_to_str(loaded),
                "arrived_date": _date_to_str(arrived_dt),
                "days": _delivery_days(row),
                "manager_name": _clean_text(row.get("sales_manager_name")),
                "logist_name": _clean_text(row.get("logist_name")),
            }
        )

    return {
        "kpis": {
            "sent_furas": sum(item["value"] for item in sent_by_month),
            "arrived_furas": sum(item["value"] for item in arrived_by_month),
            "active_bl": len(active),
            "in_transit_bl": len([row for row in active if _clean_text(row.get("status")) not in {"Toshkent(Chuqursoy ULS da)", "Mijozga yetkazib berildi"}]),
            "china_count": local_status_counts.get("Xitoy", 0) + local_status_counts.get("Yiwu", 0) + local_status_counts.get("Zhongshan", 0),
            "horgos_count": local_status_counts.get("Horgos (Qozoq)", 0),
            "yallama_count": local_status_counts.get("Yallama", 0),
            "toshkent_count": local_status_counts.get("Toshkent(Chuqursoy ULS da)", 0),
            "chuqursoy_count": local_status_counts.get("Toshkent(Chuqursoy ULS da)", 0),
            "average_delivery_days": round(average_delivery, 1),
            "delayed_shipments": len(delayed),
        },
        "series": {
            "sent_by_month": sent_by_month,
            "arrived_by_month": arrived_by_month,
            "status_counts": [{"label": label, "value": value} for label, value in local_status_counts.items()],
        },
        "table": table,
        "empty": not bool(shipments),
    }


def get_debts(args: Any) -> dict[str, Any]:
    dataset = _load_dataset()
    filters = parse_filters(args)
    sales = _filter_rows(dataset["sales"], filters, "sales")
    debt_rows = [row for row in sales if _to_float(row.get("debt_amount")) > 0]
    total_debt = _combine_currency_totals(debt_rows, "debt_amount")

    rows = []
    for row in debt_rows:
        sale_date = _parse_date(row.get("date"))
        days = (_now().date() - sale_date).days if sale_date else 0
        payment_status = _clean_text(row.get("payment_status")).lower()
        if _to_float(row.get("debt_amount")) <= 0:
            state = "green"
        elif _to_float(row.get("paid_amount")) > 0:
            state = "yellow"
        else:
            state = "red" if days > 30 else "yellow"
        rows.append(
            {
                "client_name": _clean_text(row.get("client_name")),
                "bl_code": _clean_text(row.get("bl_code")).upper(),
                "amount": _format_money(_to_float(row.get("amount")), _clean_text(row.get("currency")).upper() or BASE_CURRENCY),
                "paid_amount": _format_money(_to_float(row.get("paid_amount")), _clean_text(row.get("currency")).upper() or BASE_CURRENCY),
                "debt_amount": _format_money(_to_float(row.get("debt_amount")), _clean_text(row.get("currency")).upper() or BASE_CURRENCY),
                "currency": _clean_text(row.get("currency")).upper() or BASE_CURRENCY,
                "days": days,
                "manager_name": _clean_text(row.get("manager_name")),
                "comment": _clean_text(row.get("payment_status")),
                "state": state,
            }
        )

    by_manager = Counter()
    for row in debt_rows:
        by_manager[_clean_text(row.get("manager_name")) or "Belgilanmagan"] += _to_float(row.get("debt_amount"))
    return {
        "summary": {
            "total_debt": _format_currency_totals(total_debt),
            "overdue_count": len([row for row in rows if row["state"] == "red"]),
            "partial_paid_count": len([row for row in rows if row["state"] == "yellow"]),
            "manager_debt_leader": by_manager.most_common(1)[0][0] if by_manager else "",
        },
        "table": rows,
        "empty": not bool(rows),
    }


def get_sync_settings_payload() -> dict[str, Any]:
    from services import sheets_importer

    return sheets_importer.get_sync_status()


def get_export_dataset(report_type: str, args: Any) -> tuple[str, list[dict[str, Any]]]:
    report_type = _clean_text(report_type).lower()
    if report_type == "sales":
        rows = _filter_rows(_load_dataset()["sales"], parse_filters(args), "sales")
        return "sales_report", rows
    if report_type == "cashflow":
        rows = _filter_rows(_load_dataset()["cashflow"], parse_filters(args), "cashflow")
        return "cashflow_report", rows
    if report_type == "managers":
        data = get_managers(args)
        return "manager_kpi_report", data["table"]
    if report_type == "debts":
        data = get_debts(args)
        return "debts_report", data["table"]
    if report_type == "shipments":
        data = get_shipments(args)
        return "shipments_report", data["table"]
    raise ValueError("Unknown report type")
