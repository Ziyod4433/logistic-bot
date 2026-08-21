"""PDF-отчёты для AI-ассистента: партии, состав партии, опоздания,
проблемы, логи отправок. Рендер fpdf2 + DejaVu (кириллица/латиница)."""

import os
from datetime import datetime
from pathlib import Path

from fpdf import FPDF

import database as db

_FONT_DIR = Path(__file__).resolve().parent.parent / "static" / "fonts"


class _ReportPDF(FPDF):
    def __init__(self, title: str):
        super().__init__(orientation="L", unit="mm", format="A4")
        self.report_title = title
        self.add_font("DejaVu", "", str(_FONT_DIR / "DejaVuSans.ttf"))
        self.add_font("DejaVu", "B", str(_FONT_DIR / "DejaVuSans-Bold.ttf"))
        self.set_auto_page_break(auto=True, margin=14)
        self.add_page()

    def header(self):
        self.set_font("DejaVu", "B", 13)
        self.set_text_color(20, 20, 20)
        self.cell(0, 7, "BURAQ LOGISTICS", align="L")
        self.set_font("DejaVu", "", 9)
        self.set_text_color(110, 110, 110)
        stamp = datetime.now(db.TASHKENT_TZ).strftime("%d.%m.%Y %H:%M")
        self.cell(0, 7, stamp, align="R")
        self.ln(8)
        self.set_font("DejaVu", "B", 11)
        self.set_text_color(20, 20, 20)
        self.cell(0, 7, self.report_title, align="L")
        self.ln(9)

    def footer(self):
        self.set_y(-12)
        self.set_font("DejaVu", "", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 6, f"стр. {self.page_no()}", align="C")


def _fit(pdf: FPDF, text, width: float) -> str:
    """Обрезать текст под ширину колонки (с многоточием)."""
    value = str(text if text is not None else "").replace("\n", " ").strip()
    if not value:
        return ""
    max_w = width - 2.5
    if pdf.get_string_width(value) <= max_w:
        return value
    while value and pdf.get_string_width(value + "…") > max_w:
        value = value[:-1]
    return value + "…"


def _table(pdf: _ReportPDF, headers: list, widths: list, rows: list):
    row_h = 6.5
    pdf.set_font("DejaVu", "B", 8.5)
    pdf.set_fill_color(232, 236, 244)
    pdf.set_text_color(20, 20, 20)
    for title, w in zip(headers, widths):
        pdf.cell(w, row_h, _fit(pdf, title, w), border=1, align="C", fill=True)
    pdf.ln(row_h)
    pdf.set_font("DejaVu", "", 8.5)
    stripe = False
    for row in rows:
        pdf.set_fill_color(247, 248, 251)
        for value, w in zip(row, widths):
            pdf.cell(w, row_h, _fit(pdf, value, w), border=1, fill=stripe)
        pdf.ln(row_h)
        stripe = not stripe
    if not rows:
        pdf.set_text_color(120, 120, 120)
        pdf.cell(sum(widths), row_h, "— данных нет —", border=1, align="C")
        pdf.ln(row_h)


def _fmt_num(value) -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value or "")
    if not num:
        return ""
    return f"{num:g}"


def _group_titles() -> dict:
    conn = db.get_conn()
    try:
        return {
            r["chat_id"]: r["title"]
            for r in conn.execute("SELECT chat_id, title FROM telegram_chats").fetchall()
        }
    finally:
        conn.close()


def _report_batches():
    batches = db.get_batches()
    rows = []
    for b in batches:
        active = not (b.get("client_delivery_date") or "")
        rows.append([
            b.get("name"),
            b.get("status"),
            b.get("bl_count"),
            b.get("linked_count"),
            b.get("eta_to_toshkent") or "",
            (b.get("toshkent_arrived_at") or "")[:10],
            b.get("client_delivery_date") or "",
            "активна" if active else "закрыта",
        ])
    pdf = _ReportPDF("Отчёт по партиям")
    _table(
        pdf,
        ["Партия", "Статус", "BL", "Группы", "ETA до Toshkent", "Прибытие", "Доставка", "Состояние"],
        [34, 62, 14, 18, 40, 28, 28, 24],
        rows,
    )
    return pdf, "buraq_batches.pdf", f"📊 Отчёт по партиям: всего {len(rows)}"


def _report_batch_detail(args: dict):
    batch = db.get_batch(int(args.get("batch_id") or 0))
    if not batch:
        raise ValueError("Партия не найдена")
    bls = db.get_bl_by_batch(batch["id"])
    titles = _group_titles()
    rows = []
    for bl in bls:
        rows.append([
            bl.get("code"),
            bl.get("client_name") or "",
            titles.get(str(bl.get("chat_id") or ""), "") or ("—" if not bl.get("chat_id") else str(bl.get("chat_id"))),
            bl.get("quantity_places_breakdown") or bl.get("quantity_places") or "",
            _fmt_num(bl.get("weight_kg")),
            _fmt_num(bl.get("volume_cbm")),
            bl.get("file_count") or 0,
            "да" if bl.get("tracking_sent_current") else "нет",
        ])
    pdf = _ReportPDF(
        f"Партия {batch['name']} — {batch.get('status') or ''}"
        + (f" · ETA: {batch.get('eta_to_toshkent')}" if batch.get("eta_to_toshkent") else "")
    )
    _table(
        pdf,
        ["BL", "Клиент", "Группа", "Места", "Кг", "М³", "Файлы", "Трекинг"],
        [40, 42, 74, 34, 22, 20, 16, 20],
        rows,
    )
    return pdf, f"buraq_batch_{batch['name'].replace('.', '_')}.pdf", f"📦 Партия {batch['name']}: {len(rows)} BL"


def _report_late_cargo():
    from services import late_cargo_service

    rep = late_cargo_service.get_late_cargo_report()
    if not rep.get("ok"):
        raise ValueError(rep.get("error") or "Шитс статусов недоступен")
    rows = []
    for g in rep.get("groups", []):
        for it in g.get("items", []):
            rows.append([
                g.get("label"),
                it.get("brand"),
                it.get("sklad") or "",
                it.get("seller") or "",
                it.get("arrived") or "",
                it.get("days_late"),
            ])
    pdf = _ReportPDF(f"Опаздывающие грузы (всего {rep.get('total_late')})")
    _table(
        pdf,
        ["Рейс", "Бренд", "Склад", "Продавец", "Прибыл на склад", "Дней опоздания"],
        [46, 62, 40, 50, 36, 30],
        rows,
    )
    return pdf, "buraq_late_cargo.pdf", f"⏰ Опаздывающие грузы: {rep.get('total_late')}"


def _report_problems():
    problems = db.get_problems()
    rows = [
        [
            p.get("bl_code") or p.get("code") or "",
            p.get("problem_type"),
            p.get("description") or "",
            p.get("status"),
            (p.get("created_at") or "")[:16],
        ]
        for p in problems
    ]
    pdf = _ReportPDF("Проблемы по грузам")
    _table(pdf, ["BL", "Тип", "Описание", "Статус", "Создана"], [36, 34, 120, 26, 32], rows)
    return pdf, "buraq_problems.pdf", f"⚠️ Проблемы: {len(rows)}"


def _report_send_logs(args: dict):
    limit = max(1, min(200, int(args.get("limit") or 30)))
    conn = db.get_conn()
    try:
        logs = conn.execute(
            """
            SELECT bl_code, batch_name, status, success, error_msg, sent_at
            FROM send_logs ORDER BY id DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    rows = [
        [
            r["bl_code"],
            r["batch_name"],
            r["status"],
            "✓" if r["success"] else "✗",
            r["error_msg"] or "",
            (r["sent_at"] or "")[:16],
        ]
        for r in logs
    ]
    pdf = _ReportPDF(f"Последние отправки трекинга ({len(rows)})")
    _table(pdf, ["BL", "Партия", "Статус", "OK", "Ошибка", "Время"], [38, 30, 58, 12, 76, 32], rows)
    return pdf, "buraq_send_logs.pdf", f"📤 Отправки трекинга: последние {len(rows)}"


def build_report(report: str, args: dict):
    """(pdf_bytes, filename, caption) или ValueError с человекочитаемой причиной."""
    kind = (report or "").strip()
    if kind == "batches":
        pdf, filename, caption = _report_batches()
    elif kind == "batch_detail":
        pdf, filename, caption = _report_batch_detail(args or {})
    elif kind == "late_cargo":
        pdf, filename, caption = _report_late_cargo()
    elif kind == "problems":
        pdf, filename, caption = _report_problems()
    elif kind == "send_logs":
        pdf, filename, caption = _report_send_logs(args or {})
    else:
        raise ValueError(
            "Неизвестный отчёт. Доступны: batches, batch_detail, late_cargo, problems, send_logs"
        )
    return bytes(pdf.output()), filename, caption
