"""
bot.py — Telegram-бот для клиентов логистической компании.
Отвечает ТОЛЬКО на кнопку меню и BL-код. Всё остальное игнорирует.
"""
import asyncio
import logging
import os
import sqlite3

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ConversationHandler, ContextTypes, filters,
)
from dotenv import load_dotenv

load_dotenv()

TOKEN   = os.getenv("BOT_TOKEN", "")
DB_PATH = os.path.join(os.path.dirname(__file__), "logistic.db")

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

WAITING_BL = 1

MAIN_KB = ReplyKeyboardMarkup(
    [[KeyboardButton("📦 Узнать статус моего груза")]],
    resize_keyboard=True,
    one_time_keyboard=False,
)

# ── DB helpers ────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def find_bl(code: str):
    conn = get_conn()
    row = conn.execute("""
        SELECT bl.*, b.name as batch_name
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE UPPER(bl.code) = UPPER(?)
    """, (code.strip(),)).fetchone()
    conn.close()
    return dict(row) if row else None

def get_files(bl_id: int):
    conn = get_conn()
    rows = conn.execute("SELECT * FROM files WHERE bl_id = ?", (bl_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_template():
    conn = get_conn()
    row = conn.execute("SELECT content FROM message_template WHERE id=1").fetchone()
    conn.close()
    return row["content"] if row else "{bl_code} — {status}"

def get_status_details():
    conn = get_conn()
    rows = conn.execute("SELECT status, detail FROM status_details").fetchall()
    conn.close()
    return {r["status"]: r["detail"] for r in rows}

def build_message(bl: dict) -> str:
    template = get_template()
    details  = get_status_details()
    status   = bl.get("status", "Принят")
    return template.format(
        batch_name    = bl.get("batch_name", ""),
        bl_code       = bl.get("code", ""),
        client_name   = bl.get("client_name", ""),
        status        = status,
        status_detail = details.get(status, ""),
    )

# ── Handlers ──────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Добро пожаловать!\n\n"
        "Нажмите кнопку ниже чтобы узнать статус вашего груза 👇",
        reply_markup=MAIN_KB,
    )
    return ConversationHandler.END


async def ask_bl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь нажал кнопку — просим BL-код."""
    await update.message.reply_text(
        "🔍 Введите ваш <b>BL-код</b>:\n\n"
        "<i>Пример: BL171</i>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            [["❌ Отмена"]], resize_keyboard=True, one_time_keyboard=True
        ),
    )
    return WAITING_BL


async def handle_bl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получили BL-код — ищем и отвечаем."""
    text = update.message.text.strip()

    if text == "❌ Отмена":
        await update.message.reply_text("Отменено.", reply_markup=MAIN_KB)
        return ConversationHandler.END

    code = text.upper()
    bl   = find_bl(code)

    if not bl:
        await update.message.reply_text(
            f"❌ BL-код <b>{code}</b> не найден.\n\n"
            "Проверьте правильность и попробуйте снова.\n"
            "Если ошибка повторяется — свяжитесь с менеджером.",
            parse_mode="HTML",
            reply_markup=MAIN_KB,
        )
        return ConversationHandler.END

    # Строим сообщение по шаблону
    msg = build_message(bl)

    # Трек-бар маршрута
    all_statuses = ["Принят","Хоргос","Алматы","В пути до Ташкента","Ташкент","Доставлен"]
    current_idx  = all_statuses.index(bl["status"]) if bl["status"] in all_statuses else 0
    track_line   = ""
    for i, s in enumerate(all_statuses):
        if i < current_idx:
            track_line += f"✅ {s}\n"
        elif i == current_idx:
            track_line += f"🔶 {s}  ← сейчас\n"
        else:
            track_line += f"⬜ {s}\n"

    full_msg = msg + "\n\n━━━━━━━━━━━━━━━━\n📍 <b>Маршрут:</b>\n" + track_line

    await update.message.reply_text(full_msg, parse_mode="HTML", reply_markup=MAIN_KB)

    # Отправляем файлы если есть
    files = get_files(bl["id"])
    if files:
        await update.message.reply_text(f"📎 Документы по вашему грузу ({len(files)} шт.):")
        for f in files:
            try:
                with open(f["file_path"], "rb") as fh:
                    await update.message.reply_document(document=fh, filename=f["filename"])
            except Exception as e:
                log.warning(f"Не удалось отправить файл {f['filename']}: {e}")

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Отменено.", reply_markup=MAIN_KB)
    return ConversationHandler.END


async def chatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Служебная команда для администратора."""
    cid   = update.effective_chat.id
    title = update.effective_chat.title or "Личный чат"
    await update.message.reply_text(
        f"📍 Чат: <b>{title}</b>\n🆔 ID: <code>{cid}</code>",
        parse_mode="HTML",
    )


async def ignore(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Молча игнорировать всё что не из меню."""
    pass


# ── Main ──────────────────────────────────────────────────

def main():
    if not TOKEN:
        raise ValueError("BOT_TOKEN не задан в .env!")

    app = ApplicationBuilder().token(TOKEN).build()

    # Диалог: кнопка → BL-код → ответ
    conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("^📦 Узнать статус моего груза$"), ask_bl)
        ],
        states={
            WAITING_BL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_bl)
            ]
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            MessageHandler(filters.Regex("^❌ Отмена$"), cancel),
        ],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("chatid", chatid))
    app.add_handler(conv)

    # Всё остальное — игнорировать молча
    app.add_handler(MessageHandler(filters.ALL, ignore))

    log.info("✅ Бот запущен!")
    app.run_polling()


if __name__ == "__main__":
    asyncio.set_event_loop(asyncio.new_event_loop())
    main()
