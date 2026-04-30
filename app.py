import html
import os
import secrets
import re
import time
import threading
import csv
import io
from datetime import datetime
from functools import wraps
from urllib.parse import parse_qs, urlparse

import mimetypes

import requests as req
from dotenv import load_dotenv
from flask import (
    Response,
    Flask,
    abort,
    jsonify,
    redirect,
    render_template,
    render_template_string,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.utils import secure_filename

import database as db
from services import analytics_importer, analytics_service, monitor_service, report_exporter

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))

ADMIN_LOGIN = os.getenv("ADMIN_LOGIN", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
ADMIN1_LOGIN = os.getenv("ADMIN1_LOGIN", "Admin1")
ADMIN1_PASSWORD = os.getenv("ADMIN1_PASSWORD", "Admin6611")
GUEST_PASSWORD = os.getenv("GUEST_PASSWORD", "Guest6611")
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
PORT = int(os.getenv("PORT", "5000"))
CHAT_ADMIN_CACHE_TTL = 300
CHAT_ADMIN_CACHE = {}
WELCOME_MEDIA_PENDING = set()
WELCOME_MEDIA_LOCK = threading.Lock()
WELCOME_MEDIA_SEMAPHORE = threading.Semaphore(2)
WELCOME_MEDIA_FILE_IDS = {"video": "", "voice": ""}
WELCOME_MEDIA_FILE_IDS_LOCK = threading.Lock()

ROLE_EDITOR = "editor"
ROLE_VIEWER = "viewer"

ALLOWED_EXT = {"pdf", "png", "jpg", "jpeg", "xlsx", "xls", "xlsm", "doc", "docx", "zip"}

TRACK_BUTTON = "Yuk holati"
TRACK_BUTTON_LABELS = {
    "uz_latn": "Yuk holati",
    "uz_cyrl": "Юк ҳолати",
    "ru": "Статус груза",
}
TRACK_BUTTON_TEXTS = set(TRACK_BUTTON_LABELS.values())
GROUP_REMOVE_COMMANDS = {"removebot", "leavebot", "botni_ochir", "hidekeyboard"}
MENU_RESTORE_COMMANDS = {"menu", "keyboard", "showmenu", "tugma", "knopka"}
NO_ACTIVE_CARGO_MESSAGES = {
    "uz_latn": "Hozirgi vaqtda yo'lda kelayotgan yukingiz mavjud emas",
    "uz_cyrl": "Ҳозирги вақтда йўлда келаётган юкингиз мавжуд эмас",
    "ru": "В данный момент у вас нет груза в пути",
}
TRACK_BUTTON_COOLDOWN_SECONDS = 60
TRACK_BUTTON_COOLDOWN_MESSAGES = {
    "uz_latn": "⏳ Iltimos, keyingi so'rov uchun <b>{seconds}</b> soniya kuting.",
    "uz_cyrl": "⏳ Илтимос, кейинги сўров учун <b>{seconds}</b> сония кутинг.",
    "ru": "⏳ Пожалуйста, подождите <b>{seconds}</b> сек. перед следующим запросом.",
}
CANCEL_BUTTON = "❌ Отмена"
STATE_WAITING_BL = "waiting_bl"
COMM_RATE_PREFIX = "comm_rate"
FILE_PREFIX = "file"

CANCEL_REPLY_MARKUP = {
    "keyboard": [[{"text": CANCEL_BUTTON}]],
    "resize_keyboard": True,
    "one_time_keyboard": True,
    "is_persistent": False,
}

REMOVE_REPLY_MARKUP = {
    "remove_keyboard": True,
    "selective": False,
}

UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER") or os.path.join(str(db.APP_DATA_DIR), "uploads")
WELCOME_VIDEO_PATH = os.path.join(os.path.dirname(__file__), "media", "welcome_guide.mp4")
WELCOME_VOICE_PATH = os.path.join(os.path.dirname(__file__), "media", "welcome_voice.ogg")
GOOGLE_SHEETS_URL_SETTING_KEY = "google_sheets_url"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
db.init_db()


def _normalize_sheet_cell(value: str | None) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text


def _extract_sheet_date(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    dot_match = re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", raw)
    if dot_match:
        return dot_match.group(0)
    iso_match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", raw)
    if iso_match:
        year, month, day = iso_match.group(0).split("-")
        return f"{day}.{month}.{year}"
    return ""


def _sheet_float(value) -> float:
    raw = str(value or "").strip().replace("\u00a0", "").replace(" ", "").replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", raw)
    if not match:
        return 0.0
    try:
        return float(match.group(0))
    except ValueError:
        return 0.0


def _sheet_int(value) -> int:
    return int(round(_sheet_float(value)))


def _google_sheets_export_url(sheet_url: str) -> str:
    url = (sheet_url or "").strip()
    if not url:
        raise ValueError("Ссылка на Google Sheets не указана")
    parsed = urlparse(url)
    if "docs.google.com" not in parsed.netloc:
        return url
    if "/export" in parsed.path and ("format=csv" in parsed.query or "output=csv" in parsed.query):
        return url
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", parsed.path)
    if not match:
        raise ValueError("Не удалось распознать ссылку Google Sheets")
    spreadsheet_id = match.group(1)
    query = parse_qs(parsed.query)
    gid = ""
    if query.get("gid"):
        gid = query["gid"][0]
    elif parsed.fragment:
        fragment_match = re.search(r"gid=(\d+)", parsed.fragment)
        if fragment_match:
            gid = fragment_match.group(1)
    export_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv"
    if gid:
        export_url += f"&gid={gid}"
    return export_url


def _fetch_google_sheet_matrix(sheet_url: str) -> list[list[str]]:
    export_url = _google_sheets_export_url(sheet_url)
    response = req.get(export_url, timeout=30)
    response.raise_for_status()
    text = response.content.decode("utf-8-sig", errors="replace")
    return list(csv.reader(io.StringIO(text)))


def _parse_google_sheet_rows(sheet_url: str) -> list[dict]:
    rows = _fetch_google_sheet_matrix(sheet_url)
    if not rows:
        return []

    parsed_rows: list[dict] = []
    aggregated_rows: dict[tuple[str, str], dict] = {}
    seen_ids: set[str] = set()

    def cell(row_index: int, col_index: int) -> str:
        if row_index < 0 or row_index >= len(rows):
            return ""
        row = rows[row_index]
        if col_index < 0 or col_index >= len(row):
            return ""
        return str(row[col_index] or "").strip()

    for header_row_index, row in enumerate(rows):
        for start_col_index, value in enumerate(row):
            if _normalize_sheet_cell(value) != "shipping mark":
                continue

            sheet_date = ""
            for up_index in range(header_row_index - 1, -1, -1):
                search_row = rows[up_index]
                left = max(0, start_col_index - 1)
                right = min(len(search_row), start_col_index + 6)
                for candidate in search_row[left:right]:
                    sheet_date = _extract_sheet_date(candidate)
                    if sheet_date:
                        break
                if sheet_date:
                    break

            blank_streak = 0
            data_row_index = header_row_index + 1
            while data_row_index < len(rows):
                code = cell(data_row_index, start_col_index)
                relevant = [cell(data_row_index, start_col_index + offset) for offset in range(4)]
                normalized_code = _normalize_sheet_cell(code)

                if normalized_code == "shipping mark":
                    break
                if normalized_code in {"total", "итого"}:
                    break

                if not any(relevant):
                    blank_streak += 1
                    if blank_streak >= 3:
                        break
                    data_row_index += 1
                    continue

                blank_streak = 0
                if code:
                    row_id = f"{header_row_index}:{data_row_index}:{start_col_index}"
                    if row_id not in seen_ids:
                        normalized_code = code.strip().upper()
                        quantity_places = _sheet_int(cell(data_row_index, start_col_index + 1))
                        volume_cbm = _sheet_float(cell(data_row_index, start_col_index + 2))
                        weight_kg = _sheet_float(cell(data_row_index, start_col_index + 3))
                        quantity_piece = str(quantity_places) if quantity_places else ""
                        if sheet_date:
                            aggregate_key = (sheet_date, normalized_code)
                            existing = aggregated_rows.get(aggregate_key)
                            if existing:
                                existing["quantity_places"] += quantity_places
                                existing["volume_cbm"] += volume_cbm
                                existing["weight_kg"] += weight_kg
                                if quantity_piece:
                                    existing["quantity_places_items"].append(quantity_piece)
                                existing["source_rows"].append(data_row_index + 1)
                                existing["merged_count"] += 1
                            else:
                                aggregated_rows[aggregate_key] = {
                                    "id": f"{sheet_date}:{normalized_code}",
                                    "sheet_date": sheet_date,
                                    "code": normalized_code,
                                    "quantity_places": quantity_places,
                                    "quantity_places_items": [quantity_piece] if quantity_piece else [],
                                    "volume_cbm": volume_cbm,
                                    "weight_kg": weight_kg,
                                    "source_row": data_row_index + 1,
                                    "source_rows": [data_row_index + 1],
                                    "merged_count": 1,
                                }
                        else:
                            parsed_rows.append(
                                {
                                    "id": row_id,
                                    "sheet_date": sheet_date,
                                    "code": normalized_code,
                                    "quantity_places": quantity_places,
                                    "quantity_places_items": [quantity_piece] if quantity_piece else [],
                                    "volume_cbm": volume_cbm,
                                    "weight_kg": weight_kg,
                                    "source_row": data_row_index + 1,
                                    "source_rows": [data_row_index + 1],
                                    "merged_count": 1,
                                }
                            )
                        seen_ids.add(row_id)
                data_row_index += 1

    parsed_rows.extend(aggregated_rows.values())
    for item in parsed_rows:
        item["quantity_places_display"] = " + ".join(
            [part for part in (item.get("quantity_places_items") or []) if str(part).strip()]
        )
    parsed_rows.sort(
        key=lambda item: (
            item.get("sheet_date") or "",
            item.get("code") or "",
            min(item.get("source_rows") or [item.get("source_row") or 0]),
        )
    )
    return parsed_rows


def normalize_message_language(language: str | None) -> str:
    language = (language or "").strip().lower()
    if language in TRACK_BUTTON_LABELS:
        return language
    return getattr(db, "DEFAULT_MESSAGE_LANGUAGE", "uz_latn")


def get_track_button_text(*, chat_id=None, language: str | None = None) -> str:
    normalized_language = normalize_message_language(language)
    if chat_id is not None and language is None:
        bl = db.find_latest_active_bl_by_chat(chat_id) or db.find_latest_bl_by_chat(chat_id)
        if bl:
            normalized_language = normalize_message_language(bl.get("message_language"))
    return TRACK_BUTTON_LABELS.get(normalized_language, TRACK_BUTTON)


def get_chat_message_language(chat_id) -> str:
    bl = db.find_latest_active_bl_by_chat(chat_id) or db.find_latest_bl_by_chat(chat_id)
    if bl:
        return normalize_message_language(bl.get("message_language"))
    return normalize_message_language(None)


def get_track_button_cooldown_text(language: str | None, seconds: int) -> str:
    normalized_language = normalize_message_language(language)
    template = TRACK_BUTTON_COOLDOWN_MESSAGES.get(
        normalized_language,
        TRACK_BUTTON_COOLDOWN_MESSAGES["uz_latn"],
    )
    return template.format(seconds=max(1, int(seconds)))


def _send_with_retry(send_func, *args, retries=3, delay=1.5, **kwargs):
    last_error = None
    for attempt in range(max(1, int(retries))):
        try:
            return send_func(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            if attempt < retries - 1:
                time.sleep(delay)
    if last_error:
        raise last_error
    return None


def _extract_telegram_file_id(payload: dict | None, media_key: str) -> str:
    if not isinstance(payload, dict):
        return ""
    result = payload.get("result") or {}
    media = result.get(media_key) or {}
    if isinstance(media, dict):
        return str(media.get("file_id") or "").strip()
    return ""


def _get_cached_welcome_media_file_id(kind: str) -> str:
    with WELCOME_MEDIA_FILE_IDS_LOCK:
        return WELCOME_MEDIA_FILE_IDS.get(kind, "")


def _set_cached_welcome_media_file_id(kind: str, file_id: str) -> None:
    normalized = str(file_id or "").strip()
    if not normalized:
        return
    with WELCOME_MEDIA_FILE_IDS_LOCK:
        WELCOME_MEDIA_FILE_IDS[kind] = normalized


def telegram_send_video_by_file_id(chat_id, file_id: str):
    payload = {
        "chat_id": chat_id,
        "video": file_id,
        "supports_streaming": True,
    }
    return telegram_api("sendVideo", json=payload, timeout=60)


def telegram_send_voice_by_file_id(chat_id, file_id: str):
    payload = {
        "chat_id": chat_id,
        "voice": file_id,
    }
    return telegram_api("sendVoice", json=payload, timeout=60)


def _send_welcome_video(chat_id):
    cached_file_id = _get_cached_welcome_media_file_id("video")
    if cached_file_id:
        try:
            return _send_with_retry(
                telegram_send_video_by_file_id,
                chat_id,
                cached_file_id,
                retries=2,
                delay=1,
            )
        except Exception:
            with WELCOME_MEDIA_FILE_IDS_LOCK:
                if WELCOME_MEDIA_FILE_IDS.get("video") == cached_file_id:
                    WELCOME_MEDIA_FILE_IDS["video"] = ""

    payload = _send_with_retry(
        telegram_send_video,
        chat_id,
        WELCOME_VIDEO_PATH,
        "Buraq Logistics guide.mp4",
        retries=3,
        delay=2,
    )
    _set_cached_welcome_media_file_id("video", _extract_telegram_file_id(payload, "video"))
    return payload


def _send_welcome_voice(chat_id):
    cached_file_id = _get_cached_welcome_media_file_id("voice")
    if cached_file_id:
        try:
            return _send_with_retry(
                telegram_send_voice_by_file_id,
                chat_id,
                cached_file_id,
                retries=2,
                delay=1,
            )
        except Exception:
            with WELCOME_MEDIA_FILE_IDS_LOCK:
                if WELCOME_MEDIA_FILE_IDS.get("voice") == cached_file_id:
                    WELCOME_MEDIA_FILE_IDS["voice"] = ""

    payload = _send_with_retry(
        telegram_send_voice,
        chat_id,
        WELCOME_VOICE_PATH,
        "Buraq Logistics instruktsiya.ogg",
        retries=2,
        delay=1,
    )
    _set_cached_welcome_media_file_id("voice", _extract_telegram_file_id(payload, "voice"))
    return payload


def _send_welcome_media(chat_id):
    try:
        with WELCOME_MEDIA_SEMAPHORE:
            if os.path.exists(WELCOME_VIDEO_PATH):
                try:
                    _send_welcome_video(chat_id)
                except Exception:
                    pass
            if os.path.exists(WELCOME_VOICE_PATH):
                try:
                    _send_welcome_voice(chat_id)
                except Exception:
                    pass
    finally:
        with WELCOME_MEDIA_LOCK:
            WELCOME_MEDIA_PENDING.discard(str(chat_id))


def enqueue_welcome_media(chat_id):
    chat_key = str(chat_id)
    with WELCOME_MEDIA_LOCK:
        if chat_key in WELCOME_MEDIA_PENDING:
            return
        WELCOME_MEDIA_PENDING.add(chat_key)
    threading.Thread(
        target=_send_welcome_media,
        args=(chat_id,),
        name=f"welcome-media-{chat_key}",
        daemon=True,
    ).start()


def is_group_chat_id(chat_id) -> bool:
    try:
        return int(chat_id) < 0
    except Exception:
        return False


def build_main_reply_markup(*, chat_id=None, language: str | None = None) -> dict:
    return {
        "keyboard": [[{"text": get_track_button_text(chat_id=chat_id, language=language)}]],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True,
    }


def build_group_track_reply_markup(*, chat_id=None, language: str | None = None) -> dict:
    return {
        "keyboard": [[{"text": get_track_button_text(chat_id=chat_id, language=language)}]],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True,
    }


def get_group_welcome_text(button_text: str | None = None) -> str:
    button_text = (button_text or TRACK_BUTTON).upper()
    return (
        "👋Assalomu alaykum hurmatli mijoz! \n\n"
        "🤖Ushbu bot yuklaringiz bo‘yicha ma’lumotlarni tez va qulay tarzda olish uchun yaratilgan.\n\n"
        f"✅MENYUDA paydo bo'lgan \"{button_text}\" tugmasini bosish orqali siz ushbu platformada quyidagi imkoniyatlardan foydalanasiz:\n\n"
        "• yuk statusini kuzatasiz\n"
        "• yetkazib berish jarayonini nazorat qilasiz\n"
        "• yangilanishlarni olasiz\n"
        "• menejer bilan bog‘lanasiz\n\n"
        "🎥 Botdan foydalanish bo‘yicha qisqa videoqo‘llanma quyida taqdim etilgan.\n\n"
        "Bir marta ko‘rib chiqish tavsiya etiladi 👇\n\n"
        "━━━━━━━━━━━━━━━\n\n"
        "👋Здравствуйте, уважаемый клиент!\n\n"
        "🤖Этот бот создан для того, чтобы вы могли быстро и удобно получать информацию по вашим грузам.\n\n"
        f"✅Нажав на появившуюся в МЕНЮ кнопку \"{button_text}\", вы сможете:\n\n"
        "• отслеживать статус груза\n"
        "• контролировать процесс доставки\n"
        "• получать обновления\n"
        "• связываться с менеджером\n\n"
        "🎥 Ниже представлена короткая видеоинструкция по использованию бота.\n\n"
        "Рекомендуем посмотреть её один раз 👇"
    )


def get_no_active_cargo_text(language: str | None = None) -> str:
    normalized_language = normalize_message_language(language)
    return NO_ACTIVE_CARGO_MESSAGES.get(normalized_language, NO_ACTIVE_CARGO_MESSAGES["uz_latn"])


def get_menu_restore_text(language: str | None = None) -> str:
    normalized_language = normalize_message_language(language)
    if normalized_language == "uz_cyrl":
        return "✅ Юқоридаги меню қайта ёқилди. Пастдаги <b>Юк ҳолати</b> тугмасидан фойдаланинг."
    if normalized_language == "ru":
        return "✅ Меню снова включено. Используйте нижнюю кнопку <b>Статус груза</b>."
    return "✅ Menu qayta yoqildi. Pastdagi <b>Yuk holati</b> tugmasidan foydalaning."


def get_chat_admin_ids(chat_id):
    cache_key = str(chat_id)
    cached = CHAT_ADMIN_CACHE.get(cache_key)
    now = time.time()
    if cached and now - cached.get("loaded_at", 0) < CHAT_ADMIN_CACHE_TTL:
        return cached.get("admin_ids", set())

    admin_ids = set()
    try:
        payload = telegram_api("getChatAdministrators", json={"chat_id": chat_id})
        for member in (payload or {}).get("result", []):
            user = member.get("user") or {}
            user_id = user.get("id")
            if user_id is not None:
                admin_id = str(user_id)
                admin_ids.add(admin_id)
                db.remember_chat_member(
                    chat_id,
                    admin_id,
                    telegram_user_name(user),
                    user.get("username") or "",
                    is_admin=True,
                )
    except Exception:
        if cached:
            return cached.get("admin_ids", set())
    CHAT_ADMIN_CACHE[cache_key] = {"loaded_at": now, "admin_ids": admin_ids}
    return admin_ids


def extract_telegram_message_text(message: dict) -> str:
    text = (message.get("text") or message.get("caption") or "").strip()
    if text:
        return text
    if message.get("photo"):
        return "[photo]"
    if message.get("video"):
        return "[video]"
    if message.get("voice"):
        return "[voice]"
    if message.get("audio"):
        return "[audio]"
    if message.get("sticker"):
        return "[sticker]"
    document = message.get("document") or {}
    if document:
        filename = (document.get("file_name") or "").strip()
        return f"[document] {filename}".strip()
    return ""


def telegram_user_name(user: dict) -> str:
    if not user:
        return ""
    full_name = " ".join(
        part for part in [user.get("first_name") or "", user.get("last_name") or ""] if part
    ).strip()
    return full_name or ""


def telegram_unix_to_local(value) -> str:
    try:
        return datetime.fromtimestamp(int(value), db.TASHKENT_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return db.current_ts()


def get_responsible_response_role(assignments: dict | None, sender_id: str, admin_ids: set[str] | None = None) -> str:
    sender_value = str(sender_id or "").strip()
    if not sender_value:
        return ""
    moderator_id = str((assignments or {}).get("moderator_tg_id") or "").strip()
    sales_manager_id = str((assignments or {}).get("sales_manager_tg_id") or "").strip()
    if moderator_id and sender_value == moderator_id:
        return "moderator"
    if sales_manager_id and sender_value == sales_manager_id:
        return "sales_manager"
    if not moderator_id and not sales_manager_id and admin_ids and sender_value in admin_ids:
        return "moderator"
    return ""


def track_moderator_response_metrics(message: dict):
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    chat_type = chat.get("type")
    if chat_type not in {"group", "supergroup"} or not chat_id:
        return

    sender = message.get("from") or {}
    sender_id = sender.get("id")
    if sender.get("is_bot") or sender_id is None:
        return

    text = extract_telegram_message_text(message)
    if not text:
        return

    admin_ids = get_chat_admin_ids(chat_id)
    sender_id_str = str(sender_id)
    is_admin_sender = sender_id_str in admin_ids if admin_ids else False
    db.remember_chat_member(
        chat_id,
        sender_id_str,
        telegram_user_name(sender),
        sender.get("username") or "",
        is_admin=is_admin_sender,
    )
    reply_to = message.get("reply_to_message") or {}
    reply_sender = reply_to.get("from") or {}
    reply_sender_id = reply_sender.get("id")
    reply_sender_id_str = str(reply_sender_id) if reply_sender_id is not None else ""
    if reply_sender_id_str and not reply_sender.get("is_bot"):
        db.remember_chat_member(
            chat_id,
            reply_sender_id_str,
            telegram_user_name(reply_sender),
            reply_sender.get("username") or "",
            is_admin=reply_sender_id_str in admin_ids if admin_ids else False,
        )

    linked_bl = db.find_latest_active_bl_by_chat(chat_id) or db.find_latest_bl_by_chat(chat_id)
    chat_assignments = db.get_chat_response_assignments(chat_id) or {}
    linked_context = {
        "bl_id": linked_bl.get("id") if linked_bl else None,
        "batch_id": linked_bl.get("batch_id") if linked_bl else None,
        "batch_name": linked_bl.get("batch_name") if linked_bl else "",
        "assigned_moderator_id": (chat_assignments.get("moderator_tg_id") or (linked_bl.get("moderator_tg_id") if linked_bl else "") or ""),
        "assigned_sales_manager_id": (chat_assignments.get("sales_manager_tg_id") or (linked_bl.get("sales_manager_tg_id") if linked_bl else "") or ""),
    }

    normalized_text = text.strip()
    if normalized_text in TRACK_BUTTON_TEXTS or normalized_text == CANCEL_BUTTON:
        return
    if normalized_text.startswith("/start") or normalized_text.startswith("/chatid"):
        return
    if re.match(r"^/([A-Za-z0-9_]+)(?:@\w+)?$", normalized_text):
        return

    if reply_to and reply_sender_id_str and reply_sender_id_str != sender_id_str and not reply_sender.get("is_bot"):
        db.record_moderator_request(
            chat_id=chat_id,
            chat_title=chat.get("title") or "",
            request_message_id=reply_to.get("message_id"),
            request_user_id=reply_sender_id_str,
            request_user_name=telegram_user_name(reply_sender),
            request_username=reply_sender.get("username") or "",
            request_text=extract_telegram_message_text(reply_to),
            **linked_context,
            requested_at=telegram_unix_to_local(reply_to.get("date")),
        )

        response_role = get_responsible_response_role(
            {
                "moderator_tg_id": linked_context["assigned_moderator_id"],
                "sales_manager_tg_id": linked_context["assigned_sales_manager_id"],
            },
            sender_id_str,
            admin_ids,
        )
        if response_role:
            db.mark_moderator_response(
                chat_id=chat_id,
                request_message_id=reply_to.get("message_id"),
                responder_user_id=sender_id_str,
                responder_name=telegram_user_name(sender),
                responder_username=sender.get("username") or "",
                response_text=normalized_text,
                responded_at=telegram_unix_to_local(message.get("date")),
                response_role=response_role,
            )
        return

    if is_admin_sender or get_responsible_response_role(
        {
            "moderator_tg_id": linked_context["assigned_moderator_id"],
            "sales_manager_tg_id": linked_context["assigned_sales_manager_id"],
        },
        sender_id_str,
        admin_ids,
    ):
        return

    db.record_moderator_request(
        chat_id=chat_id,
        chat_title=chat.get("title") or "",
        request_message_id=message.get("message_id"),
        request_user_id=sender_id_str,
        request_user_name=telegram_user_name(sender),
        request_username=sender.get("username") or "",
        request_text=normalized_text,
        **linked_context,
        requested_at=telegram_unix_to_local(message.get("date")),
    )


def login_required(func):
    @wraps(func)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return func(*args, **kwargs)

    return decorated


def editor_required(func):
    @wraps(func)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            if request.path.startswith("/api/"):
                return jsonify({"error": "Login required"}), 401
            return redirect(url_for("login"))
        if session.get("role") != ROLE_EDITOR:
            if request.path.startswith("/api/"):
                return jsonify({"error": "View-only access"}), 403
            return redirect(url_for("index"))
        return func(*args, **kwargs)

    return decorated


def get_auth_users():
    guest_logins = [
        os.getenv("GUEST1_LOGIN", "Guest1"),
        os.getenv("GUEST2_LOGIN", "Guest2"),
        os.getenv("GUEST3_LOGIN", "Guest3"),
    ]
    users = {}

    def add_user(username: str, password: str, role: str):
        username = (username or "").strip()
        password = (password or "").strip()
        if username and password:
            users[username] = {"password": password, "role": role}

    add_user(ADMIN_LOGIN, ADMIN_PASSWORD, ROLE_EDITOR)
    add_user(ADMIN1_LOGIN, ADMIN1_PASSWORD, ROLE_EDITOR)
    for guest_login in guest_logins:
        add_user(guest_login, GUEST_PASSWORD, ROLE_VIEWER)

    return users


def get_role_label(role: str) -> str:
    return "Editor" if role == ROLE_EDITOR else "View only"


def get_request_ip() -> str:
    forwarded = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    return (
        forwarded
        or (request.headers.get("X-Real-IP") or "").strip()
        or (request.remote_addr or "").strip()
    )


@app.context_processor
def inject_auth_context():
    role = session.get("role", ROLE_VIEWER)
    return {
        "is_editor": role == ROLE_EDITOR,
        "current_role": role,
        "current_role_label": get_role_label(role) if session.get("logged_in") else "",
    }


def telegram_api(method: str, *, timeout: int = 15, **kwargs):
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not configured")
    response = req.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
        timeout=timeout,
        **kwargs,
    )
    response.raise_for_status()
    return response.json()


def telegram_send_message(chat_id, text: str, reply_markup: dict | None = None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return telegram_api("sendMessage", json=payload)


def telegram_send_document(chat_id, file_path: str, filename: str):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    safe_filename = filename or os.path.basename(file_path)
    mime_type = mimetypes.guess_type(safe_filename)[0] or "application/octet-stream"
    with open(file_path, "rb") as file_handle:
        response = req.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
            data={
                "chat_id": chat_id,
                "disable_content_type_detection": "true",
            },
            files={"document": (safe_filename, file_handle, mime_type)},
            timeout=30,
        )
    if not response.ok:
        try:
            payload = response.json()
            description = payload.get("description") or response.text
        except ValueError:
            description = response.text
        raise RuntimeError(description)
    return response.json()


def telegram_send_video(chat_id, file_path: str, filename: str | None = None):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Видео не найдено: {file_path}")

    safe_filename = filename or os.path.basename(file_path)
    with open(file_path, "rb") as file_handle:
        response = req.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo",
            data={
                "chat_id": chat_id,
                "supports_streaming": "true",
            },
            files={"video": (safe_filename, file_handle, "video/mp4")},
            timeout=60,
        )
    if not response.ok:
        try:
            payload = response.json()
            description = payload.get("description") or response.text
        except ValueError:
            description = response.text
        raise RuntimeError(description)
    return response.json()


def telegram_send_voice(chat_id, file_path: str, filename: str | None = None):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Озвучка не найдена: {file_path}")

    safe_filename = filename or os.path.basename(file_path)
    with open(file_path, "rb") as file_handle:
        response = req.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendVoice",
            data={
                "chat_id": chat_id,
            },
            files={"voice": (safe_filename, file_handle, "audio/ogg")},
            timeout=60,
        )
    if not response.ok:
        try:
            payload = response.json()
            description = payload.get("description") or response.text
        except ValueError:
            description = response.text
        raise RuntimeError(description)
    return response.json()


def telegram_answer_callback_query(callback_query_id, text: str):
    return telegram_api(
        "answerCallbackQuery",
        json={
            "callback_query_id": callback_query_id,
            "text": text,
            "show_alert": False,
        },
    )


def telegram_delete_message(chat_id, message_id):
    return telegram_api(
        "deleteMessage",
        json={
            "chat_id": chat_id,
            "message_id": message_id,
        },
    )


def telegram_leave_chat(chat_id):
    return telegram_api(
        "leaveChat",
        json={
            "chat_id": chat_id,
        },
    )


def communication_rating_markup(dispatch_id: int):
    options = list(range(0, 11))
    return {
        "inline_keyboard": [
            [
                {
                    "text": str(score),
                    "callback_data": f"{COMM_RATE_PREFIX}:{dispatch_id}:{score}",
                }
                for score in options[:6]
            ],
            [
                {
                    "text": str(score),
                    "callback_data": f"{COMM_RATE_PREFIX}:{dispatch_id}:{score}",
                }
                for score in options[6:]
            ],
        ]
    }


def bl_file_markup(bl_id: int):
    files = db.get_files(bl_id)
    buttons = []
    row = []
    for file_info in files:
        token = (file_info.get("public_token") or "").strip()
        label = db.prettify_file_name(file_info.get("filename") or "")
        if not token or not label:
            continue
        short_label = label if len(label) <= 28 else f"{label[:25]}..."
        row.append({"text": f"📄 {short_label}", "callback_data": f"{FILE_PREFIX}:{token}"})
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return {"inline_keyboard": buttons} if buttons else None


def clear_group_reply_keyboard(chat_id):
    try:
        telegram_send_message(chat_id, "ㅤ", reply_markup=REMOVE_REPLY_MARKUP)
    except Exception:
        pass


def _delete_message_later(chat_id, message_id, delay: float = 1.5):
    try:
        time.sleep(max(0.2, float(delay)))
        telegram_delete_message(chat_id, message_id)
    except Exception:
        pass


def extract_bot_command(text: str) -> str:
    match = re.match(r"^/([A-Za-z0-9_]+)(?:@\w+)?$", (text or "").strip())
    return (match.group(1) or "").lower() if match else ""


def handle_group_remove_request(message: dict, command: str):
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    chat_type = chat.get("type")
    sender = message.get("from") or {}
    sender_id = sender.get("id")
    if chat_type not in {"group", "supergroup"} or not chat_id:
        return False
    if command not in GROUP_REMOVE_COMMANDS:
        return False

    admin_ids = get_chat_admin_ids(chat_id)
    if str(sender_id or "") not in admin_ids:
        telegram_send_message(
            chat_id,
            "❌ Bu buyruqdan faqat guruh admini foydalanishi mumkin.",
        )
        return True

    telegram_send_message(
        chat_id,
        "✅ Klaviatura yopilmoqda. Bot guruhni tark etadi...",
        reply_markup=REMOVE_REPLY_MARKUP,
    )
    time.sleep(1)
    try:
        telegram_leave_chat(chat_id)
    except Exception:
        pass
    return True


def refresh_track_reply_keyboard(chat_id, *, language: str | None = None):
    try:
        response = telegram_send_message(
            chat_id,
            "ㅤ",
            reply_markup=(
                build_group_track_reply_markup(chat_id=chat_id, language=language)
                if is_group_chat_id(chat_id)
                else build_main_reply_markup(chat_id=chat_id, language=language)
            ),
        )
        message_id = (((response or {}).get("result") or {}).get("message_id"))
        if message_id:
            threading.Thread(
                target=_delete_message_later,
                args=(chat_id, message_id, 1.5),
                name=f"delete-keyboard-refresh-{chat_id}",
                daemon=True,
            ).start()
    except Exception:
        pass


def send_group_message_with_keyboard(chat_id, text: str, *, language: str | None = None):
    telegram_send_message(chat_id, text)
    refresh_track_reply_keyboard(chat_id, language=language)


def send_group_welcome_bundle(chat_id, button_text: str | None = None):
    send_group_message_with_keyboard(chat_id, get_group_welcome_text(button_text))
    enqueue_welcome_media(chat_id)


def send_with_track_keyboard(chat_id, text: str, *, language: str | None = None, reply_markup: dict | None = None):
    if is_group_chat_id(chat_id):
        telegram_send_message(chat_id, text, reply_markup=reply_markup)
        refresh_track_reply_keyboard(chat_id, language=language)
        return
    telegram_send_message(
        chat_id,
        text,
        reply_markup=reply_markup or build_main_reply_markup(chat_id=chat_id, language=language),
    )


def communication_rating_label(score: int) -> str:
    return f"{int(score)}/10"


def send_communication_survey(recipient: dict, month_key: str):
    dispatch_id = db.record_communication_survey_send(month_key, recipient)
    text = db.render_communication_rate_message(recipient, month_key)
    try:
        telegram_send_message(
            recipient["chat_id"],
            text,
            reply_markup=communication_rating_markup(dispatch_id),
        )
    except Exception:
        db.delete_communication_survey_dispatch(dispatch_id)
        raise


def handle_callback_query(callback_query: dict):
    callback_id = callback_query.get("id")
    data = (callback_query.get("data") or "").strip()
    voter = callback_query.get("from") or {}
    message = callback_query.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")

    if not callback_id or not data or not chat_id:
        return

    if data.startswith(f"{FILE_PREFIX}:"):
        token = data.split(":", 1)[1].strip()
        file_info = db.get_file_by_public_token(token)
        if not file_info:
            telegram_answer_callback_query(callback_id, "Файл не найден")
            return
        try:
            telegram_send_document(chat_id, file_info["file_path"], file_info["filename"])
            telegram_answer_callback_query(callback_id, "Файл отправлен")
        except Exception as exc:
            telegram_answer_callback_query(callback_id, f"Ошибка: {str(exc)[:120]}")
        return

    if not data.startswith(f"{COMM_RATE_PREFIX}:"):
        telegram_answer_callback_query(callback_id, "Неизвестное действие")
        return

    parts = data.split(":")
    if len(parts) != 3:
        telegram_answer_callback_query(callback_id, "Неверный формат оценки")
        return

    _, dispatch_or_month, score_raw = parts
    try:
        score = int(score_raw)
    except ValueError:
        telegram_answer_callback_query(callback_id, "Оценка не распознана")
        return

    dispatch_id = None
    month_key = dispatch_or_month
    try:
        dispatch_id = int(dispatch_or_month)
        month_key = ""
    except ValueError:
        pass

    if not db.save_communication_rating(dispatch_id, month_key, chat_id, score, voter=voter):
        telegram_answer_callback_query(callback_id, "Не удалось сохранить оценку")
        return

    telegram_answer_callback_query(
        callback_id,
        f"Спасибо! Оценка {communication_rating_label(score)} сохранена",
    )

    if message_id:
        try:
            telegram_delete_message(chat_id, message_id)
        except Exception:
            pass
def configure_telegram_webhook():
    if not BOT_TOKEN or not WEBHOOK_BASE_URL:
        return False

    payload = {
        "url": f"{WEBHOOK_BASE_URL.rstrip('/')}/telegram/webhook",
        "drop_pending_updates": True,
    }
    if WEBHOOK_SECRET:
        payload["secret_token"] = WEBHOOK_SECRET

    telegram_api("setWebhook", json=payload)
    return True


def remember_group_chat(chat: dict, is_active: bool = True):
    if not chat:
        return

    chat_type = chat.get("type")
    if chat_type not in {"group", "supergroup"}:
        return

    db.upsert_telegram_chat(
        chat_id=chat.get("id"),
        title=chat.get("title") or f"Group {chat.get('id')}",
        chat_type=chat_type,
        username=chat.get("username") or "",
        is_active=is_active,
    )


def send_bl_status(chat_id, bl: dict):
    text = db.render_message(bl, bl["batch_name"])
    batch = db.get_batch(bl.get("batch_id")) if bl.get("batch_id") else None
    show_packing_list = not db.is_customer_delivery_eta((batch or {}).get("eta_destination") or "")
    language = normalize_message_language(bl.get("message_language"))
    reply_markup = bl_file_markup(bl["id"]) if show_packing_list else None
    send_with_track_keyboard(chat_id, text, language=language, reply_markup=reply_markup)


def send_requested_file(chat_id, file_info: dict | None):
    if not file_info:
        telegram_send_message(chat_id, "❌ Fayl topilmadi.")
        return
    try:
        telegram_send_document(chat_id, file_info["file_path"], file_info["filename"])
    except Exception as exc:
        telegram_send_message(chat_id, f"❌ Fayl yuborilmadi: {html.escape(str(exc))}")


def handle_bl_lookup(chat_id, raw_code: str):
    code = raw_code.strip().upper()
    if not code:
        telegram_send_message(
            chat_id,
            "Введи <b>BL-код</b> текстом.\nНапример: <code>BL171</code>",
            reply_markup=CANCEL_REPLY_MARKUP,
        )
        return

    bl = db.find_bl_by_code(code)
    if not bl:
        telegram_send_message(
            chat_id,
            f"❌ BL-код <b>{code}</b> не найден.\n\nПроверь код и отправь его ещё раз.",
            reply_markup=CANCEL_REPLY_MARKUP,
        )
        return

    db.clear_chat_state(chat_id)
    send_bl_status(chat_id, bl)


def handle_telegram_message(message: dict):
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    chat_type = chat.get("type")
    sender = message.get("from") or {}
    sender_id = sender.get("id") or chat_id
    text = (message.get("text") or "").strip()

    remember_group_chat(chat, is_active=True)

    if not chat_id or not text:
        return

    bot_command = extract_bot_command(text)
    if handle_group_remove_request(message, bot_command):
        return

    if bot_command in MENU_RESTORE_COMMANDS:
        db.clear_chat_state(chat_id)
        language = get_chat_message_language(chat_id)
        if chat_type in {"group", "supergroup"}:
            send_group_message_with_keyboard(chat_id, get_menu_restore_text(language), language=language)
        else:
            send_with_track_keyboard(chat_id, get_menu_restore_text(language), language=language)
        return

    if text == "/start":
        db.clear_chat_state(chat_id)
        button_text = get_track_button_text(chat_id=chat_id)
        if chat_type in {"group", "supergroup"}:
            send_group_welcome_bundle(chat_id, button_text)
        else:
            send_with_track_keyboard(chat_id, "Привет!\n\nНажми кнопку ниже, чтобы узнать текущий статус своего груза.")
        return

    if text == "/chatid":
        title = chat.get("title") or "Личный чат"
        telegram_send_message(
            chat_id,
            f"📍 Чат: <b>{title}</b>\n🆔 ID: <code>{chat_id}</code>",
        )
        return

    file_match = re.match(r"^/([A-Za-z0-9_]+)(?:@\w+)?$", text)
    if file_match:
        file_info = db.get_file_by_command_alias(file_match.group(1))
        if file_info:
            send_requested_file(chat_id, file_info)
            return

    if text in TRACK_BUTTON_TEXTS:
        language = get_chat_message_language(chat_id)
        remaining = db.reserve_track_button_request(
            chat_id,
            sender_id,
            cooldown_seconds=TRACK_BUTTON_COOLDOWN_SECONDS,
        )
        if remaining:
            send_with_track_keyboard(
                chat_id,
                get_track_button_cooldown_text(language, remaining),
                language=language,
            )
            return

        latest_active_bl = db.find_latest_active_bl_by_chat(chat_id)
        if latest_active_bl:
            db.clear_chat_state(chat_id)
            send_bl_status(chat_id, latest_active_bl)
            return
        latest_bl = db.find_latest_bl_by_chat(chat_id)
        if latest_bl:
            db.clear_chat_state(chat_id)
            send_with_track_keyboard(
                chat_id,
                get_no_active_cargo_text(latest_bl.get("message_language")),
                language=latest_bl.get("message_language"),
            )
            return
        db.clear_chat_state(chat_id)
        send_with_track_keyboard(
            chat_id,
            get_no_active_cargo_text(language),
            language=language,
        )
        return

    if text == CANCEL_BUTTON or db.get_chat_state(chat_id) == STATE_WAITING_BL:
        db.clear_chat_state(chat_id)
        send_with_track_keyboard(
            chat_id,
            get_no_active_cargo_text(get_chat_message_language(chat_id)),
            language=get_chat_message_language(chat_id),
        )
        return


def handle_my_chat_member_update(chat_update: dict):
    chat = chat_update.get("chat") or {}
    chat_id = chat.get("id")
    chat_type = chat.get("type")
    if chat_type not in {"group", "supergroup"}:
        return

    old_status = ((chat_update.get("old_chat_member") or {}).get("status") or "").lower()
    new_status = ((chat_update.get("new_chat_member") or {}).get("status") or "").lower()
    is_active = new_status not in {"left", "kicked"}
    remember_group_chat(chat, is_active=is_active)
    if not chat_id:
        return
    if not is_active:
        clear_group_reply_keyboard(chat_id)
        return
    if new_status in {"member", "administrator"} and old_status in {"", "left", "kicked"}:
        return


def send_bl_package(bl: dict, batch_name: str):
    if not bl["chat_id"]:
        return False, "Нет chat_id"

    try:
        language = normalize_message_language(bl.get("message_language"))
        reply_markup = bl_file_markup(bl["id"])
        send_with_track_keyboard(
            bl["chat_id"],
            db.render_message(bl, batch_name),
            language=language,
            reply_markup=reply_markup,
        )
        db.record_tracking_delivery(bl)
    except Exception as exc:
        return False, str(exc)

    return True, ""


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        auth_user = get_auth_users().get(username)
        role = auth_user["role"] if auth_user else ""
        if auth_user and password == auth_user["password"]:
            session.clear()
            session["logged_in"] = True
            session["username"] = username
            session["role"] = role
            db.record_login_history(
                username=username,
                role=role,
                success=True,
                ip_address=get_request_ip(),
                user_agent=request.headers.get("User-Agent", ""),
            )
            return redirect(url_for("index"))
        db.record_login_history(
            username=username,
            role=role,
            success=False,
            ip_address=get_request_ip(),
            user_agent=request.headers.get("User-Agent", ""),
        )
        error = "Invalid login or password"
    return render_template("index.html", login_page=True, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    return render_template(
        "index.html",
        initial_view=(request.args.get("view") or "dashboard").strip() or "dashboard",
        initial_analytics_tab=(request.args.get("tab") or "overview").strip() or "overview",
    )


def _render_analytics_page(tab: str):
    return render_template("index.html", initial_view="analytics", initial_analytics_tab=tab)


@app.route("/analytics")
@login_required
def analytics_index():
    return _render_analytics_page("overview")


@app.route("/analytics/sales-growth")
@login_required
def analytics_sales_growth_page():
    return _render_analytics_page("sales-growth")


@app.route("/analytics/cashflow")
@login_required
def analytics_cashflow_page():
    return _render_analytics_page("cashflow")


@app.route("/analytics/managers")
@login_required
def analytics_managers_page():
    return _render_analytics_page("managers")


@app.route("/analytics/logists")
@login_required
def analytics_logists_page():
    return _render_analytics_page("logists")


@app.route("/analytics/shipments")
@login_required
def analytics_shipments_page():
    return _render_analytics_page("shipments")


@app.route("/analytics/debts")
@login_required
def analytics_debts_page():
    return _render_analytics_page("debts")


@app.route("/analytics/export")
@login_required
def analytics_export_page():
    return _render_analytics_page("export")


@app.route("/analytics/sync")
@login_required
def analytics_sync_page():
    return _render_analytics_page("sync")


@app.route("/analytics/monitor")
@login_required
def analytics_monitor_page():
    plans = analytics_service.list_sales_plans()
    active_plan = next((plan for plan in plans if int(plan.get("is_active") or 0) == 1), None)
    return render_template(
        "analytics/monitor.html",
        sales_plans=plans,
        active_plan_id=(active_plan or {}).get("id"),
    )


@app.route("/health")
def health():
    return jsonify(
        {
            "ok": True,
            "bot_configured": bool(BOT_TOKEN),
            "webhook_configured": bool(BOT_TOKEN and WEBHOOK_BASE_URL),
        }
    )


@app.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    if WEBHOOK_SECRET:
        incoming_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if not secrets.compare_digest(incoming_secret, WEBHOOK_SECRET):
            return jsonify({"ok": False, "error": "invalid webhook secret"}), 403

    update = request.get_json(silent=True) or {}
    callback_query = update.get("callback_query")
    if callback_query:
        handle_callback_query(callback_query)

    chat_update = update.get("my_chat_member")
    if chat_update:
        handle_my_chat_member_update(chat_update)

    message = update.get("message") or update.get("edited_message")
    if message:
        track_moderator_response_metrics(message)
        handle_telegram_message(message)
    return jsonify({"ok": True})


@app.route("/api/stats")
@login_required
def api_stats():
    return jsonify(db.get_stats())


@app.route("/api/dashboard/history/clear", methods=["POST"])
@editor_required
def api_clear_dashboard_history():
    deleted_count = db.clear_dashboard_history()
    return jsonify({"ok": True, "deleted_count": deleted_count})


@app.route("/api/attention")
@login_required
def api_attention():
    limit = int(request.args.get("limit", 10))
    return jsonify(db.get_attention_items(limit))


@app.route("/api/batches")
@login_required
def api_batches():
    return jsonify(db.get_batches())


@app.route("/api/batches", methods=["POST"])
@editor_required
def api_create_batch():
    data = request.json or {}
    name = (data.get("name") or "").strip()
    status = (data.get("status") or "Xitoy").strip() or "Xitoy"
    eta_to_toshkent = (data.get("eta_to_toshkent") or "").strip()
    eta_destination = (data.get("eta_destination") or "Toshkent").strip() or "Toshkent"
    client_delivery_date = (data.get("client_delivery_date") or "").strip()
    if not name:
        return jsonify({"error": "Имя партии обязательно"}), 400
    if not db.create_batch(name, status, eta_to_toshkent, eta_destination, client_delivery_date):
        return jsonify({"error": "Партия с таким именем уже существует"}), 400
    return jsonify({"ok": True})


@app.route("/api/batches/<int:batch_id>", methods=["PUT"])
@editor_required
def api_update_batch(batch_id):
    data = request.json or {}
    name = (data.get("name") or "").strip()
    status = (data.get("status") or "Xitoy").strip() or "Xitoy"
    client_delivery_date = (data.get("client_delivery_date") or "").strip()
    if not name:
        return jsonify({"error": "Имя партии обязательно"}), 400
    if not db.update_batch(
        batch_id,
        name,
        status,
        (data.get("eta_to_toshkent") or "").strip(),
        (data.get("eta_destination") or "Toshkent").strip() or "Toshkent",
        client_delivery_date,
    ):
        return jsonify({"error": "Партия с таким именем уже существует"}), 400
    return jsonify({"ok": True})


@app.route("/api/batches/<int:batch_id>", methods=["DELETE"])
@editor_required
def api_delete_batch(batch_id):
    db.delete_batch(batch_id)
    return jsonify({"ok": True})


@app.route("/api/batches/<int:batch_id>/bl")
@login_required
def api_bl_list(batch_id):
    batch = db.get_batch(batch_id)
    if not batch:
        abort(404)
    bl_codes = db.get_bl_by_batch(batch_id)
    return jsonify({"batch": batch, "bl_codes": bl_codes, "statuses": db.STATUSES})


@app.route("/api/chats")
@login_required
def api_chats():
    include_inactive = request.args.get("all") == "1"
    return jsonify(db.get_telegram_chats(include_inactive=include_inactive))


@app.route("/api/google-sheets/config")
@login_required
def api_google_sheets_config():
    return jsonify({"url": db.get_setting(GOOGLE_SHEETS_URL_SETTING_KEY, "")})


@app.route("/api/google-sheets/config", methods=["POST"])
@editor_required
def api_save_google_sheets_config():
    data = request.json or {}
    url = (data.get("url") or "").strip()
    db.set_setting(GOOGLE_SHEETS_URL_SETTING_KEY, url)
    return jsonify({"ok": True, "url": url})


@app.route("/api/google-sheets/preview", methods=["POST"])
@login_required
def api_google_sheets_preview():
    data = request.json or {}
    url = (data.get("url") or "").strip() or db.get_setting(GOOGLE_SHEETS_URL_SETTING_KEY, "")
    if not url:
        return jsonify({"error": "Сначала укажи ссылку на Google Sheets"}), 400
    try:
        rows = _parse_google_sheet_rows(url)
    except req.RequestException as exc:
        return jsonify({"error": f"Не удалось прочитать Google Sheets: {exc}"}), 400
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    return jsonify(
        {
            "ok": True,
            "url": url,
            "rows": rows,
            "count": len(rows),
            "sheet_dates": sorted({row.get("sheet_date", "") for row in rows if row.get("sheet_date")}),
        }
    )


@app.route("/api/batches/<int:batch_id>/import-sheet", methods=["POST"])
@editor_required
def api_import_google_sheet_rows(batch_id):
    batch = db.get_batch(batch_id)
    if not batch:
        abort(404)
    data = request.json or {}
    rows = data.get("rows") or []
    if not isinstance(rows, list) or not rows:
        return jsonify({"error": "Не выбраны строки для импорта"}), 400

    imported = []
    skipped = []
    for row in rows:
        code = str((row or {}).get("code") or "").strip()
        if not code:
            continue
        success = db.add_bl(
            batch_id=batch_id,
            code=code,
            client_name="",
            chat_id="",
            cargo_type="",
            weight_kg=(row or {}).get("weight_kg", 0),
            volume_cbm=(row or {}).get("volume_cbm", 0),
            quantity_places=(row or {}).get("quantity_places", 0),
            quantity_places_breakdown=(row or {}).get("quantity_places_display", ""),
            cargo_description="",
            message_language=getattr(db, "DEFAULT_MESSAGE_LANGUAGE", "uz_latn"),
        )
        if success:
            imported.append(code)
        else:
            skipped.append({"code": code, "reason": "duplicate"})

    return jsonify(
        {
            "ok": True,
            "imported_count": len(imported),
            "skipped_count": len(skipped),
            "imported": imported,
            "skipped": skipped,
        }
    )


@app.route("/api/bl", methods=["POST"])
@editor_required
def api_add_bl():
    data = request.json or {}
    batch_id = data.get("batch_id")
    code = (data.get("code") or "").strip()
    client_name = (data.get("client_name") or "").strip()
    chat_id = (data.get("chat_id") or "").strip()
    message_language = (data.get("message_language") or "").strip()
    moderator_tg_id = (data.get("moderator_tg_id") or "").strip()
    sales_manager_tg_id = (data.get("sales_manager_tg_id") or "").strip()
    cargo_type = (data.get("cargo_type") or "").strip()
    weight_kg = data.get("weight_kg", 0)
    volume_cbm = data.get("volume_cbm", 0)
    quantity_places = data.get("quantity_places", 0)
    quantity_places_breakdown = (data.get("quantity_places_breakdown") or data.get("quantity_places") or "").strip()
    cargo_description = (data.get("cargo_description") or "").strip()

    if not batch_id or not code:
        return jsonify({"error": "batch_id и code обязательны"}), 400

    if not db.add_bl(
        batch_id,
        code,
        client_name,
        chat_id,
        moderator_tg_id,
        sales_manager_tg_id,
        cargo_type,
        weight_kg,
        volume_cbm,
        quantity_places,
        quantity_places_breakdown,
        cargo_description,
        message_language,
    ):
        return jsonify({"error": "BL-код уже существует в этой партии"}), 400

    return jsonify({"ok": True})


@app.route("/api/bl/<int:bl_id>", methods=["PUT"])
@editor_required
def api_update_bl(bl_id):
    data = request.json or {}
    try:
        db.update_bl(
            bl_id,
            data.get("code", ""),
            data.get("client_name", ""),
            data.get("chat_id", ""),
            data.get("status"),
            data.get("moderator_tg_id", ""),
            data.get("sales_manager_tg_id", ""),
            data.get("cargo_type", ""),
            data.get("weight_kg", 0),
            data.get("volume_cbm", 0),
            data.get("quantity_places", 0),
            data.get("quantity_places_breakdown", data.get("quantity_places", "")),
            data.get("cargo_description", ""),
            data.get("message_language", ""),
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"ok": True})


@app.route("/api/bl/<int:bl_id>", methods=["DELETE"])
@editor_required
def api_delete_bl(bl_id):
    try:
        db.delete_bl(bl_id)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"ok": True})


@app.route("/api/bl/<int:bl_id>/move", methods=["POST"])
@editor_required
def api_move_bl(bl_id):
    data = request.json or {}
    target_batch_id = data.get("target_batch_id")
    if not target_batch_id:
        return jsonify({"error": "target_batch_id обязателен"}), 400
    try:
        result = db.move_bl_to_batch(bl_id, target_batch_id)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"ok": True, "result": result})


@app.route("/api/bl/<int:bl_id>/files")
@login_required
def api_files(bl_id):
    return jsonify(db.get_files(bl_id))


@app.route("/api/bl/<int:bl_id>/send-exclusion", methods=["POST"])
@editor_required
def api_set_bl_send_exclusion(bl_id):
    data = request.json or {}
    excluded = bool(data.get("excluded"))
    try:
        result = db.set_batch_send_exclusion(bl_id, excluded)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404
    return jsonify({"ok": True, "result": result})


@app.route("/api/bl/<int:bl_id>/files", methods=["POST"])
@editor_required
def api_upload(bl_id):
    if "file" not in request.files:
        return jsonify({"error": "Файл не выбран"}), 400

    uploaded_file = request.files["file"]
    if not uploaded_file.filename:
        return jsonify({"error": "Выбери файл для загрузки"}), 400
    ext = uploaded_file.filename.rsplit(".", 1)[-1].lower() if "." in uploaded_file.filename else ""
    if ext not in ALLOWED_EXT:
        return jsonify({"error": f"Тип файла .{ext} не разрешён"}), 400

    original_filename = (uploaded_file.filename or "").strip()
    storage_name = secure_filename(original_filename) or f"file_{secrets.token_hex(4)}.{ext}"
    filename = original_filename or storage_name
    unique = f"bl{bl_id}_{secrets.token_hex(4)}_{storage_name}"
    file_path = os.path.join(UPLOAD_FOLDER, unique)
    uploaded_file.save(file_path)
    db.add_file(bl_id, filename, file_path)
    return jsonify({"ok": True, "filename": filename})


@app.route("/api/files/<int:file_id>", methods=["DELETE"])
@editor_required
def api_delete_file(file_id):
    db.delete_file(file_id)
    return jsonify({"ok": True})


@app.route("/public/file/<public_token>")
def public_file(public_token):
    file_info = db.get_file_by_public_token(public_token)
    if not file_info:
        abort(404)
    file_path = file_info.get("file_path") or ""
    if not file_path or not os.path.exists(file_path):
        abort(404)
    return send_file(
        file_path,
        as_attachment=False,
        download_name=file_info.get("filename") or os.path.basename(file_path),
        conditional=True,
    )


@app.route("/api/batches/<int:batch_id>/send", methods=["POST"])
@editor_required
def api_send_batch(batch_id):
    if not BOT_TOKEN:
        return jsonify({"error": "BOT_TOKEN не настроен в .env"}), 500

    batch = db.get_batch(batch_id)
    if not batch:
        abort(404)

    data = request.json or {}
    selected_raw = data.get("selected_bl_ids") or []
    selected_ids = set()
    if isinstance(selected_raw, list):
        for item in selected_raw:
            try:
                selected_ids.add(int(item))
            except (TypeError, ValueError):
                continue

    bl_rows = db.get_bl_by_batch(batch_id)
    if selected_ids:
        bl_rows = [bl for bl in bl_rows if int(bl.get("id") or 0) in selected_ids]
    else:
        bl_rows = [
            bl
            for bl in bl_rows
            if bl.get("chat_id") and not bl.get("send_excluded") and not bl.get("tracking_sent_current")
        ]

    if not bl_rows:
        return jsonify({"error": "Нет выбранных BL для отправки"}), 400

    results = []
    sent_chats = set()
    for bl in bl_rows:
        chat_id = str(bl.get("chat_id") or "").strip()
        if not chat_id:
            results.append(
                {
                    "code": bl["code"],
                    "client": bl["client_name"],
                    "success": False,
                    "skipped": True,
                    "error": "Нет chat_id",
                }
            )
            continue
        if chat_id in sent_chats:
            results.append(
                {
                    "code": bl["code"],
                    "client": bl["client_name"],
                    "success": False,
                    "skipped": True,
                    "error": "Уже покрыто сообщением этого клиента в текущей отправке",
                }
            )
            continue
        sent_chats.add(chat_id)
        success, error_msg = send_bl_package(bl, batch["name"])
        db.add_log(
            bl["id"],
            bl["code"],
            batch["name"],
            bl["chat_id"],
            bl["status"],
            success,
            error_msg,
        )
        results.append(
            {
                "code": bl["code"],
                "client": bl["client_name"],
                "success": success,
                "skipped": False,
                "error": error_msg,
            }
        )

    sent = sum(1 for item in results if item["success"])
    skipped = sum(1 for item in results if item.get("skipped"))
    return jsonify({"ok": True, "sent": sent, "skipped": skipped, "total": len(results), "results": results})


@app.route("/api/bl/<int:bl_id>/send", methods=["POST"])
@editor_required
def api_send_one(bl_id):
    if not BOT_TOKEN:
        return jsonify({"error": "BOT_TOKEN не настроен в .env"}), 500

    bl = db.get_bl_by_id(bl_id)
    if not bl:
        abort(404)

    if not bl["chat_id"]:
        return jsonify({"error": "Не указан chat_id"}), 400

    batch = db.get_batch(bl["batch_id"])
    batch_name = batch["name"] if batch else "—"
    success, error_msg = send_bl_package(bl, batch_name)
    db.add_log(bl["id"], bl["code"], batch_name, bl["chat_id"], bl["status"], success, error_msg)

    if not success:
        return jsonify({"error": error_msg}), 500

    return jsonify({"ok": True})


@app.route("/api/logs")
@login_required
def api_logs():
    limit = int(request.args.get("limit", 100))
    return jsonify(db.get_logs(limit))


@app.route("/api/login-history")
@editor_required
def api_login_history():
    limit = int(request.args.get("limit", 200))
    return jsonify(db.get_login_history(limit))


@app.route("/api/problems")
@login_required
def api_problems():
    return jsonify(
        db.get_problems(
            problem_type=(request.args.get("type") or "").strip(),
            date_from=(request.args.get("date_from") or "").strip(),
            date_to=(request.args.get("date_to") or "").strip(),
            batch_id=(request.args.get("batch_id") or "").strip(),
        )
    )


@app.route("/api/problems", methods=["POST"])
@editor_required
def api_create_problem():
    data = request.json or {}
    bl_id = data.get("bl_id")
    problem_type = (data.get("problem_type") or "").strip()
    description = (data.get("description") or "").strip()

    if not bl_id:
        return jsonify({"error": "Не указан BL"}), 400
    if problem_type not in db.PROBLEM_TYPES:
        return jsonify({"error": "Неверный тип проблемы"}), 400
    if not db.create_problem(bl_id, problem_type, description):
        return jsonify({"error": "BL не найден"}), 404

    return jsonify({"ok": True})


@app.route("/api/problems/export")
@login_required
def api_export_problems():
    problem_type = (request.args.get("type") or "").strip()
    date_from = (request.args.get("date_from") or "").strip()
    date_to = (request.args.get("date_to") or "").strip()
    batch_id = (request.args.get("batch_id") or "").strip()
    rows = db.get_problems(
        problem_type=problem_type,
        date_from=date_from,
        date_to=date_to,
        batch_id=batch_id,
    )

    filter_items = []
    if problem_type:
        filter_items.append(("Тип", db.PROBLEM_TYPES.get(problem_type, problem_type)))
    if date_from:
        filter_items.append(("С", date_from))
    if date_to:
        filter_items.append(("По", date_to))
    if batch_id:
        filter_items.append(("Партия", batch_id))

    filters_html = "".join(
        f'<span class="chip"><span class="chip-label">{html.escape(label)}</span>{html.escape(value)}</span>'
        for label, value in filter_items
    ) or '<span class="chip chip-muted">Все инциденты</span>'

    body_rows_list = []
    for row in rows:
        problem_type_key = row.get("problem_type", "") or ""
        problem_label = db.PROBLEM_TYPES.get(problem_type_key, problem_type_key or "—")
        problem_class = {
            "damage": "badge-red",
            "delay": "badge-amber",
            "shortage": "badge-blue",
        }.get(problem_type_key, "badge-muted")
        status_text = row.get("bl_status", "") or "—"
        status_class = {
            "Xitoy": "badge-muted",
            "Yiwu": "badge-muted",
            "Zhongshan": "badge-muted",
            "Horgos (Qozoq)": "badge-blue",
            "Kashgar (Qirg'iz)": "badge-blue",
            "Altynko'l": "badge-amber",
            "Jarkent": "badge-amber",
            "Almata": "badge-blue",
            "Taraz": "badge-amber",
            "Shimkent": "badge-amber",
            "Qonusbay": "badge-amber",
            "Saryagash": "badge-amber",
            "Yallama": "badge-blue",
            "Irkeshtam": "badge-blue",
            "Osh": "badge-amber",
            "Chuqur": "badge-amber",
            "Dostlik": "badge-blue",
            "Andijon": "badge-amber",
            "Toshkent": "badge-amber",
            "Доставлен": "badge-green",
        }.get(status_text, "badge-muted")
        body_rows_list.append(
            f"""
            <tr>
              <td class="mono">{html.escape(row.get('incident_detected_at', '') or row.get('created_at', '') or '-')}</td>
              <td>{html.escape(row.get('batch_name', '') or '-')}</td>
              <td class="mono strong">{html.escape(row.get('bl_code', '') or '-')}</td>
              <td>{html.escape(row.get('client_name', '') or '-')}</td>
              <td><span class="badge {problem_class}">{html.escape(problem_label)}</span></td>
              <td class="desc">{html.escape(row.get('description', '') or '-')}</td>
              <td><span class="badge {status_class}">{html.escape(status_text)}</span></td>
              <td>{html.escape(row.get('expected_date', '') or '-')}</td>
              <td>{html.escape(row.get('actual_date', '') or '-')}</td>
            </tr>
            """
        )
    body_rows = "".join(body_rows_list)
    if not body_rows:
        body_rows = """
        <tr>
          <td colspan="9" class="empty">Инцидентов по выбранным фильтрам не найдено</td>
        </tr>
        """

    report_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>BURAQ logistics - Проблемы</title>
      <style>
        :root {
          --bg: #eef1f6;
          --card: #ffffff;
          --text: #17191f;
          --muted: #667085;
          --line: #d8dde6;
          --accent: #111827;
          --accent-soft: #f5a623;
          --accent-soft-2: #fff3dd;
          --danger: #c7344f;
          --warning: #d97706;
          --info: #2563eb;
          --ok: #0f9f6e;
        }
        * { box-sizing: border-box; }
        body { margin: 0; font-family: Arial, Helvetica, sans-serif; background: linear-gradient(180deg, #f7f8fb 0%, var(--bg) 100%); color: var(--text); }
        .page { max-width: 1400px; margin: 0 auto; padding: 28px; }
        .hero {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 20px;
          margin-bottom: 20px;
          padding: 24px 26px;
          border-radius: 24px;
          background: linear-gradient(135deg, #121722 0%, #1c2433 48%, #141922 100%);
          color: #fff;
          box-shadow: 0 24px 60px rgba(17, 24, 39, .18);
        }
        .brand { font-size: 12px; font-weight: 800; letter-spacing: .28em; text-transform: uppercase; color: #f6c467; margin-bottom: 10px; }
        .title { font-size: 32px; font-weight: 800; letter-spacing: -0.03em; }
        .meta { color: rgba(255,255,255,.72); font-size: 13px; margin-top: 8px; }
        .hero-stats { display: flex; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
        .stat {
          min-width: 138px;
          padding: 14px 16px;
          border-radius: 16px;
          background: rgba(255,255,255,.06);
          border: 1px solid rgba(255,255,255,.09);
        }
        .stat-v { font-size: 24px; font-weight: 800; line-height: 1; color: #fff; }
        .stat-l { font-size: 11px; margin-top: 8px; color: rgba(255,255,255,.64); text-transform: uppercase; letter-spacing: .12em; }
        .actions { display: flex; gap: 10px; }
        .btn { border: none; border-radius: 12px; padding: 11px 16px; cursor: pointer; font-size: 14px; font-weight: 700; }
        .btn-dark { background: var(--accent-soft); color: #17191f; }
        .btn-light { background: rgba(255,255,255,.1); color: #fff; border: 1px solid rgba(255,255,255,.12); }
        .card { background: var(--card); border: 1px solid var(--line); border-radius: 22px; overflow: hidden; box-shadow: 0 16px 40px rgba(15, 23, 42, .06); }
        .card-head { padding: 18px 22px; border-bottom: 1px solid var(--line); display: flex; justify-content: space-between; gap: 16px; align-items: center; }
        .card-title { font-size: 18px; font-weight: 700; }
        .filters { display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
        .chip {
          display: inline-flex;
          align-items: center;
          gap: 8px;
          padding: 7px 10px;
          border-radius: 999px;
          background: var(--accent-soft-2);
          color: #4b5563;
          font-size: 12px;
          font-weight: 600;
        }
        .chip-label { color: #111827; font-weight: 800; text-transform: uppercase; font-size: 10px; letter-spacing: .08em; }
        .chip-muted { background: #eef2f7; }
        table { width: 100%; border-collapse: collapse; }
        th { text-align: left; padding: 14px 16px; background: #f8fafc; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .08em; border-bottom: 1px solid var(--line); }
        td { padding: 14px 16px; border-bottom: 1px solid var(--line); vertical-align: top; font-size: 13px; line-height: 1.45; }
        tbody tr:nth-child(even) td { background: #fbfcfe; }
        tr:last-child td { border-bottom: none; }
        .mono { font-family: "Courier New", monospace; white-space: nowrap; }
        .strong { font-weight: 700; color: #111827; }
        .desc { max-width: 280px; }
        .badge {
          display: inline-flex;
          align-items: center;
          padding: 5px 10px;
          border-radius: 999px;
          font-size: 11px;
          font-weight: 700;
          letter-spacing: .03em;
          white-space: nowrap;
        }
        .badge-red { background: rgba(199, 52, 79, .12); color: var(--danger); }
        .badge-amber { background: rgba(217, 119, 6, .12); color: var(--warning); }
        .badge-blue { background: rgba(37, 99, 235, .12); color: var(--info); }
        .badge-green { background: rgba(15, 159, 110, .12); color: var(--ok); }
        .badge-muted { background: #eef2f7; color: #667085; }
        .empty { text-align: center; color: var(--muted); padding: 28px; }
        .footer { margin-top: 14px; color: var(--muted); font-size: 12px; text-align: right; }
        @media print {
          body { background: #fff; }
          .page { max-width: none; padding: 0; }
          .actions { display: none; }
          .hero { box-shadow: none; margin-bottom: 12px; }
          .card { border: none; box-shadow: none; }
          .card-head { padding-left: 0; padding-right: 0; }
          .hero-stats { gap: 6px; }
          .stat { background: rgba(255,255,255,.08); }
          th, td { font-size: 11px; padding: 10px 8px; }
        }
      </style>
    </head>
    <body>
      <div class="page">
        <div class="hero">
          <div>
            <div class="brand">BURAQ logistics</div>
            <div class="title">Отчёт по проблемам</div>
            <div class="meta">Сформирован: {{ exported_at }}</div>
          </div>
          <div>
            <div class="hero-stats">
              <div class="stat">
                <div class="stat-v">{{ rows_count }}</div>
                <div class="stat-l">Инцидентов</div>
              </div>
              <div class="stat">
                <div class="stat-v">{{ exported_at[:10] }}</div>
                <div class="stat-l">Дата отчёта</div>
              </div>
            </div>
            <div class="actions" style="margin-top:14px;justify-content:flex-end">
              <button class="btn btn-light" onclick="window.close()">Закрыть</button>
              <button class="btn btn-dark" onclick="window.print()">Сохранить в PDF</button>
            </div>
          </div>
        </div>
        <div class="card">
          <div class="card-head">
            <div class="card-title">Список инцидентов</div>
            <div class="filters">{{ filters_html|safe }}</div>
          </div>
          <table>
            <thead>
              <tr>
                <th>Выявлено</th>
                <th>Партия</th>
                <th>BL код</th>
                <th>Клиент</th>
                <th>Тип</th>
                <th>Описание</th>
                <th>Статус груза</th>
                <th>Ожидаемая</th>
                <th>Факт дата</th>
              </tr>
            </thead>
            <tbody>{{ body_rows|safe }}</tbody>
          </table>
        </div>
        <div class="footer">BURAQ logistics · Problems export</div>
      </div>
    </body>
    </html>
    """

    return Response(
        render_template_string(
            report_html,
            exported_at=db.current_ts(),
            rows_count=len(rows),
            filters_html=filters_html,
            body_rows=body_rows,
        ),
        mimetype="text/html; charset=utf-8",
    )


@app.route("/api/clients")
@login_required
def api_clients():
    return jsonify(db.get_clients())


@app.route("/api/clients/<path:client_name>")
@login_required
def api_client_detail(client_name):
    detail = db.get_client_detail(client_name)
    if not detail:
        abort(404)
    return jsonify(detail)


@app.route("/api/notifications")
@login_required
def api_notifications():
    limit = int(request.args.get("limit", 30))
    return jsonify(db.get_notifications(limit))


@app.route("/api/moderator-response")
@login_required
def api_moderator_response():
    return jsonify(
        db.get_moderator_response_stats(
            status=(request.args.get("status") or "").strip(),
            date_from=(request.args.get("date_from") or "").strip(),
            date_to=(request.args.get("date_to") or "").strip(),
            role=(request.args.get("role") or "").strip(),
            limit=int(request.args.get("limit", 300)),
        )
    )


@app.route("/api/moderator-response/assignments")
@login_required
def api_moderator_response_assignments():
    return jsonify(
        {
            "groups": db.get_moderator_response_assignment_groups(),
        }
    )


@app.route("/api/moderator-response/assignments", methods=["POST"])
@editor_required
def api_save_moderator_response_assignments():
    data = request.json or {}
    chat_id = str(data.get("chat_id") or "").strip()
    if not chat_id:
        return jsonify({"error": "chat_id обязателен"}), 400
    db.set_chat_response_assignments(
        chat_id,
        moderator_tg_id=(data.get("moderator_tg_id") or "").strip(),
        sales_manager_tg_id=(data.get("sales_manager_tg_id") or "").strip(),
    )
    return jsonify({"ok": True})


@app.route("/api/moderator-response/clear", methods=["POST"])
@editor_required
def api_clear_moderator_response():
    deleted_count = db.clear_moderator_response_requests()
    return jsonify({"ok": True, "deleted_count": deleted_count})


@app.route("/api/communication-rate")
@login_required
def api_communication_rate():
    month_key = (request.args.get("month") or db.current_month_key()).strip()
    return jsonify(
        {
            "summary": db.get_communication_rate_summary(month_key),
            "rows": db.get_communication_rate(month_key),
            "recipients": db.get_communication_recipients(),
            "sent_chat_ids": list(db.get_communication_sent_chat_ids(month_key)),
            "month_key": month_key,
        }
    )


@app.route("/api/communication-rate/template")
@login_required
def api_communication_rate_template():
    return jsonify({"content": db.get_communication_rate_template()})


@app.route("/api/communication-rate/template", methods=["POST"])
@editor_required
def api_save_communication_rate_template():
    data = request.json or {}
    content = (data.get("content") or "").strip()
    if not content:
        return jsonify({"error": "Шаблон опроса не может быть пустым"}), 400
    db.save_communication_rate_template(content)
    return jsonify({"ok": True})


@app.route("/api/communication-rate/send", methods=["POST"])
@editor_required
def api_send_communication_rate():
    if not BOT_TOKEN:
        return jsonify({"error": "BOT_TOKEN не настроен в .env"}), 500

    data = request.json or {}
    month_key = (data.get("month") or db.current_month_key()).strip()
    selected_chat_ids = {str(chat_id) for chat_id in (data.get("chat_ids") or []) if str(chat_id).strip()}
    recipients = db.get_communication_recipients()
    if selected_chat_ids:
        recipients = [item for item in recipients if str(item.get("chat_id", "")) in selected_chat_ids]

    sent = 0
    skipped = 0
    errors = []

    for recipient in recipients:
        chat_id = str(recipient.get("chat_id", ""))
        if not chat_id:
            continue
        try:
            send_communication_survey(recipient, month_key)
            sent += 1
        except Exception as exc:
            errors.append(
                {
                    "client": recipient.get("client_name", ""),
                    "chat_id": chat_id,
                    "error": str(exc),
                }
            )

    return jsonify(
        {
            "ok": True,
            "month_key": month_key,
            "sent": sent,
            "skipped": skipped,
            "total_recipients": len(recipients),
            "errors": errors,
        }
    )


@app.route("/analytics/api/overview")
@login_required
def analytics_api_overview():
    return jsonify(analytics_service.get_overview(request.args))


@app.route("/analytics/api/sales-growth")
@login_required
def analytics_api_sales_growth():
    return jsonify(analytics_service.get_sales_growth(request.args))


@app.route("/analytics/api/cashflow")
@login_required
def analytics_api_cashflow():
    return jsonify(analytics_service.get_cashflow(request.args))


@app.route("/analytics/api/managers")
@login_required
def analytics_api_managers():
    return jsonify(analytics_service.get_managers(request.args))


@app.route("/analytics/api/logists")
@login_required
def analytics_api_logists():
    return jsonify(analytics_service.get_logists(request.args))


@app.route("/analytics/api/shipments")
@login_required
def analytics_api_shipments():
    return jsonify(analytics_service.get_shipments(request.args))


@app.route("/analytics/api/debts")
@login_required
def analytics_api_debts():
    return jsonify(analytics_service.get_debts(request.args))


@app.route("/analytics/api/sync/status")
@login_required
def analytics_api_sync_status():
    return jsonify(analytics_service.get_sync_settings_payload())


@app.route("/analytics/api/plans", methods=["GET"])
@login_required
def analytics_api_plans():
    return jsonify({"plans": analytics_service.list_sales_plans()})


@app.route("/analytics/api/plans", methods=["POST"])
@editor_required
def analytics_api_plans_save():
    payload = request.json or {}
    try:
        return jsonify(analytics_service.save_sales_plan(payload))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/analytics/api/plans/<int:plan_id>/activate", methods=["POST"])
@editor_required
def analytics_api_plans_activate(plan_id: int):
    return jsonify(analytics_service.activate_sales_plan(plan_id))


@app.route("/analytics/api/plans/<int:plan_id>", methods=["DELETE"])
@editor_required
def analytics_api_plans_delete(plan_id: int):
    return jsonify(analytics_service.delete_sales_plan(plan_id))


@app.route("/analytics/api/sync/config", methods=["GET"])
@login_required
def analytics_api_sync_config():
    return jsonify(analytics_service.get_sync_settings_payload())


@app.route("/analytics/api/sync/config", methods=["POST"])
@editor_required
def analytics_api_sync_config_save():
    data = request.json or {}
    sheet_id = (data.get("sheet_id") or "").strip()
    analytics_importer.set_google_sheet_id(sheet_id)
    return jsonify({"ok": True, "sheet_id": sheet_id, "status": analytics_service.get_sync_settings_payload()})


@app.route("/analytics/api/sync/google", methods=["POST"])
@editor_required
def analytics_api_sync_google():
    data = request.json or {}
    sheet_id = (data.get("sheet_id") or "").strip() or None
    try:
        result = analytics_importer.sync_from_google(sheet_id)
        return jsonify({"ok": True, **result, "status": analytics_service.get_sync_settings_payload()})
    except analytics_importer.AnalyticsImporterError as exc:
        return jsonify({"error": str(exc), "status": analytics_service.get_sync_settings_payload()}), 400
    except Exception as exc:
        app.logger.exception("Google Sheets sync failed")
        return jsonify({"error": f"Google Sheets sync failed: {exc}"}), 500


@app.route("/analytics/api/sync/upload", methods=["POST"])
@editor_required
def analytics_api_sync_upload():
    upload = request.files.get("file")
    if not upload or not upload.filename:
        return jsonify({"error": "CSV/XLSX fayl tanlanmagan"}), 400
    try:
        result = analytics_importer.sync_from_upload(upload)
        return jsonify({"ok": True, **result, "status": analytics_service.get_sync_settings_payload()})
    except analytics_importer.AnalyticsImporterError as exc:
        return jsonify({"error": str(exc), "status": analytics_service.get_sync_settings_payload()}), 400
    except Exception as exc:
        app.logger.exception("Analytics file import failed")
        return jsonify({"error": f"Import failed: {exc}"}), 500


@app.route("/analytics/api/export")
@login_required
def analytics_api_export():
    report_type = (request.args.get("report") or "sales").strip()
    export_format = (request.args.get("format") or "csv").strip().lower()
    try:
        filename_prefix, rows = analytics_service.get_export_dataset(report_type, request.args)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    if export_format == "xlsx":
        try:
            filename, content = report_exporter.export_xlsx(filename_prefix, rows)
        except RuntimeError as exc:
            return jsonify({"error": str(exc)}), 400
        return send_file(
            io.BytesIO(content),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=filename,
        )

    filename, content = report_exporter.export_csv(filename_prefix, rows)
    return Response(
        content,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/analytics/api/monitor")
@login_required
def analytics_api_monitor():
    return jsonify(monitor_service.get_monitor_payload(request.args))


@app.route("/api/template")
@login_required
def api_get_template():
    return jsonify(
        {
            "content": db.get_template(),
            "status_details": db.get_status_details(),
            "statuses": db.STATUSES,
        }
    )


@app.route("/api/template", methods=["POST"])
@editor_required
def api_save_template():
    data = request.json or {}
    content = data.get("content", "").strip()
    if not content:
        return jsonify({"error": "Шаблон не может быть пустым"}), 400

    db.save_template(content)

    details = data.get("status_details", {})
    for status_name, detail in details.items():
        if status_name in db.STATUSES:
            db.save_status_detail(status_name, detail)

    return jsonify({"ok": True})


if __name__ == "__main__":
    if BOT_TOKEN and WEBHOOK_BASE_URL:
        try:
            configure_telegram_webhook()
            app.logger.info("Telegram webhook configured")
        except Exception as exc:
            app.logger.warning("Failed to configure Telegram webhook: %s", exc)
    else:
        app.logger.warning("Telegram webhook is not configured. Set BOT_TOKEN and WEBHOOK_BASE_URL.")

    app.run(host="0.0.0.0", port=PORT, debug=False)
