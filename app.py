import html
import os
import secrets
import re
from functools import wraps

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

ROLE_EDITOR = "editor"
ROLE_VIEWER = "viewer"

ALLOWED_EXT = {"pdf", "png", "jpg", "jpeg", "xlsx", "xls", "xlsm", "doc", "docx", "zip"}

TRACK_BUTTON = "Yuk holati"
CANCEL_BUTTON = "❌ Отмена"
STATE_WAITING_BL = "waiting_bl"
COMM_RATE_PREFIX = "comm_rate"
FILE_PREFIX = "file"
TRACK_CALLBACK = "track_status"

MAIN_REPLY_MARKUP = {
    "keyboard": [[{"text": TRACK_BUTTON}]],
    "resize_keyboard": True,
    "one_time_keyboard": True,
    "is_persistent": False,
}

CANCEL_REPLY_MARKUP = {
    "keyboard": [[{"text": CANCEL_BUTTON}]],
    "resize_keyboard": True,
    "one_time_keyboard": True,
    "is_persistent": False,
}

REMOVE_REPLY_MARKUP = {
    "remove_keyboard": True,
}

GROUP_TRACK_INLINE_MARKUP = {
    "inline_keyboard": [[{"text": TRACK_BUTTON, "callback_data": TRACK_CALLBACK}]],
}

UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER") or os.path.join(str(db.APP_DATA_DIR), "uploads")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
db.init_db()


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


def communication_rating_markup(dispatch_id: int):
    options = [
        ("YOMON", 1),
        ("O'RTA", 4),
        ("YAXSHI", 7),
        ("ALO", 10),
    ]
    return {
        "inline_keyboard": [[
            {
                "text": label,
                "callback_data": f"{COMM_RATE_PREFIX}:{dispatch_id}:{score}",
            }
            for label, score in options
        ]]
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


def chat_id_is_group(chat_id) -> bool:
    return str(chat_id or "").startswith("-")


def merge_inline_markups(*markups: dict | None):
    rows = []
    for markup in markups:
        if not markup:
            continue
        for row in markup.get("inline_keyboard", []):
            rows.append(row)
    return {"inline_keyboard": rows} if rows else None


def clear_group_reply_keyboard(chat_id):
    try:
        response = telegram_send_message(chat_id, "ㅤ", reply_markup=REMOVE_REPLY_MARKUP)
        message_id = (((response or {}).get("result") or {}).get("message_id"))
        if message_id:
            try:
                telegram_delete_message(chat_id, message_id)
            except Exception:
                pass
    except Exception:
        pass


def send_group_track_prompt(chat_id, text: str | None = None, clear_keyboard: bool = False):
    if clear_keyboard:
        clear_group_reply_keyboard(chat_id)
    telegram_send_message(
        chat_id,
        text or "Yukning joriy holatini olish uchun quyidagi tugmani bosing.",
        reply_markup=GROUP_TRACK_INLINE_MARKUP,
    )


def communication_rating_label(score: int) -> str:
    return {
        1: "YOMON",
        4: "O'RTA",
        7: "YAXSHI",
        10: "ALO",
    }.get(int(score), f"{score}/10")


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

    if data == TRACK_CALLBACK:
        latest_bl = db.find_latest_bl_by_chat(chat_id)
        if latest_bl:
            db.clear_chat_state(chat_id)
            telegram_answer_callback_query(callback_id, "Yuk holati yuborildi")
            send_bl_status(chat_id, latest_bl)
            return
        db.set_chat_state(chat_id, STATE_WAITING_BL)
        telegram_answer_callback_query(callback_id, "BL-kodni yuboring")
        telegram_send_message(
            chat_id,
            "Yukingizning <b>BL-kod</b>ini yuboring.\n\nMasalan: <code>BL171</code>",
            reply_markup=REMOVE_REPLY_MARKUP if chat_id_is_group(chat_id) else CANCEL_REPLY_MARKUP,
        )
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
    if chat_id_is_group(chat_id):
        telegram_send_message(
            chat_id,
            text,
            reply_markup=merge_inline_markups(bl_file_markup(bl["id"]), GROUP_TRACK_INLINE_MARKUP),
        )
        return
    telegram_send_message(chat_id, text, reply_markup=bl_file_markup(bl["id"]) or MAIN_REPLY_MARKUP)


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
    text = (message.get("text") or "").strip()

    remember_group_chat(chat, is_active=True)

    if not chat_id or not text:
        return

    if text == "/start":
        db.clear_chat_state(chat_id)
        if chat_type in {"group", "supergroup"}:
            send_group_track_prompt(
                chat_id,
                "Yukning joriy holatini olish uchun quyidagi tugmani bosing.",
                clear_keyboard=True,
            )
            return
        telegram_send_message(
            chat_id,
            "Привет!\n\n"
            "Нажми кнопку ниже, чтобы узнать текущий статус своего груза.",
            reply_markup=MAIN_REPLY_MARKUP,
        )
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

    if text == TRACK_BUTTON:
        if chat_type in {"group", "supergroup"}:
            clear_group_reply_keyboard(chat_id)
        latest_bl = db.find_latest_bl_by_chat(chat_id)
        if latest_bl:
            db.clear_chat_state(chat_id)
            send_bl_status(chat_id, latest_bl)
            return
        db.set_chat_state(chat_id, STATE_WAITING_BL)
        telegram_send_message(
            chat_id,
            "Yukingizning <b>BL-kod</b>ini yuboring.\n\nMasalan: <code>BL171</code>",
            reply_markup=CANCEL_REPLY_MARKUP,
        )
        return

    if text == CANCEL_BUTTON:
        db.clear_chat_state(chat_id)
        telegram_send_message(
            chat_id,
            "Запрос отменён.",
            reply_markup=MAIN_REPLY_MARKUP,
        )
        return

    if db.get_chat_state(chat_id) == STATE_WAITING_BL:
        handle_bl_lookup(chat_id, text)


def handle_my_chat_member_update(chat_update: dict):
    chat = chat_update.get("chat") or {}
    chat_id = chat.get("id")
    chat_type = chat.get("type")
    if chat_type not in {"group", "supergroup"}:
        return

    new_status = ((chat_update.get("new_chat_member") or {}).get("status") or "").lower()
    is_active = new_status not in {"left", "kicked"}
    remember_group_chat(chat, is_active=is_active)
    if not chat_id:
        return
    if not is_active:
        clear_group_reply_keyboard(chat_id)
        return
    if new_status in {"member", "administrator"}:
        send_group_track_prompt(chat_id, clear_keyboard=True)


def send_bl_package(bl: dict, batch_name: str):
    if not bl["chat_id"]:
        return False, "Нет chat_id"

    try:
        telegram_send_message(
            bl["chat_id"],
            db.render_message(bl, batch_name),
            reply_markup=bl_file_markup(bl["id"]),
        )
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
    return render_template("index.html")


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
        handle_telegram_message(message)
    return jsonify({"ok": True})


@app.route("/api/stats")
@login_required
def api_stats():
    return jsonify(db.get_stats())


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


@app.route("/api/bl", methods=["POST"])
@editor_required
def api_add_bl():
    data = request.json or {}
    batch_id = data.get("batch_id")
    code = (data.get("code") or "").strip()
    client_name = (data.get("client_name") or "").strip()
    chat_id = (data.get("chat_id") or "").strip()
    cargo_type = (data.get("cargo_type") or "").strip()
    weight_kg = data.get("weight_kg", 0)
    volume_cbm = data.get("volume_cbm", 0)
    quantity_places = data.get("quantity_places", 0)
    cargo_description = (data.get("cargo_description") or "").strip()

    if not batch_id or not code:
        return jsonify({"error": "batch_id и code обязательны"}), 400

    if not db.add_bl(
        batch_id,
        code,
        client_name,
        chat_id,
        cargo_type,
        weight_kg,
        volume_cbm,
        quantity_places,
        cargo_description,
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
            data.get("cargo_type", ""),
            data.get("weight_kg", 0),
            data.get("volume_cbm", 0),
            data.get("quantity_places", 0),
            data.get("cargo_description", ""),
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

    results = []
    for bl in db.get_bl_by_batch(batch_id):
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
                "error": error_msg,
            }
        )

    sent = sum(1 for item in results if item["success"])
    return jsonify({"ok": True, "sent": sent, "total": len(results), "results": results})


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
