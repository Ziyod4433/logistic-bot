import html
import os
import secrets
from functools import wraps

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
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
PORT = int(os.getenv("PORT", "5000"))

ALLOWED_EXT = {"pdf", "png", "jpg", "jpeg", "xlsx", "xls", "docx", "zip"}

TRACK_BUTTON = "📦 Статус моего груза"
CANCEL_BUTTON = "❌ Отмена"
STATE_WAITING_BL = "waiting_bl"
COMM_RATE_PREFIX = "comm_rate"

MAIN_REPLY_MARKUP = {
    "keyboard": [[{"text": TRACK_BUTTON}]],
    "resize_keyboard": True,
}

CANCEL_REPLY_MARKUP = {
    "keyboard": [[{"text": CANCEL_BUTTON}]],
    "resize_keyboard": True,
    "one_time_keyboard": True,
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
    with open(file_path, "rb") as file_handle:
        response = req.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
            data={"chat_id": chat_id},
            files={"document": (filename, file_handle)},
            timeout=30,
        )
    response.raise_for_status()
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
    telegram_send_message(chat_id, text, reply_markup=MAIN_REPLY_MARKUP)

    files = db.get_files(bl["id"])
    for file_info in files:
        try:
            telegram_send_document(chat_id, file_info["file_path"], file_info["filename"])
        except Exception as exc:
            app.logger.warning("Failed to send file %s: %s", file_info["filename"], exc)


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
    text = (message.get("text") or "").strip()

    remember_group_chat(chat, is_active=True)

    if not chat_id or not text:
        return

    if text == "/start":
        db.clear_chat_state(chat_id)
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

    if text == TRACK_BUTTON:
        db.set_chat_state(chat_id, STATE_WAITING_BL)
        telegram_send_message(
            chat_id,
            "Введи <b>BL-код</b> своего груза.\n\nНапример: <code>BL171</code>",
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
    chat_type = chat.get("type")
    if chat_type not in {"group", "supergroup"}:
        return

    new_status = ((chat_update.get("new_chat_member") or {}).get("status") or "").lower()
    is_active = new_status not in {"left", "kicked"}
    remember_group_chat(chat, is_active=is_active)


def send_bl_package(bl: dict, batch_name: str):
    if not bl["chat_id"]:
        return False, "Нет chat_id"

    try:
        telegram_send_message(bl["chat_id"], db.render_message(bl, batch_name))
    except Exception as exc:
        return False, str(exc)

    file_errors = []
    for file_info in db.get_files(bl["id"]):
        try:
            telegram_send_document(bl["chat_id"], file_info["file_path"], file_info["filename"])
        except Exception as exc:
            file_errors.append(f"{file_info['filename']}: {exc}")

    if file_errors:
        return False, "; ".join(file_errors)

    return True, ""


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if username == ADMIN_LOGIN and password == ADMIN_PASSWORD:
            session["logged_in"] = True
            session["username"] = username
            return redirect(url_for("index"))
        error = "Неверный логин или пароль"
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
@login_required
def api_create_batch():
    data = request.json or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Имя партии обязательно"}), 400
    if not db.create_batch(name):
        return jsonify({"error": "Партия с таким именем уже существует"}), 400
    return jsonify({"ok": True})


@app.route("/api/batches/<int:batch_id>", methods=["DELETE"])
@login_required
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
@login_required
def api_add_bl():
    data = request.json or {}
    batch_id = data.get("batch_id")
    code = (data.get("code") or "").strip()
    client_name = (data.get("client_name") or "").strip()
    chat_id = (data.get("chat_id") or "").strip()
    expected_date = (data.get("expected_date") or "").strip()
    actual_date = (data.get("actual_date") or "").strip()
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
        expected_date,
        actual_date,
        cargo_type,
        weight_kg,
        volume_cbm,
        quantity_places,
        cargo_description,
    ):
        return jsonify({"error": "BL-код уже существует в этой партии"}), 400

    return jsonify({"ok": True})


@app.route("/api/bl/<int:bl_id>", methods=["PUT"])
@login_required
def api_update_bl(bl_id):
    data = request.json or {}
    db.update_bl(
        bl_id,
        data.get("client_name", ""),
        data.get("chat_id", ""),
        data.get("status", "Принят"),
        data.get("expected_date", ""),
        data.get("actual_date", ""),
        data.get("cargo_type", ""),
        data.get("weight_kg", 0),
        data.get("volume_cbm", 0),
        data.get("quantity_places", 0),
        data.get("cargo_description", ""),
    )
    return jsonify({"ok": True})


@app.route("/api/bl/<int:bl_id>", methods=["DELETE"])
@login_required
def api_delete_bl(bl_id):
    db.delete_bl(bl_id)
    return jsonify({"ok": True})


@app.route("/api/bl/<int:bl_id>/files")
@login_required
def api_files(bl_id):
    return jsonify(db.get_files(bl_id))


@app.route("/api/bl/<int:bl_id>/files", methods=["POST"])
@login_required
def api_upload(bl_id):
    if "file" not in request.files:
        return jsonify({"error": "Файл не выбран"}), 400

    uploaded_file = request.files["file"]
    ext = uploaded_file.filename.rsplit(".", 1)[-1].lower() if "." in uploaded_file.filename else ""
    if ext not in ALLOWED_EXT:
        return jsonify({"error": f"Тип файла .{ext} не разрешён"}), 400

    filename = secure_filename(uploaded_file.filename)
    unique = f"bl{bl_id}_{secrets.token_hex(4)}_{filename}"
    file_path = os.path.join(UPLOAD_FOLDER, unique)
    uploaded_file.save(file_path)
    db.add_file(bl_id, filename, file_path)
    return jsonify({"ok": True, "filename": filename})


@app.route("/api/files/<int:file_id>", methods=["DELETE"])
@login_required
def api_delete_file(file_id):
    db.delete_file(file_id)
    return jsonify({"ok": True})


@app.route("/api/batches/<int:batch_id>/send", methods=["POST"])
@login_required
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
@login_required
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
@login_required
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
        filter_items.append(f"Type: {db.PROBLEM_TYPES.get(problem_type, problem_type)}")
    if date_from:
        filter_items.append(f"From: {date_from}")
    if date_to:
        filter_items.append(f"To: {date_to}")
    if batch_id:
        filter_items.append(f"Batch ID: {batch_id}")

    body_rows = "".join(
        f"""
        <tr>
          <td>{html.escape(row.get('created_at', '') or '-')}</td>
          <td>{html.escape(row.get('batch_name', '') or '-')}</td>
          <td>{html.escape(row.get('bl_code', '') or '-')}</td>
          <td>{html.escape(row.get('client_name', '') or '-')}</td>
          <td>{html.escape(db.PROBLEM_TYPES.get(row.get('problem_type', ''), row.get('problem_type', '') or '-'))}</td>
          <td>{html.escape(row.get('description', '') or '-')}</td>
          <td>{html.escape(row.get('bl_status', '') or '-')}</td>
          <td>{html.escape(row.get('expected_date', '') or '-')}</td>
          <td>{html.escape(row.get('actual_date', '') or '-')}</td>
        </tr>
        """
        for row in rows
    )
    if not body_rows:
        body_rows = """
        <tr>
          <td colspan="9" class="empty">No problems found for the selected filters</td>
        </tr>
        """

    report_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Problems Report</title>
      <style>
        :root {
          --bg: #f4f5f7;
          --card: #ffffff;
          --text: #17191f;
          --muted: #667085;
          --line: #d8dde6;
          --accent: #111827;
          --accent-soft: #eef2f7;
        }
        * { box-sizing: border-box; }
        body { margin: 0; font-family: Arial, Helvetica, sans-serif; background: var(--bg); color: var(--text); }
        .page { max-width: 1400px; margin: 0 auto; padding: 28px; }
        .toolbar { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 20px; }
        .title { font-size: 30px; font-weight: 800; letter-spacing: -0.02em; }
        .meta { color: var(--muted); font-size: 13px; margin-top: 6px; }
        .actions { display: flex; gap: 10px; }
        .btn { border: none; border-radius: 10px; padding: 11px 16px; cursor: pointer; font-size: 14px; font-weight: 700; }
        .btn-dark { background: var(--accent); color: #fff; }
        .btn-light { background: var(--accent-soft); color: var(--accent); }
        .card { background: var(--card); border: 1px solid var(--line); border-radius: 18px; overflow: hidden; box-shadow: 0 10px 30px rgba(15, 23, 42, .06); }
        .card-head { padding: 18px 22px; border-bottom: 1px solid var(--line); display: flex; justify-content: space-between; gap: 16px; align-items: center; }
        .card-title { font-size: 18px; font-weight: 700; }
        .filters { color: var(--muted); font-size: 13px; }
        table { width: 100%; border-collapse: collapse; }
        th { text-align: left; padding: 14px 16px; background: #f8fafc; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .08em; border-bottom: 1px solid var(--line); }
        td { padding: 14px 16px; border-bottom: 1px solid var(--line); vertical-align: top; font-size: 13px; line-height: 1.45; }
        tr:last-child td { border-bottom: none; }
        .empty { text-align: center; color: var(--muted); padding: 28px; }
        .footer { margin-top: 14px; color: var(--muted); font-size: 12px; text-align: right; }
        @media print {
          body { background: #fff; }
          .page { max-width: none; padding: 0; }
          .actions { display: none; }
          .card { border: none; box-shadow: none; }
          .card-head { padding-left: 0; padding-right: 0; }
          th, td { font-size: 11px; padding: 10px 8px; }
        }
      </style>
    </head>
    <body>
      <div class="page">
        <div class="toolbar">
          <div>
            <div class="title">Problems Report</div>
            <div class="meta">Exported: {{ exported_at }} / Total rows: {{ rows_count }}</div>
          </div>
          <div class="actions">
            <button class="btn btn-light" onclick="window.close()">Close</button>
            <button class="btn btn-dark" onclick="window.print()">Save as PDF</button>
          </div>
        </div>
        <div class="card">
          <div class="card-head">
            <div class="card-title">Problem List</div>
            <div class="filters">{{ filters_label }}</div>
          </div>
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Batch</th>
                <th>BL Code</th>
                <th>Client</th>
                <th>Type</th>
                <th>Description</th>
                <th>Cargo Status</th>
                <th>Expected</th>
                <th>Actual Date</th>
              </tr>
            </thead>
            <tbody>{{ body_rows|safe }}</tbody>
          </table>
        </div>
        <div class="footer">BURAQ logistics ? Problems export</div>
      </div>
    </body>
    </html>
    """

    return Response(
        render_template_string(
            report_html,
            exported_at=db.current_ts(),
            rows_count=len(rows),
            filters_label=" / ".join(filter_items) if filter_items else "No filters",
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
@login_required
def api_save_communication_rate_template():
    data = request.json or {}
    content = (data.get("content") or "").strip()
    if not content:
        return jsonify({"error": "Шаблон опроса не может быть пустым"}), 400
    db.save_communication_rate_template(content)
    return jsonify({"ok": True})


@app.route("/api/communication-rate/send", methods=["POST"])
@login_required
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
@login_required
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
