import os
import secrets
from functools import wraps
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, jsonify, send_from_directory, abort
)
import requests as req
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
import database as db

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))

ADMIN_LOGIN    = os.getenv("ADMIN_LOGIN", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
UPLOAD_FOLDER  = os.path.join(os.path.dirname(__file__), "uploads")
ALLOWED_EXT    = {"pdf", "png", "jpg", "jpeg", "xlsx", "xls", "docx", "zip"}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

db.init_db()


# ── Auth ──────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        u = request.form.get("username", "")
        p = request.form.get("password", "")
        if u == ADMIN_LOGIN and p == ADMIN_PASSWORD:
            session["logged_in"] = True
            session["username"] = u
            return redirect(url_for("index"))
        error = "Неверный логин или пароль"
    return render_template("index.html", login_page=True, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ── Main pages ────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    return render_template("index.html")


# ── API: Stats ────────────────────────────────────────────

@app.route("/api/stats")
@login_required
def api_stats():
    return jsonify(db.get_stats())


# ── API: Batches ──────────────────────────────────────────

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
    ok = db.create_batch(name)
    if not ok:
        return jsonify({"error": "Партия с таким именем уже существует"}), 400
    return jsonify({"ok": True})


@app.route("/api/batches/<int:batch_id>", methods=["DELETE"])
@login_required
def api_delete_batch(batch_id):
    db.delete_batch(batch_id)
    return jsonify({"ok": True})


# ── API: BL Codes ─────────────────────────────────────────

@app.route("/api/batches/<int:batch_id>/bl")
@login_required
def api_bl_list(batch_id):
    batch = db.get_batch(batch_id)
    if not batch:
        abort(404)
    bls = db.get_bl_by_batch(batch_id)
    return jsonify({"batch": batch, "bl_codes": bls, "statuses": db.STATUSES})


@app.route("/api/bl", methods=["POST"])
@login_required
def api_add_bl():
    data = request.json or {}
    batch_id    = data.get("batch_id")
    code        = (data.get("code") or "").strip()
    client_name = (data.get("client_name") or "").strip()
    chat_id     = (data.get("chat_id") or "").strip()
    if not batch_id or not code:
        return jsonify({"error": "batch_id и code обязательны"}), 400
    ok = db.add_bl(batch_id, code, client_name, chat_id)
    if not ok:
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
    )
    return jsonify({"ok": True})


@app.route("/api/bl/<int:bl_id>", methods=["DELETE"])
@login_required
def api_delete_bl(bl_id):
    db.delete_bl(bl_id)
    return jsonify({"ok": True})


# ── API: Files ────────────────────────────────────────────

@app.route("/api/bl/<int:bl_id>/files")
@login_required
def api_files(bl_id):
    return jsonify(db.get_files(bl_id))


@app.route("/api/bl/<int:bl_id>/files", methods=["POST"])
@login_required
def api_upload(bl_id):
    if "file" not in request.files:
        return jsonify({"error": "Файл не выбран"}), 400
    f = request.files["file"]
    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ALLOWED_EXT:
        return jsonify({"error": f"Тип файла .{ext} не разрешён"}), 400
    filename = secure_filename(f.filename)
    # Unique name
    unique = f"bl{bl_id}_{secrets.token_hex(4)}_{filename}"
    path = os.path.join(UPLOAD_FOLDER, unique)
    f.save(path)
    db.add_file(bl_id, filename, path)
    return jsonify({"ok": True, "filename": filename})


@app.route("/api/files/<int:file_id>", methods=["DELETE"])
@login_required
def api_delete_file(file_id):
    db.delete_file(file_id)
    return jsonify({"ok": True})


# ── API: Send ─────────────────────────────────────────────

@app.route("/api/batches/<int:batch_id>/send", methods=["POST"])
@login_required
def api_send_batch(batch_id):
    if not BOT_TOKEN:
        return jsonify({"error": "BOT_TOKEN не настроен в .env"}), 500

    batch = db.get_batch(batch_id)
    if not batch:
        abort(404)

    bls = db.get_bl_by_batch(batch_id)
    results = []

    for bl in bls:
        if not bl["chat_id"]:
            results.append({
                "code": bl["code"],
                "success": False,
                "error": "Нет chat_id"
            })
            continue

        text = db.render_message(bl, batch["name"])

        # Send message
        try:
            r = req.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": bl["chat_id"], "text": text, "parse_mode": "HTML"},
                timeout=10,
            )
            r.raise_for_status()
            msg_ok = True
            msg_err = ""
        except Exception as e:
            msg_ok = False
            msg_err = str(e)

        # Send files
        files = db.get_files(bl["id"])
        for file in files:
            try:
                with open(file["file_path"], "rb") as fh:
                    req.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                        data={"chat_id": bl["chat_id"]},
                        files={"document": (file["filename"], fh)},
                        timeout=30,
                    )
            except Exception:
                pass

        db.add_log(
            bl["id"], bl["code"], batch["name"],
            bl["chat_id"], bl["status"], msg_ok, msg_err
        )

        results.append({
            "code": bl["code"],
            "client": bl["client_name"],
            "success": msg_ok,
            "error": msg_err,
        })

    sent  = sum(1 for r in results if r["success"])
    total = len(results)
    return jsonify({"ok": True, "sent": sent, "total": total, "results": results})


# ── API: Single BL send ───────────────────────────────────

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

    # Get batch name
    conn = db.get_conn()
    batch = conn.execute("SELECT name FROM batches WHERE id=?", (bl["batch_id"],)).fetchone()
    conn.close()
    batch_name = batch["name"] if batch else "—"

    text = db.render_message(bl, batch_name)
    try:
        r = req.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": bl["chat_id"], "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        r.raise_for_status()
        db.add_log(bl["id"], bl["code"], batch_name, bl["chat_id"], bl["status"], True)
        return jsonify({"ok": True})
    except Exception as e:
        db.add_log(bl["id"], bl["code"], batch_name, bl["chat_id"], bl["status"], False, str(e))
        return jsonify({"error": str(e)}), 500


# ── API: Logs ─────────────────────────────────────────────

@app.route("/api/logs")
@login_required
def api_logs():
    limit = int(request.args.get("limit", 100))
    return jsonify(db.get_logs(limit))


# ── API: Template ─────────────────────────────────────────

@app.route("/api/template")
@login_required
def api_get_template():
    return jsonify({
        "content": db.get_template(),
        "status_details": db.get_status_details(),
        "statuses": db.STATUSES,
    })


@app.route("/api/template", methods=["POST"])
@login_required
def api_save_template():
    data = request.json or {}
    content = data.get("content", "").strip()
    if not content:
        return jsonify({"error": "Шаблон не может быть пустым"}), 400
    db.save_template(content)

    # Save status details
    details = data.get("status_details", {})
    for status, detail in details.items():
        if status in db.STATUSES:
            db.save_status_detail(status, detail)

    return jsonify({"ok": True})
import threading

def run_bot():
    try:
        import asyncio
        from bot import main
        asyncio.set_event_loop(asyncio.new_event_loop())
        main()
    except Exception as e:
        print(f"БОТ ОШИБКА: {e}", flush=True)

bot_thread = threading.Thread(target=run_bot, daemon=True)
bot_thread.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
