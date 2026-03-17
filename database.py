import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "logistic.db")

STATUSES = [
    "Принят",
    "Хоргос",
    "Алматы",
    "В пути до Ташкента",
    "Ташкент",
    "Доставлен",
]

DEFAULT_TEMPLATE = """🗓 Дата загрузки: {batch_name}
📦 BL код: {bl_code}
👤 Клиент: {client_name}

📍 Текущий статус: {status}

{status_detail}

---
По вопросам обращайтесь к вашему менеджеру."""

DEFAULT_STATUS_DETAILS = {
    "Принят": "✅ Груз принят к перевозке и оформляется на складе отправления.",
    "Хоргос": "🛃 Груз находится на таможне Хоргос. Ожидайте прохождения таможенного контроля (1–3 дня).",
    "Алматы": "🏙 Груз прибыл в Алматы. Идёт сортировка и подготовка к дальнейшей отправке.",
    "В пути до Ташкента": "🚛 Груз в пути до Ташкента. Ориентировочное время прибытия — 1–2 дня.",
    "Ташкент": "📦 Груз прибыл в Ташкент. Подготовка к выдаче клиенту.",
    "Доставлен": "✅ Груз успешно доставлен. Спасибо, что выбрали нас!",
}


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS batches (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            name      TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS bl_codes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id    INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
            code        TEXT NOT NULL,
            client_name TEXT DEFAULT '',
            chat_id     TEXT DEFAULT '',
            status      TEXT DEFAULT 'Принят',
            created_at  TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(batch_id, code)
        );

        CREATE TABLE IF NOT EXISTS files (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            bl_id      INTEGER NOT NULL REFERENCES bl_codes(id) ON DELETE CASCADE,
            filename   TEXT NOT NULL,
            file_path  TEXT NOT NULL,
            uploaded_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS send_logs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            bl_id      INTEGER REFERENCES bl_codes(id),
            bl_code    TEXT NOT NULL,
            batch_name TEXT NOT NULL,
            chat_id    TEXT NOT NULL,
            status     TEXT NOT NULL,
            success    INTEGER NOT NULL DEFAULT 1,
            error_msg  TEXT DEFAULT '',
            sent_at    TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS message_template (
            id      INTEGER PRIMARY KEY,
            content TEXT NOT NULL,
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS status_details (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT UNIQUE NOT NULL,
            detail TEXT NOT NULL
        );
    """)

    # Insert default template if not exists
    row = c.execute("SELECT id FROM message_template WHERE id=1").fetchone()
    if not row:
        c.execute("INSERT INTO message_template(id,content) VALUES(1,?)", (DEFAULT_TEMPLATE,))

    # Insert default status details
    for status, detail in DEFAULT_STATUS_DETAILS.items():
        c.execute("""
            INSERT OR IGNORE INTO status_details(status, detail) VALUES(?,?)
        """, (status, detail))

    conn.commit()
    conn.close()


# ── Batches ───────────────────────────────────────────────

def create_batch(name):
    conn = get_conn()
    try:
        conn.execute("INSERT INTO batches(name) VALUES(?)", (name,))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def get_batches():
    conn = get_conn()
    rows = conn.execute("""
        SELECT b.*, 
               COUNT(bl.id) as bl_count,
               SUM(CASE WHEN bl.chat_id != '' THEN 1 ELSE 0 END) as linked_count
        FROM batches b
        LEFT JOIN bl_codes bl ON bl.batch_id = b.id
        GROUP BY b.id
        ORDER BY b.created_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_batch(batch_id):
    conn = get_conn()
    row = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def delete_batch(batch_id):
    conn = get_conn()
    conn.execute("DELETE FROM batches WHERE id=?", (batch_id,))
    conn.commit()
    conn.close()


# ── BL Codes ──────────────────────────────────────────────

def add_bl(batch_id, code, client_name="", chat_id=""):
    conn = get_conn()
    try:
        conn.execute("""
            INSERT INTO bl_codes(batch_id, code, client_name, chat_id)
            VALUES(?,?,?,?)
        """, (batch_id, code.upper().strip(), client_name, chat_id))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def get_bl_by_batch(batch_id):
    conn = get_conn()
    rows = conn.execute("""
        SELECT bl.*, COUNT(f.id) as file_count
        FROM bl_codes bl
        LEFT JOIN files f ON f.bl_id = bl.id
        WHERE bl.batch_id=?
        GROUP BY bl.id
        ORDER BY bl.created_at
    """, (batch_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def update_bl(bl_id, client_name, chat_id, status):
    conn = get_conn()
    conn.execute("""
        UPDATE bl_codes SET client_name=?, chat_id=?, status=? WHERE id=?
    """, (client_name, chat_id, status, bl_id))
    conn.commit()
    conn.close()

def update_bl_status(bl_id, status):
    conn = get_conn()
    conn.execute("UPDATE bl_codes SET status=? WHERE id=?", (status, bl_id))
    conn.commit()
    conn.close()

def delete_bl(bl_id):
    conn = get_conn()
    conn.execute("DELETE FROM bl_codes WHERE id=?", (bl_id,))
    conn.commit()
    conn.close()

def get_bl_by_id(bl_id):
    conn = get_conn()
    row = conn.execute("SELECT * FROM bl_codes WHERE id=?", (bl_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


# ── Files ─────────────────────────────────────────────────

def add_file(bl_id, filename, file_path):
    conn = get_conn()
    conn.execute("INSERT INTO files(bl_id, filename, file_path) VALUES(?,?,?)",
                 (bl_id, filename, file_path))
    conn.commit()
    conn.close()

def get_files(bl_id):
    conn = get_conn()
    rows = conn.execute("SELECT * FROM files WHERE bl_id=?", (bl_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def delete_file(file_id):
    conn = get_conn()
    row = conn.execute("SELECT file_path FROM files WHERE id=?", (file_id,)).fetchone()
    if row:
        try: os.remove(row['file_path'])
        except: pass
    conn.execute("DELETE FROM files WHERE id=?", (file_id,))
    conn.commit()
    conn.close()


# ── Logs ──────────────────────────────────────────────────

def add_log(bl_id, bl_code, batch_name, chat_id, status, success, error_msg=""):
    conn = get_conn()
    conn.execute("""
        INSERT INTO send_logs(bl_id, bl_code, batch_name, chat_id, status, success, error_msg)
        VALUES(?,?,?,?,?,?,?)
    """, (bl_id, bl_code, batch_name, chat_id, status, 1 if success else 0, error_msg))
    conn.commit()
    conn.close()

def get_logs(limit=100):
    conn = get_conn()
    rows = conn.execute("""
        SELECT * FROM send_logs ORDER BY sent_at DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_stats():
    conn = get_conn()
    stats = {
        'batches':   conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0],
        'bl_total':  conn.execute("SELECT COUNT(*) FROM bl_codes").fetchone()[0],
        'linked':    conn.execute("SELECT COUNT(*) FROM bl_codes WHERE chat_id != ''").fetchone()[0],
        'sent':      conn.execute("SELECT COUNT(*) FROM send_logs WHERE success=1").fetchone()[0],
        'failed':    conn.execute("SELECT COUNT(*) FROM send_logs WHERE success=0").fetchone()[0],
        'delivered': conn.execute("SELECT COUNT(*) FROM bl_codes WHERE status='Доставлен'").fetchone()[0],
    }
    conn.close()
    return stats


# ── Template ──────────────────────────────────────────────

def get_template():
    conn = get_conn()
    row = conn.execute("SELECT content FROM message_template WHERE id=1").fetchone()
    conn.close()
    return row['content'] if row else DEFAULT_TEMPLATE

def save_template(content):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO message_template(id, content, updated_at)
        VALUES(1, ?, datetime('now','localtime'))
    """, (content,))
    conn.commit()
    conn.close()

def get_status_details():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM status_details ORDER BY id").fetchall()
    conn.close()
    return {r['status']: r['detail'] for r in rows}

def save_status_detail(status, detail):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO status_details(status, detail) VALUES(?,?)
    """, (status, detail))
    conn.commit()
    conn.close()


def render_message(bl: dict, batch_name: str) -> str:
    template = get_template()
    details = get_status_details()
    status = bl.get('status', 'Принят')
    return template.format(
        batch_name=batch_name,
        bl_code=bl.get('code', ''),
        client_name=bl.get('client_name', ''),
        status=status,
        status_detail=details.get(status, ''),
    )
