import os
import sqlite3
from datetime import date

DB_PATH = os.path.join(os.path.dirname(__file__), "logistic.db")

STATUSES = [
    "Принят",
    "Хоргос",
    "Алматы",
    "В пути до Ташкента",
    "Ташкент",
    "Доставлен",
]

PROBLEM_TYPES = {
    "damage": "Повреждение",
    "delay": "Опоздание",
    "shortage": "Недостача",
    "other": "Другое",
}

DEFAULT_TEMPLATE = """🗓 Дата загрузки: {batch_name}
📦 BL код: {bl_code}
👤 Клиент: {client_name}

📍 Текущий статус: {status}

{status_detail}

---
По вопросам обращайтесь к вашему менеджеру."""

DEFAULT_COMMUNICATION_RATE_TEMPLATE = """Опрос за {month_key}

Пожалуйста, оцени работу менеджера по коммуникации для клиента <b>{client_name}</b>.
Партия: <b>{batch_name}</b>

Шкала: <b>1–10</b>, где 10 — отлично.
Эта оценка не будет показана в группе. Её увидит только админ панели."""

DEFAULT_STATUS_DETAILS = {
    "Принят": "✅ Груз принят к перевозке и оформляется на складе отправления.",
    "Хоргос": "🛃 Груз находится на таможне Хоргос. Ожидайте прохождения контроля в течение 1-3 дней.",
    "Алматы": "🏙 Груз прибыл в Алматы. Идёт сортировка и подготовка к дальнейшей отправке.",
    "В пути до Ташкента": "🚛 Груз в пути до Ташкента. Ориентировочное время прибытия — 1-2 дня.",
    "Ташкент": "📦 Груз прибыл в Ташкент. Подготовка к выдаче клиенту.",
    "Доставлен": "✅ Груз успешно доставлен. Спасибо, что выбрали нас!",
}

STUCK_DAYS = 5


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(row["name"] == column_name for row in rows)


def _late_sql(alias: str = "bl") -> str:
    return f"""
    CASE
        WHEN COALESCE({alias}.expected_date, '') = '' THEN 0
        WHEN COALESCE({alias}.actual_date, '') != ''
             AND date({alias}.actual_date) > date({alias}.expected_date) THEN 1
        WHEN COALESCE({alias}.actual_date, '') = ''
             AND {alias}.status != 'Доставлен'
             AND date('now','localtime') > date({alias}.expected_date) THEN 1
        ELSE 0
    END
    """


def _stuck_sql(alias: str = "bl") -> str:
    return f"""
    CASE
        WHEN {alias}.status = 'Доставлен' THEN 0
        WHEN julianday('now','localtime') - julianday(COALESCE(NULLIF({alias}.status_updated_at, ''), {alias}.created_at)) >= {STUCK_DAYS}
            THEN 1
        ELSE 0
    END
    """


def init_db():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.executescript(
        """
        CREATE TABLE IF NOT EXISTS batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS bl_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
            code TEXT NOT NULL,
            client_name TEXT DEFAULT '',
            chat_id TEXT DEFAULT '',
            status TEXT DEFAULT 'Принят',
            cargo_type TEXT DEFAULT '',
            weight_kg REAL NOT NULL DEFAULT 0,
            volume_cbm REAL NOT NULL DEFAULT 0,
            quantity_places INTEGER NOT NULL DEFAULT 0,
            cargo_description TEXT DEFAULT '',
            expected_date TEXT DEFAULT '',
            actual_date TEXT DEFAULT '',
            status_updated_at TEXT DEFAULT (datetime('now','localtime')),
            created_at TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(batch_id, code)
        );

        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bl_id INTEGER NOT NULL REFERENCES bl_codes(id) ON DELETE CASCADE,
            filename TEXT NOT NULL,
            file_path TEXT NOT NULL,
            uploaded_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS send_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bl_id INTEGER REFERENCES bl_codes(id),
            bl_code TEXT NOT NULL,
            batch_name TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            status TEXT NOT NULL,
            success INTEGER NOT NULL DEFAULT 1,
            error_msg TEXT DEFAULT '',
            sent_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS message_template (
            id INTEGER PRIMARY KEY,
            content TEXT NOT NULL,
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS status_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT UNIQUE NOT NULL,
            detail TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS telegram_sessions (
            chat_id TEXT PRIMARY KEY,
            state TEXT NOT NULL DEFAULT '',
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS telegram_chats (
            chat_id TEXT PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            chat_type TEXT NOT NULL DEFAULT 'group',
            username TEXT DEFAULT '',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            last_seen_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS problems (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bl_id INTEGER NOT NULL REFERENCES bl_codes(id) ON DELETE CASCADE,
            batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
            problem_type TEXT NOT NULL,
            description TEXT DEFAULT '',
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS communication_survey_sends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month_key TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            client_name TEXT NOT NULL DEFAULT '',
            bl_id INTEGER REFERENCES bl_codes(id) ON DELETE SET NULL,
            batch_id INTEGER REFERENCES batches(id) ON DELETE SET NULL,
            batch_name TEXT DEFAULT '',
            sent_at TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(month_key, chat_id)
        );

        CREATE TABLE IF NOT EXISTS communication_ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month_key TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            client_name TEXT NOT NULL DEFAULT '',
            bl_id INTEGER REFERENCES bl_codes(id) ON DELETE SET NULL,
            batch_id INTEGER REFERENCES batches(id) ON DELETE SET NULL,
            score INTEGER NOT NULL,
            submitted_at TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(month_key, chat_id)
        );

        CREATE TABLE IF NOT EXISTS communication_rate_template (
            id INTEGER PRIMARY KEY,
            content TEXT NOT NULL,
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );
        """
    )

    legacy_columns = [
        ("cargo_type", "TEXT DEFAULT ''"),
        ("weight_kg", "REAL NOT NULL DEFAULT 0"),
        ("volume_cbm", "REAL NOT NULL DEFAULT 0"),
        ("quantity_places", "INTEGER NOT NULL DEFAULT 0"),
        ("cargo_description", "TEXT DEFAULT ''"),
        ("expected_date", "TEXT DEFAULT ''"),
        ("actual_date", "TEXT DEFAULT ''"),
        ("status_updated_at", "TEXT DEFAULT ''"),
    ]
    for column_name, column_def in legacy_columns:
        if not _table_has_column(conn, "bl_codes", column_name):
            conn.execute(f"ALTER TABLE bl_codes ADD COLUMN {column_name} {column_def}")

    conn.execute(
        """
        UPDATE bl_codes
        SET status_updated_at = COALESCE(NULLIF(status_updated_at, ''), created_at, datetime('now','localtime'))
        WHERE status_updated_at IS NULL OR status_updated_at = ''
        """
    )

    row = cursor.execute("SELECT id FROM message_template WHERE id = 1").fetchone()
    if not row:
        cursor.execute("INSERT INTO message_template(id, content) VALUES(1, ?)", (DEFAULT_TEMPLATE,))

    row = cursor.execute("SELECT id FROM communication_rate_template WHERE id = 1").fetchone()
    if not row:
        cursor.execute(
            "INSERT INTO communication_rate_template(id, content) VALUES(1, ?)",
            (DEFAULT_COMMUNICATION_RATE_TEMPLATE,),
        )

    for status_name, detail in DEFAULT_STATUS_DETAILS.items():
        cursor.execute(
            "INSERT OR IGNORE INTO status_details(status, detail) VALUES(?, ?)",
            (status_name, detail),
        )

    conn.commit()
    conn.close()


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
    rows = conn.execute(
        f"""
        SELECT
            b.*,
            (SELECT COUNT(*) FROM bl_codes bl WHERE bl.batch_id = b.id) AS bl_count,
            (SELECT COUNT(*) FROM bl_codes bl WHERE bl.batch_id = b.id AND bl.chat_id != '') AS linked_count,
            (SELECT COUNT(*) FROM bl_codes bl WHERE bl.batch_id = b.id AND {_late_sql('bl')} = 1) AS late_count,
            (
                SELECT COUNT(DISTINCT p.bl_id)
                FROM problems p
                WHERE p.batch_id = b.id AND p.status = 'open'
            ) AS problem_count
        FROM batches b
        ORDER BY b.created_at DESC
        """
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_batch(batch_id):
    conn = get_conn()
    row = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def delete_batch(batch_id):
    conn = get_conn()
    conn.execute("DELETE FROM batches WHERE id = ?", (batch_id,))
    conn.commit()
    conn.close()


def _to_float(value):
    if value in (None, ""):
        return 0.0
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def _to_int(value):
    if value in (None, ""):
        return 0
    try:
        return int(float(str(value).replace(",", ".")))
    except (TypeError, ValueError):
        return 0


def format_cargo_info(bl: dict) -> str:
    parts = []
    cargo_type = (bl.get("cargo_type") or "").strip()
    if cargo_type:
        parts.append(f"Вид товара: {cargo_type}")

    weight = _to_float(bl.get("weight_kg"))
    if weight:
        parts.append(f"Вес: {weight:g} кг")

    volume = _to_float(bl.get("volume_cbm"))
    if volume:
        parts.append(f"Объём: {volume:g} м³")

    quantity = _to_int(bl.get("quantity_places"))
    if quantity:
        parts.append(f"Количество: {quantity} мест")

    description = (bl.get("cargo_description") or "").strip()
    if description:
        parts.append(f"Описание: {description}")

    return "\n".join(parts) if parts else "Грузовые параметры не указаны."


def add_bl(
    batch_id,
    code,
    client_name="",
    chat_id="",
    expected_date="",
    actual_date="",
    cargo_type="",
    weight_kg=0,
    volume_cbm=0,
    quantity_places=0,
    cargo_description="",
):
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO bl_codes(
                batch_id, code, client_name, chat_id, cargo_type, weight_kg, volume_cbm, quantity_places,
                cargo_description, expected_date, actual_date, status_updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
            """,
            (
                batch_id,
                code.upper().strip(),
                client_name.strip(),
                chat_id.strip(),
                (cargo_type or "").strip(),
                _to_float(weight_kg),
                _to_float(volume_cbm),
                _to_int(quantity_places),
                (cargo_description or "").strip(),
                (expected_date or "").strip(),
                (actual_date or "").strip(),
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def get_bl_by_batch(batch_id):
    conn = get_conn()
    rows = conn.execute(
        f"""
        SELECT
            bl.*,
            (SELECT COUNT(*) FROM files f WHERE f.bl_id = bl.id) AS file_count,
            (SELECT COUNT(*) FROM problems p WHERE p.bl_id = bl.id AND p.status = 'open') AS problem_count,
            (SELECT p.problem_type FROM problems p WHERE p.bl_id = bl.id AND p.status = 'open' ORDER BY p.created_at DESC LIMIT 1) AS latest_problem_type,
            {_late_sql('bl')} AS is_late,
            {_stuck_sql('bl')} AS is_stuck
        FROM bl_codes bl
        WHERE bl.batch_id = ?
        ORDER BY bl.created_at
        """,
        (batch_id,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_bl_by_id(bl_id):
    conn = get_conn()
    row = conn.execute("SELECT * FROM bl_codes WHERE id = ?", (bl_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def find_bl_by_code(code):
    conn = get_conn()
    row = conn.execute(
        """
        SELECT bl.*, b.name AS batch_name
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE UPPER(bl.code) = UPPER(?)
        ORDER BY bl.created_at DESC
        LIMIT 1
        """,
        (code.strip(),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def update_bl(
    bl_id,
    client_name,
    chat_id,
    status,
    expected_date="",
    actual_date="",
    cargo_type="",
    weight_kg=0,
    volume_cbm=0,
    quantity_places=0,
    cargo_description="",
):
    conn = get_conn()
    conn.execute(
        """
        UPDATE bl_codes
        SET
            client_name = ?,
            chat_id = ?,
            status = ?,
            cargo_type = ?,
            weight_kg = ?,
            volume_cbm = ?,
            quantity_places = ?,
            cargo_description = ?,
            expected_date = ?,
            actual_date = ?,
            status_updated_at = CASE
                WHEN status != ? THEN datetime('now','localtime')
                ELSE status_updated_at
            END
        WHERE id = ?
        """,
        (
            client_name.strip(),
            chat_id.strip(),
            status,
            (cargo_type or "").strip(),
            _to_float(weight_kg),
            _to_float(volume_cbm),
            _to_int(quantity_places),
            (cargo_description or "").strip(),
            (expected_date or "").strip(),
            (actual_date or "").strip(),
            status,
            bl_id,
        ),
    )
    conn.commit()
    conn.close()


def delete_bl(bl_id):
    conn = get_conn()
    conn.execute("DELETE FROM bl_codes WHERE id = ?", (bl_id,))
    conn.commit()
    conn.close()


def add_file(bl_id, filename, file_path):
    conn = get_conn()
    conn.execute(
        "INSERT INTO files(bl_id, filename, file_path) VALUES(?, ?, ?)",
        (bl_id, filename, file_path),
    )
    conn.commit()
    conn.close()


def get_files(bl_id):
    conn = get_conn()
    rows = conn.execute("SELECT * FROM files WHERE bl_id = ?", (bl_id,)).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def delete_file(file_id):
    conn = get_conn()
    row = conn.execute("SELECT file_path FROM files WHERE id = ?", (file_id,)).fetchone()
    if row:
        try:
            os.remove(row["file_path"])
        except OSError:
            pass
    conn.execute("DELETE FROM files WHERE id = ?", (file_id,))
    conn.commit()
    conn.close()


def add_log(bl_id, bl_code, batch_name, chat_id, status, success, error_msg=""):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO send_logs(bl_id, bl_code, batch_name, chat_id, status, success, error_msg)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (bl_id, bl_code, batch_name, chat_id, status, 1 if success else 0, error_msg),
    )
    conn.commit()
    conn.close()


def get_logs(limit=100):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM send_logs ORDER BY sent_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_stats():
    conn = get_conn()
    stats = {
        "batches": conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0],
        "bl_total": conn.execute("SELECT COUNT(*) FROM bl_codes").fetchone()[0],
        "linked": conn.execute("SELECT COUNT(*) FROM bl_codes WHERE chat_id != ''").fetchone()[0],
        "sent": conn.execute("SELECT COUNT(*) FROM send_logs WHERE success = 1").fetchone()[0],
        "failed": conn.execute("SELECT COUNT(*) FROM send_logs WHERE success = 0").fetchone()[0],
        "delivered": conn.execute("SELECT COUNT(*) FROM bl_codes WHERE status = 'Доставлен'").fetchone()[0],
        "late": conn.execute(f"SELECT COUNT(*) FROM bl_codes bl WHERE {_late_sql('bl')} = 1").fetchone()[0],
        "problematic": conn.execute(
            "SELECT COUNT(DISTINCT bl_id) FROM problems WHERE status = 'open'"
        ).fetchone()[0],
        "clients": conn.execute(
            "SELECT COUNT(DISTINCT TRIM(client_name)) FROM bl_codes WHERE TRIM(client_name) != ''"
        ).fetchone()[0],
    }
    conn.close()
    return stats


def get_template():
    conn = get_conn()
    row = conn.execute("SELECT content FROM message_template WHERE id = 1").fetchone()
    conn.close()
    return row["content"] if row else DEFAULT_TEMPLATE


def save_template(content):
    conn = get_conn()
    conn.execute(
        """
        INSERT OR REPLACE INTO message_template(id, content, updated_at)
        VALUES (1, ?, datetime('now','localtime'))
        """,
        (content,),
    )
    conn.commit()
    conn.close()


def get_status_details():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM status_details ORDER BY id").fetchall()
    conn.close()
    return {row["status"]: row["detail"] for row in rows}


def save_status_detail(status, detail):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO status_details(status, detail) VALUES(?, ?)",
        (status, detail),
    )
    conn.commit()
    conn.close()


def render_message(bl: dict, batch_name: str) -> str:
    template = get_template()
    details = get_status_details()
    status = bl.get("status", "Принят")
    return template.format(
        batch_name=batch_name,
        bl_code=bl.get("code", ""),
        client_name=bl.get("client_name", ""),
        status=status,
        cargo_info=format_cargo_info(bl),
        status_detail=details.get(status, ""),
    )


def set_chat_state(chat_id, state):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO telegram_sessions(chat_id, state, updated_at)
        VALUES (?, ?, datetime('now','localtime'))
        ON CONFLICT(chat_id) DO UPDATE SET
            state = excluded.state,
            updated_at = datetime('now','localtime')
        """,
        (str(chat_id), state),
    )
    conn.commit()
    conn.close()


def get_chat_state(chat_id):
    conn = get_conn()
    row = conn.execute(
        "SELECT state FROM telegram_sessions WHERE chat_id = ?",
        (str(chat_id),),
    ).fetchone()
    conn.close()
    return row["state"] if row else ""


def clear_chat_state(chat_id):
    conn = get_conn()
    conn.execute("DELETE FROM telegram_sessions WHERE chat_id = ?", (str(chat_id),))
    conn.commit()
    conn.close()


def upsert_telegram_chat(chat_id, title, chat_type, username="", is_active=True):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO telegram_chats(chat_id, title, chat_type, username, is_active, created_at, last_seen_at)
        VALUES (?, ?, ?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))
        ON CONFLICT(chat_id) DO UPDATE SET
            title = excluded.title,
            chat_type = excluded.chat_type,
            username = excluded.username,
            is_active = excluded.is_active,
            last_seen_at = datetime('now','localtime')
        """,
        (str(chat_id), title or "", chat_type or "group", username or "", 1 if is_active else 0),
    )
    conn.commit()
    conn.close()


def get_telegram_chats(include_inactive=False):
    conn = get_conn()
    where = "" if include_inactive else "WHERE c.is_active = 1"
    rows = conn.execute(
        f"""
        SELECT
            c.*,
            COUNT(bl.id) AS linked_count
        FROM telegram_chats c
        LEFT JOIN bl_codes bl ON bl.chat_id = c.chat_id
        {where}
        GROUP BY c.chat_id
        ORDER BY c.is_active DESC, c.last_seen_at DESC, c.created_at DESC
        """
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def create_problem(bl_id, problem_type, description=""):
    conn = get_conn()
    bl = conn.execute("SELECT id, batch_id FROM bl_codes WHERE id = ?", (bl_id,)).fetchone()
    if not bl:
        conn.close()
        return False

    conn.execute(
        """
        INSERT INTO problems(bl_id, batch_id, problem_type, description, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, 'open', datetime('now','localtime'), datetime('now','localtime'))
        """,
        (bl["id"], bl["batch_id"], problem_type, (description or "").strip()),
    )
    conn.commit()
    conn.close()
    return True


def get_problems(problem_type="", date_from="", date_to="", batch_id=""):
    conn = get_conn()
    conditions = []
    params = []

    if problem_type:
        conditions.append("p.problem_type = ?")
        params.append(problem_type)
    if batch_id:
        conditions.append("p.batch_id = ?")
        params.append(batch_id)
    if date_from:
        conditions.append("date(p.created_at) >= date(?)")
        params.append(date_from)
    if date_to:
        conditions.append("date(p.created_at) <= date(?)")
        params.append(date_to)

    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = conn.execute(
        f"""
        SELECT
            p.*,
            bl.code AS bl_code,
            bl.client_name,
            bl.chat_id,
            bl.status AS bl_status,
            bl.expected_date,
            bl.actual_date,
            {_late_sql('bl')} AS is_late,
            b.name AS batch_name
        FROM problems p
        JOIN bl_codes bl ON bl.id = p.bl_id
        JOIN batches b ON b.id = p.batch_id
        {where_sql}
        ORDER BY p.created_at DESC
        """,
        params,
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_clients():
    conn = get_conn()
    rows = conn.execute(
        f"""
        SELECT
            TRIM(bl.client_name) AS client_name,
            COUNT(*) AS bl_count,
            COUNT(DISTINCT bl.batch_id) AS batch_count,
            SUM(CASE WHEN bl.status = 'Доставлен' THEN 1 ELSE 0 END) AS delivered_count,
            SUM(CASE WHEN {_late_sql('bl')} = 1 THEN 1 ELSE 0 END) AS late_count,
            (
                SELECT COUNT(*)
                FROM problems p
                JOIN bl_codes bl2 ON bl2.id = p.bl_id
                WHERE TRIM(bl2.client_name) = TRIM(bl.client_name)
            ) AS problem_count,
            MAX(bl.created_at) AS last_bl_at
        FROM bl_codes bl
        WHERE TRIM(bl.client_name) != ''
        GROUP BY TRIM(bl.client_name)
        ORDER BY last_bl_at DESC, client_name COLLATE NOCASE ASC
        """
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_client_detail(client_name):
    conn = get_conn()
    summary_row = conn.execute(
        f"""
        SELECT
            TRIM(bl.client_name) AS client_name,
            COUNT(*) AS bl_count,
            COUNT(DISTINCT bl.batch_id) AS batch_count,
            SUM(CASE WHEN bl.status = 'Доставлен' THEN 1 ELSE 0 END) AS delivered_count,
            SUM(CASE WHEN bl.status != 'Доставлен' THEN 1 ELSE 0 END) AS active_count,
            SUM(CASE WHEN {_late_sql('bl')} = 1 THEN 1 ELSE 0 END) AS late_count,
            (
                SELECT COUNT(*)
                FROM problems p
                JOIN bl_codes bl2 ON bl2.id = p.bl_id
                WHERE TRIM(bl2.client_name) = ?
            ) AS problem_count,
            MAX(bl.created_at) AS last_bl_at
        FROM bl_codes bl
        WHERE TRIM(bl.client_name) = ?
        GROUP BY TRIM(bl.client_name)
        """,
        (client_name, client_name),
    ).fetchone()

    if not summary_row:
        conn.close()
        return None

    items = conn.execute(
        f"""
        SELECT
            bl.*,
            b.name AS batch_name,
            (SELECT COUNT(*) FROM problems p WHERE p.bl_id = bl.id) AS problem_count,
            {_late_sql('bl')} AS is_late
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE TRIM(bl.client_name) = ?
        ORDER BY bl.created_at DESC
        """,
        (client_name,),
    ).fetchall()
    conn.close()
    return {
        "summary": dict(summary_row),
        "bl_codes": [dict(row) for row in items],
    }


def get_attention_items(limit=10):
    conn = get_conn()
    rows = conn.execute(
        f"""
        SELECT
            bl.id,
            bl.code,
            bl.client_name,
            bl.status,
            bl.expected_date,
            bl.actual_date,
            bl.status_updated_at,
            b.id AS batch_id,
            b.name AS batch_name,
            {_late_sql('bl')} AS is_late,
            {_stuck_sql('bl')} AS is_stuck,
            (SELECT COUNT(*) FROM problems p WHERE p.bl_id = bl.id AND p.status = 'open') AS problem_count,
            (SELECT p.problem_type FROM problems p WHERE p.bl_id = bl.id AND p.status = 'open' ORDER BY p.created_at DESC LIMIT 1) AS latest_problem_type
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE {_late_sql('bl')} = 1
           OR {_stuck_sql('bl')} = 1
           OR EXISTS (SELECT 1 FROM problems p WHERE p.bl_id = bl.id AND p.status = 'open')
        ORDER BY
            {_late_sql('bl')} DESC,
            (SELECT COUNT(*) FROM problems p WHERE p.bl_id = bl.id AND p.status = 'open') DESC,
            bl.status_updated_at ASC,
            bl.created_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    items = []
    for row in rows:
        item = dict(row)
        reasons = []
        if item["is_late"]:
            if item.get("actual_date"):
                reasons.append("факт позже ожидаемой даты")
            else:
                reasons.append("груз просрочен")
        if item["problem_count"]:
            problem_label = PROBLEM_TYPES.get(item.get("latest_problem_type") or "", "Проблема")
            reasons.append(f"открытых инцидентов: {item['problem_count']} ({problem_label})")
        if item["is_stuck"]:
            reasons.append(f"статус не менялся более {STUCK_DAYS} дней")
        item["attention_reason"] = " • ".join(reasons)
        items.append(item)
    return items


def get_notifications(limit=30):
    conn = get_conn()
    notifications = []

    problem_rows = conn.execute(
        """
        SELECT
            p.created_at AS event_at,
            p.problem_type,
            p.description,
            b.id AS batch_id,
            b.name AS batch_name,
            bl.code AS bl_code,
            bl.client_name
        FROM problems p
        JOIN bl_codes bl ON bl.id = p.bl_id
        JOIN batches b ON b.id = p.batch_id
        ORDER BY p.created_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    for row in problem_rows:
        item = dict(row)
        notifications.append(
            {
                "kind": "problem",
                "level": "high" if item["problem_type"] in {"damage", "shortage"} else "medium",
                "title": f"{item['bl_code']} — {PROBLEM_TYPES.get(item['problem_type'], 'Проблема')}",
                "message": item["description"] or item["client_name"] or "Новый инцидент по грузу",
                "batch_id": item["batch_id"],
                "batch_name": item["batch_name"],
                "created_at": item["event_at"],
            }
        )

    late_rows = conn.execute(
        f"""
        SELECT
            bl.expected_date AS event_at,
            b.id AS batch_id,
            b.name AS batch_name,
            bl.code AS bl_code,
            bl.client_name,
            bl.expected_date,
            bl.actual_date
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE {_late_sql('bl')} = 1
        ORDER BY date(bl.expected_date) DESC, bl.created_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    for row in late_rows:
        item = dict(row)
        late_msg = (
            f"Факт: {item['actual_date']} позже ожидания {item['expected_date']}"
            if item["actual_date"]
            else f"Ожидалась дата {item['expected_date']}, груз ещё не закрыт"
        )
        notifications.append(
            {
                "kind": "late",
                "level": "high",
                "title": f"{item['bl_code']} — опаздывает",
                "message": late_msg,
                "batch_id": item["batch_id"],
                "batch_name": item["batch_name"],
                "created_at": item["event_at"] or "",
            }
        )

    failed_rows = conn.execute(
        """
        SELECT
            sent_at AS event_at,
            bl_id,
            bl_code,
            batch_name,
            error_msg
        FROM send_logs
        WHERE success = 0
        ORDER BY sent_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    for row in failed_rows:
        item = dict(row)
        batch_id_row = conn.execute(
            "SELECT batch_id FROM bl_codes WHERE id = ?",
            (item["bl_id"],),
        ).fetchone()
        notifications.append(
            {
                "kind": "send_fail",
                "level": "medium",
                "title": f"{item['bl_code']} — ошибка отправки",
                "message": item["error_msg"] or "Telegram не принял сообщение или файл",
                "batch_id": batch_id_row["batch_id"] if batch_id_row else None,
                "batch_name": item["batch_name"],
                "created_at": item["event_at"],
            }
        )

    conn.close()
    notifications.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return notifications[:limit]


def current_month_key():
    return date.today().strftime("%Y-%m")


def get_communication_rate_template():
    conn = get_conn()
    row = conn.execute("SELECT content FROM communication_rate_template WHERE id = 1").fetchone()
    conn.close()
    return row["content"] if row else DEFAULT_COMMUNICATION_RATE_TEMPLATE


def save_communication_rate_template(content):
    conn = get_conn()
    conn.execute(
        """
        INSERT OR REPLACE INTO communication_rate_template(id, content, updated_at)
        VALUES (1, ?, datetime('now','localtime'))
        """,
        (content,),
    )
    conn.commit()
    conn.close()


def render_communication_rate_message(recipient: dict, month_key: str) -> str:
    return get_communication_rate_template().format(
        month_key=month_key,
        client_name=recipient.get("client_name") or "клиент",
        batch_name=recipient.get("batch_name") or "—",
        chat_id=recipient.get("chat_id") or "",
    )


def _get_latest_chat_recipient(conn, chat_id):
    row = conn.execute(
        """
        SELECT
            bl.id AS bl_id,
            bl.batch_id,
            bl.chat_id,
            COALESCE(NULLIF(TRIM(bl.client_name), ''), bl.code) AS client_name,
            b.name AS batch_name
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE bl.chat_id = ?
        ORDER BY bl.created_at DESC
        LIMIT 1
        """,
        (str(chat_id),),
    ).fetchone()
    return dict(row) if row else None


def get_communication_recipients():
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            bl.id AS bl_id,
            bl.batch_id,
            bl.chat_id,
            COALESCE(NULLIF(TRIM(bl.client_name), ''), bl.code) AS client_name,
            b.name AS batch_name
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE bl.chat_id != ''
          AND bl.id = (
              SELECT bl2.id
              FROM bl_codes bl2
              WHERE bl2.chat_id = bl.chat_id
              ORDER BY bl2.created_at DESC
              LIMIT 1
          )
        ORDER BY client_name COLLATE NOCASE ASC
        """
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_communication_sent_chat_ids(month_key):
    conn = get_conn()
    rows = conn.execute(
        "SELECT chat_id FROM communication_survey_sends WHERE month_key = ?",
        (month_key,),
    ).fetchall()
    conn.close()
    return {str(row["chat_id"]) for row in rows}


def record_communication_survey_send(month_key, recipient):
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO communication_survey_sends(
                month_key, chat_id, client_name, bl_id, batch_id, batch_name
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                month_key,
                str(recipient.get("chat_id", "")),
                recipient.get("client_name", ""),
                recipient.get("bl_id"),
                recipient.get("batch_id"),
                recipient.get("batch_name", ""),
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def save_communication_rating(month_key, chat_id, score):
    score_value = _to_int(score)
    if score_value < 1 or score_value > 10:
        return False

    conn = get_conn()
    existing = conn.execute(
        "SELECT id FROM communication_ratings WHERE month_key = ? AND chat_id = ?",
        (month_key, str(chat_id)),
    ).fetchone()
    if existing:
        conn.close()
        return "exists"

    send_row = conn.execute(
        """
        SELECT chat_id, client_name, bl_id, batch_id
        FROM communication_survey_sends
        WHERE month_key = ? AND chat_id = ?
        """,
        (month_key, str(chat_id)),
    ).fetchone()

    recipient = dict(send_row) if send_row else _get_latest_chat_recipient(conn, chat_id)
    if not recipient:
        conn.close()
        return False

    conn.execute(
        """
        INSERT OR IGNORE INTO communication_ratings(
            month_key, chat_id, client_name, bl_id, batch_id, score, submitted_at
        )
        VALUES (?, ?, ?, ?, ?, ?, datetime('now','localtime'))
        """,
        (
            month_key,
            str(chat_id),
            recipient.get("client_name", ""),
            recipient.get("bl_id"),
            recipient.get("batch_id"),
            score_value,
        ),
    )
    conn.commit()
    conn.close()
    return True


def get_communication_rate(month_key):
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            s.month_key,
            s.chat_id,
            s.client_name,
            s.batch_id,
            s.batch_name,
            s.sent_at,
            r.score,
            r.submitted_at
        FROM communication_survey_sends s
        LEFT JOIN communication_ratings r
            ON r.month_key = s.month_key
           AND r.chat_id = s.chat_id
        WHERE s.month_key = ?
        ORDER BY
            CASE WHEN r.score IS NULL THEN 1 ELSE 0 END,
            COALESCE(r.submitted_at, '') DESC,
            s.sent_at DESC
        """,
        (month_key,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_communication_rate_summary(month_key):
    conn = get_conn()
    sent = conn.execute(
        "SELECT COUNT(*) FROM communication_survey_sends WHERE month_key = ?",
        (month_key,),
    ).fetchone()[0]
    answered = conn.execute(
        "SELECT COUNT(*) FROM communication_ratings WHERE month_key = ?",
        (month_key,),
    ).fetchone()[0]
    avg_row = conn.execute(
        "SELECT ROUND(AVG(score), 1) FROM communication_ratings WHERE month_key = ?",
        (month_key,),
    ).fetchone()
    conn.close()
    return {
        "month_key": month_key,
        "sent": sent,
        "answered": answered,
        "pending": max(sent - answered, 0),
        "average_score": avg_row[0] if avg_row and avg_row[0] is not None else 0,
    }
