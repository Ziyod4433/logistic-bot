import html
import os
import re
import secrets
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent
APP_DATA_DIR = Path(
    os.getenv("APP_DATA_DIR")
    or os.getenv("RAILWAY_VOLUME_MOUNT_PATH")
    or str(BASE_DIR)
)
APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = os.getenv("DB_PATH") or str(APP_DATA_DIR / "logistic.db")
PUBLIC_BASE_URL = (os.getenv("PUBLIC_BASE_URL") or os.getenv("WEBHOOK_BASE_URL") or "").rstrip("/")
try:
    TASHKENT_TZ = ZoneInfo("Asia/Tashkent")
except Exception:
    TASHKENT_TZ = timezone(timedelta(hours=5))

STATUSES = [
    "Xitoy",
    "Yiwu",
    "Zhongshan",
    "Horgos (Qozoq)",
    "Nurjo'li",
    "Jarkent",
    "Almata",
    "Taraz",
    "Shimkent",
    "Qonusbay",
    "Saryagash",
    "Yallama",
    "Toshkent",
    "Kashgar (Qirg'iz)",
    "Irkeshtam",
    "Osh",
    "Dostlik",
    "Andijon",
    "Доставлен",
]

PROBLEM_TYPES = {
    "damage": "Повреждение",
    "delay": "Опоздание",
    "shortage": "Недостача",
    "other": "Другое",
}

LEGACY_DEFAULT_TEMPLATE = """🗓 Дата загрузки: {batch_name}
📦 BL код: {bl_code}
👤 Клиент: {client_name}

📍 Текущий статус: {status}

{status_detail}

---
По вопросам обращайтесь к вашему менеджеру."""

DEFAULT_TEMPLATE = """📦 Sizning yukingiz bo‘yicha yangilangan treking ma’lumotlari:

━━━━━━━━━━━━━━━━━━━

🚛 Partiya: {batch_date}
🆔 BL-kod: {bl_code}

📍 Joriy holati:
-{status}

⏳{arrival_eta_label}:
-{arrival_eta}
━━━━━━━━━━━━━━━━━━━
📄 Yuk haqida ma'lumotlar:
{cargo_info}

━━━━━━━━━━━━━━━━━━━
👨‍💼 Ma'sul menejer:
Ziyodilla
📞 +998 95 975 66 11
📲 @Ziyodilla_Tracking_Manager
━━━━━━━━━━━━━━━━━━━

🖇Tovar bo'yicha packing list⤵️
{packing_list}"""

DEFAULT_COMMUNICATION_RATE_TEMPLATE = """Опрос за {month_key}

Пожалуйста, оцени работу менеджера по коммуникации для клиента <b>{client_name}</b>.

Шкала:
<b>YOMON</b> — плохо
<b>O'RTA</b> — средне
<b>YAXSHI</b> — хорошо
<b>ALO</b> — отлично

Эта оценка не будет показана в группе. Её увидит только админ панели."""

DEFAULT_STATUS_DETAILS = {
    "Xitoy": "🇨🇳 Yuk Xitoydagi jo'nash nuqtasida tayyorlanmoqda va marshrutga chiqarilmoqda.",
    "Yiwu": "🏭 Yuk Yiwu omboridan jo'natishga tayyorlanmoqda.",
    "Zhongshan": "🏭 Yuk Zhongshan omboridan jo'natishga tayyorlanmoqda.",
    "Horgos (Qozoq)": "🛃 Yuk Horgos orqali Qozoq yo'nalishiga kirdi. Chegara va bojxona jarayoni ketmoqda.",
    "Nurjo'li": "🚛 Yuk Nurjo'li hududidan o'tmoqda.",
    "Jarkent": "🚛 Yuk Jarkent hududidan o'tmoqda.",
    "Almata": "🏙 Yuk Almata shahriga yetib keldi yoki shu yo'nalishda harakatlanmoqda.",
    "Taraz": "🚛 Yuk Taraz yo'nalishida harakatlanmoqda.",
    "Shimkent": "🚛 Yuk Shimkent yo'nalishida harakatlanmoqda.",
    "Qonusbay": "🚛 Yuk Qonusbay nazorat nuqtasi orqali o'tmoqda.",
    "Saryagash": "🚛 Yuk Saryagash yo'nalishida harakatlanmoqda.",
    "Yallama": "🛃 Yuk Yallama chegara nuqtasiga yaqinlashdi yoki u yerdan o'tmoqda.",
    "Toshkent": "📦 Yuk Toshkentga yetib keldi. Bojxona rasmiylashtirish ishlari boshlanmoqda.",
    "Kashgar (Qirg'iz)": "🛃 Yuk Kashgar orqali Qirg'iz yo'nalishiga kirdi. Yo'l haydovchi tomonidan tanlangan marshrut bo'yicha davom etmoqda.",
    "Irkeshtam": "🛃 Yuk Irkeshtam orqali Qirg'iz yo'nalishida o'tmoqda.",
    "Osh": "🚛 Yuk Osh yo'nalishida harakatlanmoqda.",
    "Dostlik": "🛃 Yuk Dostlik chegara nuqtasiga yaqinlashdi yoki u yerdan o'tmoqda.",
    "Andijon": "🚛 Yuk Andijon yo'nalishida harakatlanmoqda.",
    "Доставлен": "✅ Yuk muvaffaqiyatli topshirildi.",
}

ETA_DESTINATION_LABELS = {
    "Toshkent": "Toshkentga yetib borish vaqti",
    "Horgos (Qozoq)": "Horgosga yetib borish vaqti",
}

DEFAULT_ETA_DESTINATION = "Toshkent"

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


def _delay_days_sql(alias: str = "bl") -> str:
    return f"""
    CASE
        WHEN COALESCE({alias}.expected_date, '') = '' THEN 0
        WHEN COALESCE({alias}.actual_date, '') != ''
             AND date({alias}.actual_date) > date({alias}.expected_date)
            THEN CAST(julianday(date({alias}.actual_date)) - julianday(date({alias}.expected_date)) AS INTEGER)
        WHEN COALESCE({alias}.actual_date, '') = ''
             AND {alias}.status != 'Доставлен'
             AND date('now','localtime') > date({alias}.expected_date)
            THEN CAST(julianday(date('now','localtime')) - julianday(date({alias}.expected_date)) AS INTEGER)
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


def _normalize_eta_destination(value: str) -> str:
    normalized = (value or "").strip()
    if normalized in ETA_DESTINATION_LABELS:
        return normalized
    return DEFAULT_ETA_DESTINATION


def _eta_destination_label(value: str) -> str:
    return ETA_DESTINATION_LABELS.get(
        _normalize_eta_destination(value),
        ETA_DESTINATION_LABELS[DEFAULT_ETA_DESTINATION],
    )


def init_db():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.executescript(
        """
        CREATE TABLE IF NOT EXISTS batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            status TEXT DEFAULT 'Xitoy',
            expected_date TEXT DEFAULT '',
            actual_date TEXT DEFAULT '',
            eta_to_toshkent TEXT DEFAULT '',
            eta_destination TEXT DEFAULT 'Toshkent',
            status_updated_at TEXT DEFAULT (datetime('now','localtime')),
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS bl_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
            code TEXT NOT NULL,
            client_name TEXT DEFAULT '',
            chat_id TEXT DEFAULT '',
            status TEXT DEFAULT 'Xitoy',
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
            public_token TEXT NOT NULL DEFAULT '',
            command_alias TEXT NOT NULL DEFAULT '',
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

        CREATE TABLE IF NOT EXISTS communication_survey_dispatches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month_key TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            client_name TEXT NOT NULL DEFAULT '',
            bl_id INTEGER REFERENCES bl_codes(id) ON DELETE SET NULL,
            batch_id INTEGER REFERENCES batches(id) ON DELETE SET NULL,
            batch_name TEXT DEFAULT '',
            sent_at TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS communication_ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month_key TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            client_name TEXT NOT NULL DEFAULT '',
            bl_id INTEGER REFERENCES bl_codes(id) ON DELETE SET NULL,
            batch_id INTEGER REFERENCES batches(id) ON DELETE SET NULL,
            voter_user_id TEXT NOT NULL DEFAULT '',
            voter_name TEXT NOT NULL DEFAULT '',
            voter_username TEXT NOT NULL DEFAULT '',
            score INTEGER NOT NULL,
            submitted_at TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(month_key, chat_id)
        );

        CREATE TABLE IF NOT EXISTS communication_rating_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dispatch_id INTEGER REFERENCES communication_survey_dispatches(id) ON DELETE CASCADE,
            month_key TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            client_name TEXT NOT NULL DEFAULT '',
            bl_id INTEGER REFERENCES bl_codes(id) ON DELETE SET NULL,
            batch_id INTEGER REFERENCES batches(id) ON DELETE SET NULL,
            voter_user_id TEXT NOT NULL DEFAULT '',
            voter_name TEXT NOT NULL DEFAULT '',
            voter_username TEXT NOT NULL DEFAULT '',
            score INTEGER NOT NULL,
            submitted_at TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS communication_rate_template (
            id INTEGER PRIMARY KEY,
            content TEXT NOT NULL,
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS login_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL DEFAULT '',
            role TEXT NOT NULL DEFAULT '',
            success INTEGER NOT NULL DEFAULT 1,
            ip_address TEXT NOT NULL DEFAULT '',
            user_agent TEXT NOT NULL DEFAULT '',
            logged_at TEXT NOT NULL DEFAULT ''
        );
        """
    )

    batch_columns = [
        ("status", "TEXT DEFAULT 'Xitoy'"),
        ("expected_date", "TEXT DEFAULT ''"),
        ("actual_date", "TEXT DEFAULT ''"),
        ("eta_to_toshkent", "TEXT DEFAULT ''"),
        ("eta_destination", "TEXT DEFAULT 'Toshkent'"),
        ("status_updated_at", "TEXT DEFAULT ''"),
    ]
    for column_name, column_def in batch_columns:
        if not _table_has_column(conn, "batches", column_name):
            conn.execute(f"ALTER TABLE batches ADD COLUMN {column_name} {column_def}")

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

    file_columns = [
        ("public_token", "TEXT NOT NULL DEFAULT ''"),
        ("command_alias", "TEXT NOT NULL DEFAULT ''"),
    ]
    for column_name, column_def in file_columns:
        if not _table_has_column(conn, "files", column_name):
            conn.execute(f"ALTER TABLE files ADD COLUMN {column_name} {column_def}")

    communication_rating_columns = [
        ("voter_user_id", "TEXT NOT NULL DEFAULT ''"),
        ("voter_name", "TEXT NOT NULL DEFAULT ''"),
        ("voter_username", "TEXT NOT NULL DEFAULT ''"),
    ]
    for column_name, column_def in communication_rating_columns:
        if not _table_has_column(conn, "communication_ratings", column_name):
            conn.execute(f"ALTER TABLE communication_ratings ADD COLUMN {column_name} {column_def}")

    conn.execute(
        """
        UPDATE bl_codes
        SET status_updated_at = COALESCE(NULLIF(status_updated_at, ''), created_at, datetime('now','localtime'))
        WHERE status_updated_at IS NULL OR status_updated_at = ''
        """
    )

    conn.execute(
        """
        UPDATE batches
        SET status = COALESCE(
                NULLIF(status, ''),
                (
                    SELECT status
                    FROM bl_codes bl
                    WHERE bl.batch_id = batches.id AND COALESCE(bl.status, '') != ''
                    ORDER BY COALESCE(NULLIF(bl.status_updated_at, ''), bl.created_at) DESC
                    LIMIT 1
                ),
                'Xitoy'
            ),
            expected_date = COALESCE(
                NULLIF(expected_date, ''),
                (
                    SELECT expected_date
                    FROM bl_codes bl
                    WHERE bl.batch_id = batches.id AND COALESCE(bl.expected_date, '') != ''
                    ORDER BY bl.created_at DESC
                    LIMIT 1
                ),
                ''
            ),
            actual_date = COALESCE(
                NULLIF(actual_date, ''),
                (
                    SELECT actual_date
                    FROM bl_codes bl
                    WHERE bl.batch_id = batches.id AND COALESCE(bl.actual_date, '') != ''
                    ORDER BY bl.created_at DESC
                    LIMIT 1
                ),
                ''
            ),
            status_updated_at = COALESCE(
                NULLIF(status_updated_at, ''),
                (
                    SELECT COALESCE(NULLIF(bl.status_updated_at, ''), bl.created_at)
                    FROM bl_codes bl
                    WHERE bl.batch_id = batches.id
                    ORDER BY COALESCE(NULLIF(bl.status_updated_at, ''), bl.created_at) DESC
                    LIMIT 1
                ),
                created_at,
                datetime('now','localtime')
            )
        """
    )

    conn.execute(
        """
        UPDATE bl_codes
        SET
            status = COALESCE(
                (SELECT b.status FROM batches b WHERE b.id = bl_codes.batch_id),
                status,
                'Xitoy'
            ),
            expected_date = COALESCE(
                (SELECT b.expected_date FROM batches b WHERE b.id = bl_codes.batch_id),
                ''
            ),
            actual_date = COALESCE(
                (SELECT b.actual_date FROM batches b WHERE b.id = bl_codes.batch_id),
                ''
            ),
            status_updated_at = COALESCE(
                (SELECT b.status_updated_at FROM batches b WHERE b.id = bl_codes.batch_id),
                status_updated_at,
                created_at,
                datetime('now','localtime')
            )
        WHERE EXISTS (SELECT 1 FROM batches b WHERE b.id = bl_codes.batch_id)
        """
    )

    file_rows = conn.execute(
        "SELECT id, filename FROM files WHERE public_token IS NULL OR public_token = '' OR command_alias IS NULL OR command_alias = ''"
    ).fetchall()
    for row in file_rows:
        public_token = _generate_public_token()
        command_alias = _generate_command_alias(row["filename"], row["id"])
        conn.execute(
            "UPDATE files SET public_token = COALESCE(NULLIF(public_token, ''), ?), command_alias = COALESCE(NULLIF(command_alias, ''), ?) WHERE id = ?",
            (public_token, command_alias, row["id"]),
        )

    row = cursor.execute("SELECT id, content FROM message_template WHERE id = 1").fetchone()
    if not row:
        cursor.execute("INSERT INTO message_template(id, content) VALUES(1, ?)", (DEFAULT_TEMPLATE,))
    else:
        current_template = (row["content"] or "").strip()
        if not current_template or current_template == LEGACY_DEFAULT_TEMPLATE.strip():
            cursor.execute(
                """
                UPDATE message_template
                SET content = ?, updated_at = datetime('now','localtime')
                WHERE id = 1
                """,
                (DEFAULT_TEMPLATE,),
            )

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

    legacy_status_map = {
        "Принят": "Xitoy",
        "Хоргос": "Horgos (Qozoq)",
        "Алматы": "Almata",
        "В пути до Ташкента": "Yallama",
        "Ташкент": "Toshkent",
        "Altynko'l": "Nurjo'li",
        "Chuqur": "Dostlik",
        "Chuqursoy": "Dostlik",
    }
    for old_status, new_status in legacy_status_map.items():
        cursor.execute("UPDATE batches SET status = ? WHERE status = ?", (new_status, old_status))
        cursor.execute("UPDATE bl_codes SET status = ? WHERE status = ?", (new_status, old_status))

    conn.commit()
    conn.close()


def create_batch(name, status="Xitoy", eta_to_toshkent="", eta_destination="Toshkent"):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO batches(name, status, expected_date, actual_date, eta_to_toshkent, eta_destination, status_updated_at) VALUES(?, ?, '', '', ?, ?, datetime('now','localtime'))",
            (
                (name or "").strip(),
                (status or "Xitoy").strip(),
                (eta_to_toshkent or "").strip(),
                _normalize_eta_destination(eta_destination),
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def update_batch(batch_id, name, status="Xitoy", eta_to_toshkent="", eta_destination="Toshkent"):
    conn = get_conn()
    try:
        new_status = (status or "Xitoy").strip()
        current_batch = conn.execute(
            "SELECT expected_date, actual_date FROM batches WHERE id = ?",
            (batch_id,),
        ).fetchone()
        expected_date = (current_batch["expected_date"] if current_batch else "") or ""
        actual_date = (current_batch["actual_date"] if current_batch else "") or ""
        conn.execute(
            """
            UPDATE batches
            SET
                name = ?,
                status = ?,
                eta_to_toshkent = ?,
                eta_destination = ?,
                status_updated_at = CASE
                    WHEN COALESCE(status, 'Xitoy') != ? THEN datetime('now','localtime')
                    ELSE COALESCE(NULLIF(status_updated_at, ''), datetime('now','localtime'))
                END
            WHERE id = ?
            """,
            (
                (name or "").strip(),
                new_status,
                (eta_to_toshkent or "").strip(),
                _normalize_eta_destination(eta_destination),
                new_status,
                batch_id,
            ),
        )
        conn.execute(
            """
            UPDATE bl_codes
            SET
                status = ?,
                status_updated_at = CASE
                    WHEN COALESCE(status, 'Xitoy') != ? THEN datetime('now','localtime')
                    ELSE COALESCE(NULLIF(status_updated_at, ''), datetime('now','localtime'))
                END
            WHERE batch_id = ?
            """,
            (
                new_status,
                new_status,
                batch_id,
            ),
        )
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
            {_delay_days_sql('b')} AS delay_days,
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
    row = conn.execute(
        f"""
        SELECT
            b.*,
            {_delay_days_sql('b')} AS delay_days,
            (
                SELECT MAX(sl.sent_at)
                FROM send_logs sl
                JOIN bl_codes bl ON bl.id = sl.bl_id
                WHERE bl.batch_id = b.id
                  AND sl.success = 1
            ) AS last_tracking_at
        FROM batches b
        WHERE b.id = ?
        """,
        (batch_id,),
    ).fetchone()
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


def current_ts():
    return datetime.now(TASHKENT_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _generate_public_token():
    return secrets.token_urlsafe(24)


def _slugify_file_command(filename: str) -> str:
    stem = (filename or "").strip()
    if "." in stem:
        stem = stem.rsplit(".", 1)[0]
    stem = stem.replace("&", " and ")
    stem = re.sub(r"[^A-Za-z0-9]+", "_", stem).strip("_").lower()
    stem = re.sub(r"_+", "_", stem)
    return stem[:28] if stem else "file"


def _generate_command_alias(filename: str, file_id: int) -> str:
    base = _slugify_file_command(filename)
    return f"{base}_{file_id}"


def prettify_file_name(filename: str) -> str:
    value = (filename or "").strip()
    if "." in value:
        value = value.rsplit(".", 1)[0]
    value = value.replace("_", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value or "Fayl"


def record_login_history(username, role="", success=True, ip_address="", user_agent=""):
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO login_history(username, role, success, ip_address, user_agent, logged_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                (username or "").strip(),
                (role or "").strip(),
                1 if success else 0,
                (ip_address or "").strip()[:120],
                (user_agent or "").strip()[:255],
                current_ts(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_login_history(limit=200):
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT id, username, role, success, ip_address, user_agent, logged_at
        FROM login_history
        ORDER BY logged_at DESC, id DESC
        LIMIT ?
        """,
        (_to_int(limit) or 200,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def format_cargo_info(bl: dict) -> str:
    parts = []
    cargo_type = (bl.get("cargo_type") or "").strip()
    if cargo_type:
        parts.append(f"• Tovar turi: {html.escape(cargo_type, quote=False)}")

    weight = _to_float(bl.get("weight_kg"))
    if weight:
        parts.append(f"• Og'irligi: {weight:g} kg")

    volume = _to_float(bl.get("volume_cbm"))
    if volume:
        parts.append(f"• Hajmi: {volume:g} m³")

    quantity = _to_int(bl.get("quantity_places"))
    if quantity:
        parts.append(f"• Joylar soni: {quantity}")

    description = (bl.get("cargo_description") or "").strip()
    if description:
        parts.append(f"• Tavsif: {html.escape(description, quote=False)}")

    return "\n".join(parts)


class _TemplateContext(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def _inject_cargo_info_placeholder(template: str) -> str:
    if "{cargo_info}" in template:
        return template

    for anchor in ("{batch_date}", "{batch_name}"):
        anchor_pos = template.find(anchor)
        if anchor_pos != -1:
            line_end = template.find("\n", anchor_pos)
            insert_at = len(template) if line_end == -1 else line_end + 1
            return template[:insert_at] + "\n🕯 Yuk haqida ma'lumot:\n{cargo_info}\n" + template[insert_at:]

    status_pos = template.find("{status}")
    if status_pos != -1:
        line_start = template.rfind("\n", 0, status_pos)
        insert_at = 0 if line_start == -1 else line_start + 1
        return template[:insert_at] + "🕯 Yuk haqida ma'lumot:\n{cargo_info}\n\n" + template[insert_at:]

    return template + "\n\n🕯 Yuk haqida ma'lumot:\n{cargo_info}"


def _inject_bl_code_placeholder(template: str) -> str:
    if "{bl_code}" in template:
        return template

    template = (template or "").replace("\r\n", "\n")
    patterns = [
        r"(🚛\s*Partiya:\s*[^\n]+)",
        r"(📌\s*Partiya:\s*[^\n]+)",
    ]
    for pattern in patterns:
        updated = re.sub(pattern, r"\1\n🆔 BL-kod: {bl_code}", template, count=1)
        if updated != template:
            return updated
    return template


def _normalize_client_template(template: str) -> str:
    text = (template or "").replace("\r\n", "\n").strip()
    legacy_markers = [
        "Assalomu alaykum hurmatli mijoz",
        "Assalomu alaykum, hurmatli mijoz",
        "Quyida yukingiz bo'yicha treking ma'lumotlari keltirilgan",
        "Quyida yukingiz bo‘yicha treking ma’lumotlari keltirilgan",
        "Hozirgi holati:",
        "Yuk haqida ma'lumot:",
        "Yuk haqida ma’lumot:",
    ]
    if any(marker in text for marker in legacy_markers):
        return DEFAULT_TEMPLATE
    return text or DEFAULT_TEMPLATE


def _inject_arrival_eta_placeholder(template: str) -> str:
    template = (template or "").replace("\r\n", "\n")
    regex_replacements = [
        (r"🇺🇿\s*Yetib kelish vaqti\s*:\s*\{arrival_eta\}", "🇺🇿 {arrival_eta_label}: {arrival_eta}"),
        (r"🇺🇿\s*Yetib kelish vaqti\s*:\s*\{expected_date\}", "🇺🇿 {arrival_eta_label}: {arrival_eta}"),
        (r"🖥\s*Kutilayotgan sana\s*:\s*\{expected_date\}", "🇺🇿 {arrival_eta_label}: {arrival_eta}"),
    ]
    for pattern, target in regex_replacements:
        updated = re.sub(pattern, target, template, count=1)
        if updated != template:
            return updated
    return template


def _inject_packing_list_placeholder(template: str) -> str:
    if "{packing_list}" in template or "{bl_files}" in template:
        return _move_packing_list_placeholder_to_end(template)
    variants = [
        ("📎 Tovar bo'yicha: Packing list", "📎 Tovar bo'yicha: {packing_list}"),
        ("📎Tovar bo'yicha: Packing list", "📎Tovar bo'yicha: {packing_list}"),
        ("📎 Tovar bo'yicha:Packing list", "📎 Tovar bo'yicha:{packing_list}"),
        ("📎Tovar bo'yicha:Packing list", "📎Tovar bo'yicha:{packing_list}"),
        ("🖇Tovar bo'yicha packing list⤵️", "🖇Tovar bo'yicha packing list⤵️\n{packing_list}"),
        ("🖇 Tovar bo'yicha packing list ⤵️", "🖇Tovar bo'yicha packing list⤵️\n{packing_list}"),
    ]
    for source, target in variants:
        if source in template:
            template = template.replace(source, target, 1)
            return _move_packing_list_placeholder_to_end(template)
    if "Packing list" in template:
        template = template.replace("Packing list", "{packing_list}", 1)
        return _move_packing_list_placeholder_to_end(template)
    return _move_packing_list_placeholder_to_end(template + "\n\n🖇Tovar bo'yicha packing list⤵️\n{packing_list}")


def _move_packing_list_placeholder_to_end(template: str) -> str:
    template = (template or "").replace("\r\n", "\n")
    lines = template.split("\n")
    kept_lines = []
    packing_lines = []
    capture_following = False

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()
        if "{packing_list}" in stripped or "{bl_files}" in stripped:
            capture_following = True
            continue
        if capture_following and stripped.startswith("•"):
            continue
        if capture_following and not stripped:
            capture_following = False
            continue
        if capture_following:
            capture_following = False
        kept_lines.append(line)

    while kept_lines and not kept_lines[-1].strip():
        kept_lines.pop()

    kept_lines.append("")
    kept_lines.append("")
    kept_lines.append("🖇Tovar bo'yicha packing list⤵️")
    kept_lines.append("{packing_list}")
    return "\n".join(kept_lines)


def _normalize_template_value(value):
    if value is None:
        return ""
    return html.escape(str(value), quote=False)


def add_bl(
    batch_id,
    code,
    client_name="",
    chat_id="",
    cargo_type="",
    weight_kg=0,
    volume_cbm=0,
    quantity_places=0,
    cargo_description="",
):
    conn = get_conn()
    try:
        batch_row = conn.execute(
            "SELECT status, expected_date, actual_date, status_updated_at FROM batches WHERE id = ?",
            (batch_id,),
        ).fetchone()
        batch_status = batch_row["status"] if batch_row else "Xitoy"
        expected_date = batch_row["expected_date"] if batch_row else ""
        actual_date = batch_row["actual_date"] if batch_row else ""
        status_updated_at = (
            batch_row["status_updated_at"]
            if batch_row and batch_row["status_updated_at"]
            else current_ts()
        )
        conn.execute(
            """
            INSERT INTO bl_codes(
                batch_id, code, client_name, chat_id, status, cargo_type, weight_kg, volume_cbm, quantity_places,
                cargo_description, expected_date, actual_date, status_updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                batch_id,
                code.upper().strip(),
                client_name.strip(),
                chat_id.strip(),
                batch_status,
                (cargo_type or "").strip(),
                _to_float(weight_kg),
                _to_float(volume_cbm),
                _to_int(quantity_places),
                (cargo_description or "").strip(),
                (expected_date or "").strip(),
                (actual_date or "").strip(),
                status_updated_at,
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
    status=None,
    cargo_type="",
    weight_kg=0,
    volume_cbm=0,
    quantity_places=0,
    cargo_description="",
):
    conn = get_conn()
    current = conn.execute(
        "SELECT status FROM bl_codes WHERE id = ?",
        (bl_id,),
    ).fetchone()
    effective_status = (status if status is not None else (current["status"] if current else "Xitoy")) or "Xitoy"
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
            status_updated_at = CASE
                WHEN status != ? THEN datetime('now','localtime')
                ELSE status_updated_at
            END
        WHERE id = ?
        """,
        (
            client_name.strip(),
            chat_id.strip(),
            effective_status,
            (cargo_type or "").strip(),
            _to_float(weight_kg),
            _to_float(volume_cbm),
            _to_int(quantity_places),
            (cargo_description or "").strip(),
            effective_status,
            bl_id,
        ),
    )
    conn.commit()
    conn.close()


def move_bl_to_batch(bl_id, target_batch_id):
    conn = get_conn()
    try:
        bl_row = conn.execute(
            "SELECT id, batch_id, code FROM bl_codes WHERE id = ?",
            (bl_id,),
        ).fetchone()
        if not bl_row:
            raise ValueError("BL не найден")

        current_batch_id = int(bl_row["batch_id"])
        target_batch_id = int(target_batch_id)
        if current_batch_id == target_batch_id:
            raise ValueError("BL уже находится в этой партии")

        target_batch = conn.execute(
            """
            SELECT id, name, status, expected_date, actual_date, status_updated_at
            FROM batches
            WHERE id = ?
            """,
            (target_batch_id,),
        ).fetchone()
        if not target_batch:
            raise ValueError("Целевая партия не найдена")

        duplicate = conn.execute(
            """
            SELECT 1
            FROM bl_codes
            WHERE batch_id = ? AND UPPER(code) = UPPER(?) AND id != ?
            LIMIT 1
            """,
            (target_batch_id, bl_row["code"], bl_id),
        ).fetchone()
        if duplicate:
            raise ValueError("В целевой партии уже есть такой BL код")

        target_status_updated_at = (
            target_batch["status_updated_at"] if target_batch["status_updated_at"] else current_ts()
        )

        conn.execute(
            """
            UPDATE bl_codes
            SET
                batch_id = ?,
                status = ?,
                expected_date = ?,
                actual_date = ?,
                status_updated_at = ?
            WHERE id = ?
            """,
            (
                target_batch_id,
                target_batch["status"] or "Xitoy",
                (target_batch["expected_date"] or "").strip(),
                (target_batch["actual_date"] or "").strip(),
                target_status_updated_at,
                bl_id,
            ),
        )

        conn.execute(
            "UPDATE problems SET batch_id = ? WHERE bl_id = ?",
            (target_batch_id, bl_id),
        )

        conn.commit()
        return {
            "bl_id": bl_id,
            "code": bl_row["code"],
            "from_batch_id": current_batch_id,
            "to_batch_id": target_batch_id,
            "to_batch_name": target_batch["name"],
        }
    finally:
        conn.close()


def delete_bl(bl_id):
    conn = get_conn()
    try:
        file_rows = conn.execute(
            "SELECT id, file_path FROM files WHERE bl_id = ?",
            (bl_id,),
        ).fetchall()

        conn.execute("UPDATE send_logs SET bl_id = NULL WHERE bl_id = ?", (bl_id,))
        conn.execute("UPDATE communication_survey_sends SET bl_id = NULL WHERE bl_id = ?", (bl_id,))
        conn.execute("UPDATE communication_survey_dispatches SET bl_id = NULL WHERE bl_id = ?", (bl_id,))
        conn.execute("UPDATE communication_ratings SET bl_id = NULL WHERE bl_id = ?", (bl_id,))
        conn.execute("UPDATE communication_rating_events SET bl_id = NULL WHERE bl_id = ?", (bl_id,))

        conn.execute("DELETE FROM problems WHERE bl_id = ?", (bl_id,))
        conn.execute("DELETE FROM files WHERE bl_id = ?", (bl_id,))
        conn.execute("DELETE FROM bl_codes WHERE id = ?", (bl_id,))
        conn.commit()

        for row in file_rows:
            file_path = row["file_path"]
            if not file_path:
                continue
            try:
                os.remove(file_path)
            except OSError:
                pass
    finally:
        conn.close()


def add_file(bl_id, filename, file_path):
    conn = get_conn()
    cursor = conn.execute(
        "INSERT INTO files(bl_id, filename, file_path, public_token, command_alias) VALUES(?, ?, ?, ?, '')",
        (bl_id, filename, file_path, _generate_public_token()),
    )
    file_id = cursor.lastrowid
    conn.execute(
        "UPDATE files SET command_alias = ? WHERE id = ?",
        (_generate_command_alias(filename, file_id), file_id),
    )
    conn.commit()
    conn.close()


def get_files(bl_id):
    conn = get_conn()
    rows = conn.execute("SELECT * FROM files WHERE bl_id = ?", (bl_id,)).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_file_by_public_token(public_token):
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM files WHERE public_token = ? LIMIT 1",
        ((public_token or "").strip(),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_file_by_id(file_id):
    conn = get_conn()
    row = conn.execute("SELECT * FROM files WHERE id = ?", (file_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_file_by_command_alias(command_alias):
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM files WHERE command_alias = ? LIMIT 1",
        (((command_alias or "").strip().lstrip("/").lower()),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def format_packing_list(bl_id) -> str:
    files = get_files(bl_id)
    if not files:
        return "Packing list biriktirilmagan"
    items = []
    for file_info in files:
        name = (file_info.get("filename") or "").strip()
        if not name:
            continue
        items.append(f"• {html.escape(prettify_file_name(name))}")
    if not items:
        return "Packing list biriktirilmagan"
    return "\n".join(items)


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
    sent_at = current_ts()
    conn.execute(
        """
        INSERT INTO send_logs(bl_id, bl_code, batch_name, chat_id, status, success, error_msg, sent_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (bl_id, bl_code, batch_name, chat_id, status, 1 if success else 0, error_msg, sent_at),
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
        "delivered": conn.execute("SELECT COUNT(*) FROM batches WHERE status = 'Доставлен'").fetchone()[0],
        "late": conn.execute(f"SELECT COUNT(*) FROM batches b WHERE {_late_sql('b')} = 1").fetchone()[0],
        "delay_days_total": conn.execute(
            f"SELECT COALESCE(SUM({_delay_days_sql('b')}), 0) FROM batches b"
        ).fetchone()[0],
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


def _message_status_label(status: str) -> str:
    value = (status or "").strip()
    if value in {"Yiwu", "Zhongshan"}:
        return f"{value} omborimizdan yo'lga chiqib ketdi"
    return value


def render_message(bl: dict, batch_name: str) -> str:
    template = _inject_packing_list_placeholder(
        _inject_arrival_eta_placeholder(
            _inject_cargo_info_placeholder(
                _inject_bl_code_placeholder(
                    _normalize_client_template(get_template())
                )
            )
        )
    )
    template = template.replace("\r\n", "\n")
    template = template.replace("\n\n{status_detail}", "")
    template = template.replace("{status_detail}\n\n", "")
    template = template.replace("{status_detail}", "")
    status = bl.get("status", "Xitoy")
    cargo_type = (bl.get("cargo_type") or "").strip()
    weight_value = _to_float(bl.get("weight_kg"))
    volume_value = _to_float(bl.get("volume_cbm"))
    places_value = _to_int(bl.get("quantity_places"))
    description = (bl.get("cargo_description") or "").strip()
    expected_date = (bl.get("expected_date") or "").strip()
    actual_date = (bl.get("actual_date") or "").strip()
    packing_list = format_packing_list(bl.get("id"))
    batch = get_batch(bl.get("batch_id")) if bl.get("batch_id") else None
    arrival_eta = ((batch or {}).get("eta_to_toshkent") or "").strip()
    arrival_eta_value = arrival_eta or expected_date
    arrival_eta_label = _eta_destination_label((batch or {}).get("eta_destination") or "")

    context = _TemplateContext(
        batch_name=_normalize_template_value(batch_name),
        batch_date=_normalize_template_value(batch_name),
        bl_code=_normalize_template_value(bl.get("code", "")),
        client_name=_normalize_template_value(bl.get("client_name", "")),
        status=_normalize_template_value(_message_status_label(status)),
        cargo_info=format_cargo_info(bl),
        cargo_type=_normalize_template_value(cargo_type),
        weight_kg=_normalize_template_value(f"{weight_value:g}" if weight_value else ""),
        volume_cbm=_normalize_template_value(f"{volume_value:g}" if volume_value else ""),
        volume_m3=_normalize_template_value(f"{volume_value:g}" if volume_value else ""),
        quantity_places=_normalize_template_value(places_value if places_value else ""),
        places=_normalize_template_value(places_value if places_value else ""),
        cargo_description=_normalize_template_value(description),
        description=_normalize_template_value(description),
        expected_date=_normalize_template_value(arrival_eta_value),
        arrival_eta=_normalize_template_value(arrival_eta_value),
        arrival_eta_label=_normalize_template_value(arrival_eta_label),
        actual_date=_normalize_template_value(actual_date),
        packing_list=_normalize_template_value(packing_list),
        bl_files=_normalize_template_value(packing_list),
        status_detail="",
    )
    rendered = template.format_map(context)
    rendered = re.sub(
        r"🇺🇿\s*Yetib kelish vaqti\s*:",
        f"🇺🇿 {arrival_eta_label}:",
        rendered,
        count=1,
    )
    rendered = re.sub(r"\n{3,}", "\n\n", rendered)
    rendered = re.sub(
        r"\n+(🖇\s*Tovar bo'yicha packing list\s*⤵️|🖇Tovar bo'yicha packing list⤵️)",
        r"\n\n\1",
        rendered,
        count=1,
    )
    return rendered.strip()


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

    created_at = current_ts()
    conn.execute(
        """
        INSERT INTO problems(bl_id, batch_id, problem_type, description, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, 'open', ?, ?)
        """,
        (bl["id"], bl["batch_id"], problem_type, (description or "").strip(), created_at, created_at),
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
            p.created_at AS incident_detected_at,
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
    return datetime.now(TASHKENT_TZ).strftime("%Y-%m")


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
            COALESCE(NULLIF(TRIM(tc.title), ''), NULLIF(TRIM(bl.client_name), ''), bl.code) AS client_name,
            b.name AS batch_name
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        LEFT JOIN telegram_chats tc ON tc.chat_id = bl.chat_id
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
            c.chat_id,
            COALESCE(
                NULLIF(TRIM(c.title), ''),
                NULLIF(TRIM(c.username), ''),
                c.chat_id
            ) AS client_name,
            c.title AS chat_title,
            c.chat_type,
            c.username,
            c.last_seen_at,
            (
                SELECT bl2.id
                FROM bl_codes bl2
                WHERE bl2.chat_id = c.chat_id
                ORDER BY bl2.created_at DESC
                LIMIT 1
            ) AS bl_id,
            (
                SELECT bl2.batch_id
                FROM bl_codes bl2
                WHERE bl2.chat_id = c.chat_id
                ORDER BY bl2.created_at DESC
                LIMIT 1
            ) AS batch_id,
            (
                SELECT b.name
                FROM bl_codes bl2
                LEFT JOIN batches b ON b.id = bl2.batch_id
                WHERE bl2.chat_id = c.chat_id
                ORDER BY bl2.created_at DESC
                LIMIT 1
            ) AS batch_name
        FROM telegram_chats c
        WHERE c.chat_id != ''
          AND c.is_active = 1
        ORDER BY
            COALESCE(NULLIF(TRIM(c.title), ''), NULLIF(TRIM(c.username), ''), c.chat_id) COLLATE NOCASE ASC
        """
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_communication_sent_chat_ids(month_key):
    conn = get_conn()
    rows = conn.execute(
        "SELECT DISTINCT chat_id FROM communication_survey_dispatches WHERE month_key = ?",
        (month_key,),
    ).fetchall()
    conn.close()
    return {str(row["chat_id"]) for row in rows}


def record_communication_survey_send(month_key, recipient):
    conn = get_conn()
    try:
        sent_at = current_ts()
        cursor = conn.execute(
            """
            INSERT INTO communication_survey_dispatches(
                month_key, chat_id, client_name, bl_id, batch_id, batch_name, sent_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                month_key,
                str(recipient.get("chat_id", "")),
                recipient.get("client_name", ""),
                recipient.get("bl_id"),
                recipient.get("batch_id"),
                recipient.get("batch_name", ""),
                sent_at,
            ),
        )
        conn.execute(
            """
            INSERT INTO communication_survey_sends(
                month_key, chat_id, client_name, bl_id, batch_id, batch_name, sent_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(month_key, chat_id) DO UPDATE SET
                client_name = excluded.client_name,
                bl_id = excluded.bl_id,
                batch_id = excluded.batch_id,
                batch_name = excluded.batch_name,
                sent_at = excluded.sent_at
            """,
            (
                month_key,
                str(recipient.get("chat_id", "")),
                recipient.get("client_name", ""),
                recipient.get("bl_id"),
                recipient.get("batch_id"),
                recipient.get("batch_name", ""),
                sent_at,
            ),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def delete_communication_survey_dispatch(dispatch_id):
    if not dispatch_id:
        return
    conn = get_conn()
    row = conn.execute(
        "SELECT month_key, chat_id FROM communication_survey_dispatches WHERE id = ?",
        (dispatch_id,),
    ).fetchone()
    if not row:
        conn.close()
        return

    month_key = row["month_key"]
    chat_id = row["chat_id"]

    conn.execute("DELETE FROM communication_survey_dispatches WHERE id = ?", (dispatch_id,))

    latest = conn.execute(
        """
        SELECT month_key, chat_id, client_name, bl_id, batch_id, batch_name, sent_at
        FROM communication_survey_dispatches
        WHERE month_key = ? AND chat_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (month_key, chat_id),
    ).fetchone()
    if latest:
        conn.execute(
            """
            INSERT INTO communication_survey_sends(
                month_key, chat_id, client_name, bl_id, batch_id, batch_name, sent_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(month_key, chat_id) DO UPDATE SET
                client_name = excluded.client_name,
                bl_id = excluded.bl_id,
                batch_id = excluded.batch_id,
                batch_name = excluded.batch_name,
                sent_at = excluded.sent_at
            """,
            (
                latest["month_key"],
                latest["chat_id"],
                latest["client_name"],
                latest["bl_id"],
                latest["batch_id"],
                latest["batch_name"],
                latest["sent_at"],
            ),
        )
    else:
        conn.execute(
            "DELETE FROM communication_survey_sends WHERE month_key = ? AND chat_id = ?",
            (month_key, chat_id),
        )
    conn.commit()
    conn.close()


def save_communication_rating(dispatch_id, month_key, chat_id, score, voter=None):
    score_value = _to_int(score)
    if score_value < 1 or score_value > 10:
        return False

    conn = get_conn()
    if dispatch_id:
        send_row = conn.execute(
            """
            SELECT id, month_key, chat_id, client_name, bl_id, batch_id
            FROM communication_survey_dispatches
            WHERE id = ?
            """,
            (dispatch_id,),
        ).fetchone()
    else:
        send_row = conn.execute(
            """
            SELECT id, month_key, chat_id, client_name, bl_id, batch_id
            FROM communication_survey_dispatches
            WHERE month_key = ? AND chat_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (month_key, str(chat_id)),
        ).fetchone()

    recipient = dict(send_row) if send_row else _get_latest_chat_recipient(conn, chat_id)
    if not recipient:
        conn.close()
        return False

    dispatch_id = recipient.get("id")
    month_key = recipient.get("month_key") or month_key
    chat_id = recipient.get("chat_id") or chat_id
    voter = voter or {}
    voter_user_id = str(voter.get("id") or "")
    first_name = (voter.get("first_name") or "").strip()
    last_name = (voter.get("last_name") or "").strip()
    voter_name = " ".join(part for part in [first_name, last_name] if part).strip()
    voter_username = (voter.get("username") or "").strip()
    if not voter_name and voter_username:
        voter_name = f"@{voter_username}"

    submitted_at = current_ts()

    conn.execute(
        """
        INSERT INTO communication_rating_events(
            dispatch_id, month_key, chat_id, client_name, bl_id, batch_id,
            voter_user_id, voter_name, voter_username, score, submitted_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            dispatch_id,
            month_key,
            str(chat_id),
            recipient.get("client_name", ""),
            recipient.get("bl_id"),
            recipient.get("batch_id"),
            voter_user_id,
            voter_name,
            voter_username,
            score_value,
            submitted_at,
        ),
    )

    conn.execute(
        """
        INSERT INTO communication_ratings(
            month_key, chat_id, client_name, bl_id, batch_id,
            voter_user_id, voter_name, voter_username,
            score, submitted_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(month_key, chat_id) DO UPDATE SET
            client_name = excluded.client_name,
            bl_id = excluded.bl_id,
            batch_id = excluded.batch_id,
            voter_user_id = excluded.voter_user_id,
            voter_name = excluded.voter_name,
            voter_username = excluded.voter_username,
            score = excluded.score,
            submitted_at = excluded.submitted_at
        """,
        (
            month_key,
            str(chat_id),
            recipient.get("client_name", ""),
            recipient.get("bl_id"),
            recipient.get("batch_id"),
            voter_user_id,
            voter_name,
            voter_username,
            score_value,
            submitted_at,
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
            d.id AS dispatch_id,
            d.month_key,
            d.chat_id,
            COALESCE(NULLIF(TRIM(tc.title), ''), NULLIF(TRIM(d.client_name), ''), d.chat_id) AS client_name,
            d.sent_at,
            e.voter_user_id,
            e.voter_name,
            e.voter_username,
            e.score,
            e.submitted_at
        FROM communication_survey_dispatches d
        LEFT JOIN telegram_chats tc ON tc.chat_id = d.chat_id
        LEFT JOIN communication_rating_events e
            ON e.dispatch_id = d.id
        WHERE d.month_key = ?
        ORDER BY
            d.sent_at DESC,
            COALESCE(e.submitted_at, '') DESC,
            d.id DESC,
            COALESCE(e.id, 0) DESC
        """,
        (month_key,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_communication_rate_summary(month_key):
    conn = get_conn()
    sent = conn.execute(
        "SELECT COUNT(*) FROM communication_survey_dispatches WHERE month_key = ?",
        (month_key,),
    ).fetchone()[0]
    answered = conn.execute(
        "SELECT COUNT(*) FROM communication_rating_events WHERE month_key = ?",
        (month_key,),
    ).fetchone()[0]
    avg_row = conn.execute(
        "SELECT ROUND(AVG(score), 1) FROM communication_rating_events WHERE month_key = ?",
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
