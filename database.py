import hashlib
import html
import json
import os
import re
import secrets
import sqlite3
import time
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
    "Horgos",
    "Nurjo'li",
    "Jarkent",
    "Almata",
    "Taraz",
    "Shimkent",
    "Qonusbay",
    "Saryagash",
    "Yallama",
    "Toshkent(Chuqursoy ULS da)",
    "Р”РѕСЃС‚Р°РІР»РµРЅ",
    "Kashgar (Qirg'iz)",
    "Irkeshtam",
    "Osh",
    "Dostlik",
    "Andijon",
    "Доставлен",
]

LEGACY_DELIVERED_STATUS = STATUSES[-1]
DELIVERED_STATUS = "Mijozga yetkazib berildi"
STATUSES = [
    "Xitoy",
    "Yiwu",
    "Zhongshan",
    "Horgos",
    "Nurjo'li",
    "Jarkent",
    "Almata",
    "Taraz",
    "Shimkent",
    "Qonusbay",
    "Saryagash",
    "Yallama",
    "Toshkent(Chuqursoy ULS da)",
    "Kashgar (Qirg'iz)",
    "Irkeshtam",
    "Osh",
    "Dostlik",
    "Andijon",
]

PROBLEM_TYPES = {
    "damage": "Shikastlanish",
    "delay": "Kechikish",
    "shortage": "Kamomad",
    "packing_damage": "Qadoq buzilishi",
    "other": "Boshqa",
}

MESSAGE_LANGUAGES = {
    "uz_latn": "O'zbekcha (lotin)",
    "uz_cyrl": "Ўзбекча (кирилл)",
    "ru": "Русский",
    "en": "English",
}

DEFAULT_MESSAGE_LANGUAGE = "uz_latn"

LEGACY_DEFAULT_TEMPLATE = """🗓 Дата загрузки: {batch_name}
📦 BL код: {bl_code}
👤 Клиент: {client_name}

📍 Текущий статус: {status}

{status_detail}

---
По вопросам обращайтесь к вашему менеджеру."""

DEFAULT_TEMPLATE = """👋Assalomu alaykum hurmatli mijoz!

📦 Sizning yukingiz bo‘yicha yangilangan treking ma’lumotlari:⤵️
━━━━━━━━━━━━━━━
📍 Joriy holati:
-<b>{status}</b>

⏳{arrival_eta_label}:
-<b>{arrival_eta}</b>

🚛 Partiya: <b>{batch_date}</b>
🆔 BL-kod: <b>{bl_code}</b>
━━━━━━━━━━━━━━━
📄 Yuk haqida ma'lumotlar:
{cargo_info}
━━━━━━━━━━━━━━━
🖇Tovar bo'yicha packing list⤵️
{packing_list}"""

# The previous default template kept a Telegram contact block ("📲 Aloqa
# uchun:" / phone / @username) between the cargo info and the packing
# list footer. We dropped it on user request — files now arrive right
# after the message, so an embedded support contact is redundant. The
# regex below also strips an equivalent block from custom templates that
# were stored in message_template before this change.
_CONTACT_BLOCK_REGEX = re.compile(
    r"\n*"                                  # leading blank lines
    r"━+\n"                                # divider before
    r"📲[^\n]*Aloqa uchun[^\n]*\n"        # uz_latn header (still appears in russian / cyrillic forms after localization)
    r"[^\n]*-?\s*95[-\s]975[-\s]66[-\s]11[^\n]*\n"
    r"[^\n]*@Ziyodilla_Tracking_Manager[^\n]*\n"
    r"━+\n",                              # divider after
)
_CONTACT_BLOCK_REGEX_LOCALIZED = re.compile(
    r"\n*"
    r"━+\n"
    r"📲[^\n]*(?:Aloqa uchun|Алоқа учун|Для связи|Contact)[^\n]*\n"
    r"[^\n]*\d[\d\-\s]+\d[^\n]*\n"
    r"[^\n]*@\w+[^\n]*\n"
    r"━+\n",
)


def _strip_contact_block(text: str) -> str:
    """Remove the legacy 'Aloqa uchun' contact block from any tracking text."""
    if not text:
        return text
    cleaned = _CONTACT_BLOCK_REGEX.sub("\n", text)
    cleaned = _CONTACT_BLOCK_REGEX_LOCALIZED.sub("\n", cleaned)
    # Collapse any runs of empty lines this leaves behind so the layout
    # doesn't get a gaping hole between cargo info and packing-list block.
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned

DEFAULT_COMMUNICATION_RATE_TEMPLATE = """Assalomu alaykum hurmatli mijoz!

Iltimos, bizning guruh moderatorimiz ishiga 1 dan 10 gacha baho bering.

Iltimos faqat guruh moderatori ishigagina baho berishingizni soraymiz , bu hodim vazifasi , oy davomida sizga tezlik bilan masul hodimni  guruhga jalb qilib berish edi, va u qanchalik bu vazifani yaxshi uddaladi , shuni baholab berishingizni so'raymiz ? 
Sizning fikringiz biz uchun juda muhim va xizmatimiz sifatini yanada oshirishga yordam beradi.

Moderator xizmatiga qanday baho berasiz? Iltimos, quyidagi variantlardan birini tanlab baholang.
1–6 ball: xizmatdan qoniqmadim
7–8 ball: xizmat yomon emas, lekin yaxshilash kerak
9–10 ball: xizmatdan juda mamnunman, boshqalarga ham tavsiya qilaman
Oldindan tashakkur!
Buraq Logistics jamoasi."""

DEFAULT_ANNOUNCEMENT_TEMPLATE = ""

ANNOUNCEMENT_ATTACHMENT_NAME_KEY = "announcement_attachment_name"
ANNOUNCEMENT_ATTACHMENT_PATH_KEY = "announcement_attachment_path"
ANNOUNCEMENT_ATTACHMENT_KIND_KEY = "announcement_attachment_kind"
ANNOUNCEMENT_LAST_SENT_AT_KEY = "announcement_last_sent_at"

DEFAULT_STATUS_DETAILS = {
    "Xitoy": "🇨🇳 Yuk Xitoydagi jo'nash nuqtasida tayyorlanmoqda va marshrutga chiqarilmoqda.",
    "Yiwu": "🏭 Yuk Yiwu omboridan jo'natishga tayyorlanmoqda.",
    "Zhongshan": "🏭 Yuk Zhongshan omboridan jo'natishga tayyorlanmoqda.",
    "Horgos": "🛃 Yuk Horgos orqali Qozoq yo'nalishiga kirdi. Chegara va bojxona jarayoni ketmoqda.",
    "Nurjo'li": "🚛 Yuk Nurjo'li hududidan o'tmoqda.",
    "Jarkent": "🚛 Yuk Jarkent hududidan o'tmoqda.",
    "Almata": "🏙 Yuk Almata shahriga yetib keldi yoki shu yo'nalishda harakatlanmoqda.",
    "Taraz": "🚛 Yuk Taraz yo'nalishida harakatlanmoqda.",
    "Shimkent": "🚛 Yuk Shimkent yo'nalishida harakatlanmoqda.",
    "Qonusbay": "🚛 Yuk Qonusbay nazorat nuqtasi orqali o'tmoqda.",
    "Saryagash": "🚛 Yuk Saryagash yo'nalishida harakatlanmoqda.",
    "Yallama": "🛃 Yuk Yallama chegara nuqtasiga yaqinlashdi yoki u yerdan o'tmoqda.",
    "Toshkent(Chuqursoy ULS da)": "📦 Yuk Toshkent(Chuqursoy ULS da)ga yetib keldi. Bojxona rasmiylashtirish ishlari boshlanmoqda.",
    "Kashgar (Qirg'iz)": "🛃 Yuk Kashgar orqali Qirg'iz yo'nalishiga kirdi. Yo'l haydovchi tomonidan tanlangan marshrut bo'yicha davom etmoqda.",
    "Irkeshtam": "🛃 Yuk Irkeshtam orqali Qirg'iz yo'nalishida o'tmoqda.",
    "Osh": "🚛 Yuk Osh yo'nalishida harakatlanmoqda.",
    "Dostlik": "🛃 Yuk Dostlik chegara nuqtasiga yaqinlashdi yoki u yerdan o'tmoqda.",
    "Andijon": "🚛 Yuk Andijon yo'nalishida harakatlanmoqda.",
    "Доставлен": "✅ Yuk muvaffaqiyatli topshirildi.",
}

DEFAULT_STATUS_DETAILS[DELIVERED_STATUS] = "✅ Yuk mijozga yetkazib berildi."
DEFAULT_STATUS_DETAILS[LEGACY_DELIVERED_STATUS] = DEFAULT_STATUS_DETAILS[DELIVERED_STATUS]

ETA_DESTINATION_LABELS = {
    "Toshkent": "Toshkentga yetib kelish vaqti",
    "Horgos": "Horgosga yetib kelish vaqti",
    "Qozog'istonga o'tish": "Qozog'istonga o'tish vaqti",
    "Qozoq furaga ortilish": "Qozoq furaga ortilish vaqti",
    "Mijozga yetib borish": "Mijozga yetib borish vaqti",
}

ETA_DESTINATION_LABELS_LOCALIZED = {
    "uz_latn": ETA_DESTINATION_LABELS,
    "uz_cyrl": {
        "Toshkent": "Тошкентга етиб келиш вақти",
        "Horgos": "Хоргосга етиб келиш вақти",
        "Qozog'istonga o'tish": "Қозоғистонга ўтиш вақти",
        "Qozoq furaga ortilish": "Қозоқ фурага ортилиш вақти",
        "Mijozga yetib borish": "Мижозга етиб бориш вақти",
    },
    "ru": {
        "Toshkent": "Срок прибытия в Ташкент",
        "Horgos": "Срок прибытия в Хоргос",
        "Qozog'istonga o'tish": "Срок перехода в Казахстан",
        "Qozoq furaga ortilish": "Срок погрузки на казахскую фуру",
        "Mijozga yetib borish": "Срок доставки клиенту",
    },
    "en": {
        "Toshkent": "Arrival time to Tashkent",
        "Horgos": "Arrival time to Horgos",
        "Qozog'istonga o'tish": "Kazakhstan border crossing time",
        "Qozoq furaga ortilish": "Loading time onto Kazakh truck",
        "Mijozga yetib borish": "Delivery time to client",
    },
}

STATUS_MESSAGE_LABELS = {
    "uz_cyrl": {
        "Xitoy": "Хитой",
        "Horgos": "Хоргос",
        "Nurjo'li": "Нуржўли",
        "Jarkent": "Жаркент",
        "Almata": "Алмата",
        "Taraz": "Тараз",
        "Shimkent": "Шимкент",
        "Qonusbay": "Қонусбай",
        "Saryagash": "Сариоғаш",
        "Yallama": "Яллама",
        "Toshkent(Chuqursoy ULS da)": "Тошкент(Чуқурсой ULS да)",
        "Kashgar (Qirg'iz)": "Кашгар (Қирғиз)",
        "Irkeshtam": "Иркештам",
        "Osh": "Ош",
        "Dostlik": "Дўстлик",
        "Andijon": "Андижон",
        DELIVERED_STATUS: "Мижозга етказиб берилди",
        LEGACY_DELIVERED_STATUS: "Мижозга етказиб берилди",
    },
    "ru": {
        "Xitoy": "Китай",
        "Horgos": "Хоргос",
        "Nurjo'li": "Нуржоли",
        "Jarkent": "Жаркент",
        "Almata": "Алматы",
        "Taraz": "Тараз",
        "Shimkent": "Шымкент",
        "Qonusbay": "Конысбай",
        "Saryagash": "Сарыагаш",
        "Yallama": "Яллама",
        "Toshkent(Chuqursoy ULS da)": "Ташкент (Чукурсой ULS)",
        "Kashgar (Qirg'iz)": "Кашгар (Кыргызстан)",
        "Irkeshtam": "Иркештам",
        "Osh": "Ош",
        "Dostlik": "Достлик",
        "Andijon": "Андижан",
        DELIVERED_STATUS: "Доставлено клиенту",
        LEGACY_DELIVERED_STATUS: "Доставлено клиенту",
    },
    "en": {
        "Xitoy": "China",
        "Yiwu": "Yiwu",
        "Zhongshan": "Zhongshan",
        "Horgos": "Horgos",
        "Nurjo'li": "Nurjo'li",
        "Jarkent": "Jarkent",
        "Almata": "Almaty",
        "Taraz": "Taraz",
        "Shimkent": "Shymkent",
        "Qonusbay": "Qonusbay",
        "Saryagash": "Saryagash",
        "Yallama": "Yallama",
        "Toshkent(Chuqursoy ULS da)": "Tashkent (Chuqursoy ULS)",
        "Kashgar (Qirg'iz)": "Kashgar (Kyrgyzstan)",
        "Irkeshtam": "Irkeshtam",
        "Osh": "Osh",
        "Dostlik": "Dostlik",
        "Andijon": "Andijan",
        DELIVERED_STATUS: "Delivered to client",
        LEGACY_DELIVERED_STATUS: "Delivered to client",
    },
}

TEMPLATE_LOCALIZATION = {
    "uz_cyrl": {
        "👋Assalomu alaykum hurmatli mijoz!": "👋Ассалому алайкум ҳурматли мижоз!",
        "📦 Sizning yukingiz bo‘yicha yangilangan treking ma’lumotlari:⤵️": "📦 Сизнинг юкингиз бўйича янгиланган трекинг маълумотлари:⤵️",
        "🚛 Partiya:": "🚛 Партия:",
        "🗓Bugungi sana:": "🗓Бугунги сана:",
        "🆔 BL-kod:": "🆔 BL-код:",
        "📍 Joriy holati:": "📍 Жорий ҳолати:",
        "📄 Yuk haqida ma'lumotlar:": "📄 Юк ҳақида маълумотлар:",
        "📲Aloqa uchun:": "📲Алоқа учун:",
        "🖇Tovar bo'yicha packing list⤵️": "🖇Товар бўйича packing list⤵️",
        "Packing list biriktirilmagan": "Packing list бириктирилмаган",
    },
    "ru": {
        "👋Assalomu alaykum hurmatli mijoz!": "👋Здравствуйте, уважаемый клиент!",
        "📦 Sizning yukingiz bo‘yicha yangilangan treking ma’lumotlari:⤵️": "📦 Обновлённая информация по трекингу вашего груза:⤵️",
        "🚛 Partiya:": "🚛 Партия:",
        "🗓Bugungi sana:": "🗓Дата обновления:",
        "🆔 BL-kod:": "🆔 BL-код:",
        "📍 Joriy holati:": "📍 Текущий статус:",
        "📄 Yuk haqida ma'lumotlar:": "📄 Информация о грузе:",
        "📲Aloqa uchun:": "📲Для связи:",
        "🖇Tovar bo'yicha packing list⤵️": "🖇Packing list по товару⤵️",
        "Packing list biriktirilmagan": "Packing list не прикреплён",
    },
    "en": {
        "👋Assalomu alaykum hurmatli mijoz!": "👋Hello, dear client!",
        "📦 Sizning yukingiz bo‘yicha yangilangan treking ma’lumotlari:⤵️": "📦 Updated tracking information for your cargo:⤵️",
        "🚛 Partiya:": "🚛 Batch:",
        "🗓Bugungi sana:": "🗓Update date:",
        "🆔 BL-kod:": "🆔 BL code:",
        "📍 Joriy holati:": "📍 Current status:",
        "📄 Yuk haqida ma'lumotlar:": "📄 Cargo information:",
        "📲Aloqa uchun:": "📲Contact:",
        "🖇Tovar bo'yicha packing list⤵️": "🖇Packing list⤵️",
        "Packing list biriktirilmagan": "Packing list not attached",
    },
}

DEFAULT_ETA_DESTINATION = "Toshkent"

STUCK_DAYS = 5


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=30000")
    except sqlite3.OperationalError as exc:
        # Disk I/O at this point usually means the underlying volume isn't
        # ready yet (e.g. Railway volume mount glitching). Surface it once
        # but don't crash — let the caller see a clearer error downstream.
        print(f"[get_conn] busy_timeout PRAGMA failed: {exc}", flush=True)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError as exc:
        # WAL requires writable disk. If the volume is in a half-mounted
        # state (Railway sometimes ships disk I/O errors during boot) fall
        # back to DELETE journal so the app at least boots and can serve
        # read-only traffic while ops investigates.
        print(f"[get_conn] WAL PRAGMA failed ({exc}); falling back to DELETE journal", flush=True)
        try:
            conn.execute("PRAGMA journal_mode=DELETE")
        except sqlite3.OperationalError as exc2:
            print(f"[get_conn] DELETE journal PRAGMA also failed: {exc2}", flush=True)
    try:
        conn.execute("PRAGMA foreign_keys=ON")
    except sqlite3.OperationalError as exc:
        print(f"[get_conn] foreign_keys PRAGMA failed: {exc}", flush=True)
    return conn


def _extract_eta_last_number(value: str) -> int | None:
    matches = re.findall(r"\d+", value or "")
    if not matches:
        return None
    try:
        return int(matches[-1])
    except Exception:
        return None


def _russian_days_word(number: int | None) -> str:
    if number is None:
        return "дней"
    number = abs(int(number))
    last_two = number % 100
    last = number % 10
    if 11 <= last_two <= 14:
        return "дней"
    if last == 1:
        return "день"
    if last in (2, 3, 4):
        return "дня"
    return "дней"


def _english_days_word(number: int | None, has_range: bool = False) -> str:
    if has_range:
        return "days"
    return "day" if number == 1 else "days"


# Operators frequently type free-form Uzbek (mixed with stray Russian
# loan-words like "Peregruz") into the ETA value field. The day-word
# swap below only normalizes "N kun" → "N дней / days / кун", but leaves
# everything BEFORE the number untranslated, so a Russian recipient
# would see "Peregruz bo'lish vaqti 4-5 дней" — half-translated.
#
# This dictionary catches the common descriptive phrases and rewrites
# them per target language. Each entry is (pattern, replacement). Order
# matters: longer / more-specific phrases first so a 1-word match
# doesn't shadow a 3-word phrase that contains it.
_ETA_PHRASE_TRANSLATIONS: dict[str, list[tuple[str, str]]] = {
    "uz_cyrl": [
        # Word-order-aware patterns first
        (r"(\d+(?:\s*-\s*\d+)?)\s+kun\s+ichida",               r"\1 кун ичида"),
        (r"(\d+(?:\s*-\s*\d+)?)\s+kun\s+qoldi",                r"\1 кун қолди"),
        # Multi-word phrases
        (r"Peregruz\s+bo[\'’ʻʼ`]?lish\s+vaqti",                "Перегруз бўлиш вақти"),
        (r"Qozoq\s+furaga\s+ortilish\s+vaqti",                 "Қозоқ фурага ортилиш вақти"),
        (r"Qozoq\s+furaga\s+ortilvotti",                       "Қозоқ фурага ортилвотти"),
        (r"Toshkentga\s+yetib\s+kelish\s+vaqti",               "Тошкентга етиб келиш вақти"),
        (r"Horgosga\s+yetib\s+kelish\s+vaqti",                 "Хоргосга етиб келиш вақти"),
        (r"Mijozga\s+yetib\s+borish\s+vaqti",                  "Мижозга етиб бориш вақти"),
        (r"Yetib\s+kelish\s+vaqti",                            "Етиб келиш вақти"),
        # Single-word fallbacks
        (r"\bichida\b",                                        "ичида"),
        (r"\bqoldi\b",                                         "қолди"),
        (r"\bbugun\b",                                         "бугун"),
        (r"\bertaga\b",                                        "эртага"),
        (r"\bhozir\b",                                         "ҳозир"),
        (r"\btaxminan\b",                                      "тахминан"),
        (r"\byo[\'’ʻʼ`]?lda\b",                                "йўлда"),
    ],
    "ru": [
        # "4-5 kun ichida" → "в течение 4-5 kun" (then kun → дней by day-word swap)
        (r"(\d+(?:\s*-\s*\d+)?)\s+kun\s+ichida",               r"в течение \1 kun"),
        # "3 kun qoldi" → "осталось 3 kun"
        (r"(\d+(?:\s*-\s*\d+)?)\s+kun\s+qoldi",                r"осталось \1 kun"),
        (r"Peregruz\s+bo[\'’ʻʼ`]?lish\s+vaqti",                "Срок перегруза"),
        (r"Qozoq\s+furaga\s+ortilish\s+vaqti",                 "Срок погрузки на казахскую фуру"),
        (r"Qozoq\s+furaga\s+ortilvotti",                       "Уже погружен на казахскую фуру"),
        (r"Toshkentga\s+yetib\s+kelish\s+vaqti",               "Срок прибытия в Ташкент"),
        (r"Horgosga\s+yetib\s+kelish\s+vaqti",                 "Срок прибытия в Хоргос"),
        (r"Mijozga\s+yetib\s+borish\s+vaqti",                  "Срок доставки клиенту"),
        (r"Yetib\s+kelish\s+vaqti",                            "Срок прибытия"),
        (r"\bichida\b",                                        "в течение"),
        (r"\bqoldi\b",                                         "осталось"),
        (r"\bbugun\b",                                         "сегодня"),
        (r"\bertaga\b",                                        "завтра"),
        (r"\bhozir\b",                                         "сейчас"),
        (r"\btaxminan\b",                                      "примерно"),
        (r"\byo[\'’ʻʼ`]?lda\b",                                "в пути"),
    ],
    "en": [
        (r"(\d+(?:\s*-\s*\d+)?)\s+kun\s+ichida",               r"within \1 kun"),
        (r"(\d+(?:\s*-\s*\d+)?)\s+kun\s+qoldi",                r"\1 kun remaining"),
        (r"Peregruz\s+bo[\'’ʻʼ`]?lish\s+vaqti",                "Reloading time"),
        (r"Qozoq\s+furaga\s+ortilish\s+vaqti",                 "Loading time onto Kazakh truck"),
        (r"Qozoq\s+furaga\s+ortilvotti",                       "Already loaded onto Kazakh truck"),
        (r"Toshkentga\s+yetib\s+kelish\s+vaqti",               "Arrival time to Tashkent"),
        (r"Horgosga\s+yetib\s+kelish\s+vaqti",                 "Arrival time to Horgos"),
        (r"Mijozga\s+yetib\s+borish\s+vaqti",                  "Delivery time to client"),
        (r"Yetib\s+kelish\s+vaqti",                            "Arrival time"),
        (r"\bichida\b",                                        "within"),
        (r"\bqoldi\b",                                         "remaining"),
        (r"\bbugun\b",                                         "today"),
        (r"\bertaga\b",                                        "tomorrow"),
        (r"\bhozir\b",                                         "now"),
        (r"\btaxminan\b",                                      "approximately"),
        (r"\byo[\'’ʻʼ`]?lda\b",                                "on the way"),
    ],
}


def _apply_eta_phrase_translations(text: str, language: str) -> str:
    """Rewrite known Uzbek tracking phrases inside the ETA value.

    Operates BEFORE the day-word swap so "Peregruz bo'lish vaqti 4-5 kun"
    can become "Срок перегруза 4-5 kun" first, and then the day-word swap
    turns the trailing "kun" into "дней" / "days".
    """
    table = _ETA_PHRASE_TRANSLATIONS.get(language) or []
    if not table:
        return text
    out = text
    for pattern, replacement in table:
        out = re.sub(pattern, replacement, out, flags=re.IGNORECASE)
    return out


def _localize_eta_value(value: str, language: str = DEFAULT_MESSAGE_LANGUAGE) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized_language = _normalize_message_language(language)
    if normalized_language == "uz_latn":
        return text

    # 1) Phrase pass — translate Uzbek descriptors (Peregruz / Yetib /
    #    bugun / qoldi etc.) so the value reads naturally in the target
    #    language even when the operator typed a multi-word note.
    text = _apply_eta_phrase_translations(text, normalized_language)

    # 2) Day-word pass — pluralize "kun / кун / day(s) / день/дня/дней"
    #    according to the trailing number, in the target language.
    last_number = _extract_eta_last_number(text)
    has_range = bool(re.search(r"\d+\s*-\s*\d+", text))
    day_word_map = {
        "uz_cyrl": "кун",
        "ru": _russian_days_word(last_number),
        "en": _english_days_word(last_number, has_range),
    }
    replacement = day_word_map.get(normalized_language)
    if not replacement:
        return text

    if re.search(r"\b(kun|кун|day|days|день|дня|дней)\b", text, flags=re.I):
        return re.sub(
            r"\b(kun|кун|day|days|день|дня|дней)\b",
            replacement,
            text,
            flags=re.I,
        )

    if re.fullmatch(r"\d+(?:\s*-\s*\d+)?", text):
        return f"{text} {replacement}"

    return text


def _is_locked_error(exc: Exception) -> bool:
    return "locked" in str(exc or "").lower()


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(row["name"] == column_name for row in rows)


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


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


# «Horgos (Qozoq)» переименован в «Horgos» (владелец, 24.08.2026) — старое
# значение может прийти из сохранённой формы или из строки, ещё не
# затронутой миграцией, поэтому принимаем оба написания.
LEGACY_HORGOS_STATUS = "Horgos (Qozoq)"
HORGOS_STATUS = "Horgos"


def normalize_status_value(value: str) -> str:
    text = (value or "").strip()
    return HORGOS_STATUS if text == LEGACY_HORGOS_STATUS else text


def _normalize_eta_destination(value: str) -> str:
    normalized = normalize_status_value(value)
    if normalized in ETA_DESTINATION_LABELS:
        return normalized
    return DEFAULT_ETA_DESTINATION


def _eta_destination_label(value: str, language: str = DEFAULT_MESSAGE_LANGUAGE) -> str:
    normalized_language = _normalize_message_language(language)
    labels = ETA_DESTINATION_LABELS_LOCALIZED.get(
        normalized_language,
        ETA_DESTINATION_LABELS_LOCALIZED[DEFAULT_MESSAGE_LANGUAGE],
    )
    normalized_destination = _normalize_eta_destination(value)
    return labels.get(
        normalized_destination,
        labels.get(DEFAULT_ETA_DESTINATION, ETA_DESTINATION_LABELS[DEFAULT_ETA_DESTINATION]),
    )


def _packing_list_label(language: str = DEFAULT_MESSAGE_LANGUAGE) -> str:
    normalized_language = _normalize_message_language(language)
    if normalized_language == "uz_cyrl":
        return "🖇Товар бўйича packing list⤵️"
    if normalized_language == "ru":
        return "🖇Packing list по товару⤵️"
    if normalized_language == "en":
        return "🖇Packing list⤵️"
    return "🖇Tovar bo'yicha packing list⤵️"


def _normalize_message_language(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized in MESSAGE_LANGUAGES:
        return normalized
    return DEFAULT_MESSAGE_LANGUAGE


def is_customer_delivery_eta(value: str) -> bool:
    return _normalize_eta_destination(value) == "Mijozga yetib borish"


def _is_delivered_status(value: str) -> bool:
    normalized = (value or "").strip()
    return normalized in {LEGACY_DELIVERED_STATUS, DELIVERED_STATUS}


def _normalize_status(value: str) -> str:
    normalized = (value or "Xitoy").strip() or "Xitoy"
    # старое написание «Horgos (Qozoq)» ещё может прийти из открытой формы
    # или внешнего вызова — приводим к новому, чтобы в базу не попадало
    normalized = normalize_status_value(normalized)
    if normalized == DELIVERED_STATUS:
        return LEGACY_DELIVERED_STATUS
    return normalized


def _migrate_drop_batches_name_unique(conn):
    """Batches used to require a UNIQUE name; two trucks can now depart
    on the same date, so duplicates are allowed. SQLite can't drop a
    constraint in place — rebuild the table once (legacy DBs only)."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='batches'"
    ).fetchone()
    create_sql = (row["sql"] if row else "") or ""
    if "UNIQUE" not in create_sql.upper():
        return
    new_sql = re.sub(
        r"(?i)\bname\s+TEXT\s+UNIQUE\s+NOT\s+NULL",
        "name TEXT NOT NULL",
        create_sql,
        count=1,
    )
    if new_sql == create_sql:
        # Unexpected schema layout — better keep the constraint than risk
        # a broken rebuild.
        return
    new_sql = re.sub(
        r"(?i)^\s*CREATE\s+TABLE\s+\"?batches\"?",
        'CREATE TABLE "batches__new"',
        new_sql,
        count=1,
    )
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute(new_sql)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(batches)").fetchall()]
        col_list = ", ".join(f'"{c}"' for c in cols)
        conn.execute(f'INSERT INTO "batches__new" ({col_list}) SELECT {col_list} FROM "batches"')
        conn.execute('DROP TABLE "batches"')
        conn.execute('ALTER TABLE "batches__new" RENAME TO "batches"')
        conn.commit()
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def init_db():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.executescript(
        """
        CREATE TABLE IF NOT EXISTS batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            status TEXT DEFAULT 'Xitoy',
            expected_date TEXT DEFAULT '',
            actual_date TEXT DEFAULT '',
            eta_to_toshkent TEXT DEFAULT '',
            eta_destination TEXT DEFAULT 'Toshkent',
            client_delivery_date TEXT DEFAULT '',
            route_started_at TEXT DEFAULT '',
            toshkent_arrived_at TEXT DEFAULT '',
            status_updated_at TEXT DEFAULT (datetime('now','localtime')),
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS bl_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
            code TEXT NOT NULL,
            merged_codes TEXT DEFAULT '',
            client_name TEXT DEFAULT '',
            chat_id TEXT DEFAULT '',
            status TEXT DEFAULT 'Xitoy',
            message_language TEXT DEFAULT 'uz_latn',
            moderator_tg_id TEXT DEFAULT '',
            sales_manager_tg_id TEXT DEFAULT '',
            cargo_type TEXT DEFAULT '',
            weight_kg REAL NOT NULL DEFAULT 0,
            volume_cbm REAL NOT NULL DEFAULT 0,
            quantity_places INTEGER NOT NULL DEFAULT 0,
            quantity_places_breakdown TEXT DEFAULT '',
            cargo_description TEXT DEFAULT '',
            expected_date TEXT DEFAULT '',
            actual_date TEXT DEFAULT '',
            status_updated_at TEXT DEFAULT (datetime('now','localtime')),
            created_at TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(batch_id, code)
        );

        -- Permanent BL ↔ Telegram-group link memory. Survives batch
        -- deletion (bl_codes rows die with their batch), so once a BL
        -- code was ever linked to a group, future batches can auto-link
        -- it even when the group title has no recognizable match.
        CREATE TABLE IF NOT EXISTS bl_link_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code_norm TEXT NOT NULL,
            code_raw TEXT NOT NULL DEFAULT '',
            chat_id TEXT NOT NULL,
            chat_title TEXT NOT NULL DEFAULT '',
            batch_id INTEGER NOT NULL DEFAULT 0,
            batch_name TEXT NOT NULL DEFAULT '',
            linked_at TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(code_norm, chat_id)
        );

        CREATE INDEX IF NOT EXISTS idx_bl_link_history_code
            ON bl_link_history(code_norm, linked_at);

        CREATE TABLE IF NOT EXISTS ai_assistant_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_user_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE INDEX IF NOT EXISTS idx_ai_assistant_messages_user
            ON ai_assistant_messages(tg_user_id, id);

        CREATE TABLE IF NOT EXISTS ai_scheduled_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_by TEXT NOT NULL DEFAULT '',
            run_at TEXT NOT NULL,
            recurrence TEXT NOT NULL DEFAULT 'once',
            kind TEXT NOT NULL,
            params_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'pending',
            last_run_at TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS ai_pending_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_user_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            params_json TEXT NOT NULL DEFAULT '{}',
            summary TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            result TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            decided_at TEXT
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
            filled_by TEXT NOT NULL DEFAULT '',
            confirmed_by TEXT NOT NULL DEFAULT '',
            sent_via TEXT NOT NULL DEFAULT '',
            sent_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS drive_seen_files (
            file_id TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            folder_name TEXT NOT NULL DEFAULT '',
            seen_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS packing_file_questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            file_path TEXT NOT NULL,
            chat_id TEXT NOT NULL DEFAULT '',
            message_id TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            answer TEXT NOT NULL DEFAULT '',
            bl_id INTEGER,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            resolved_at TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS tracking_delivery_coverage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bl_id INTEGER NOT NULL REFERENCES bl_codes(id) ON DELETE CASCADE,
            batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
            chat_id TEXT NOT NULL DEFAULT '',
            last_source_batch_id INTEGER REFERENCES batches(id) ON DELETE SET NULL,
            last_source_bl_id INTEGER REFERENCES bl_codes(id) ON DELETE SET NULL,
            tracking_signature TEXT NOT NULL DEFAULT '',
            sent_at TEXT NOT NULL DEFAULT '',
            UNIQUE(bl_id)
        );

        CREATE TABLE IF NOT EXISTS batch_send_exclusions (
            batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
            bl_id INTEGER NOT NULL REFERENCES bl_codes(id) ON DELETE CASCADE,
            is_excluded INTEGER NOT NULL DEFAULT 1,
            source TEXT NOT NULL DEFAULT 'manual',
            updated_at TEXT DEFAULT (datetime('now','localtime')),
            PRIMARY KEY(batch_id, bl_id)
        );

        -- Queued announcement broadcasts: snapshot the text + recipients +
        -- attachment AT SCHEDULE TIME so swapping the global announcement
        -- template later doesn't affect a pending job. A background thread
        -- (announcement_scheduler) wakes up every 30 sec and fires due rows.
        CREATE TABLE IF NOT EXISTS announcement_schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL DEFAULT '',
            chat_ids TEXT NOT NULL DEFAULT '',       -- JSON array of chat_id strings
            attachment_snapshot TEXT NOT NULL DEFAULT '',  -- JSON {filename, file_path, kind}
            scheduled_at TEXT NOT NULL,              -- "YYYY-MM-DD HH:MM:SS" Tashkent local
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            created_by TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',  -- pending | sending | sent | cancelled | failed
            sent_at TEXT NOT NULL DEFAULT '',
            result_sent INTEGER NOT NULL DEFAULT 0,
            result_failed INTEGER NOT NULL DEFAULT 0,
            last_error TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_anns_status_at
            ON announcement_schedules(status, scheduled_at);

        -- Director dashboard: one row per section (savdo / ombor / agentlar).
        -- Stores the Google Sheets source URL plus a JSON column-mapping so
        -- the director can re-connect/re-configure sources without code change.
        CREATE TABLE IF NOT EXISTS director_dashboard_config (
            section TEXT PRIMARY KEY,                -- 'savdo' | 'ombor' | 'agentlar'
            sheet_url TEXT NOT NULL DEFAULT '',
            sheet_id TEXT NOT NULL DEFAULT '',
            sheet_gid TEXT NOT NULL DEFAULT '',
            sheet_name TEXT NOT NULL DEFAULT '',
            header_rows INTEGER NOT NULL DEFAULT 1,
            columns_json TEXT NOT NULL DEFAULT '{}', -- JSON {field_key: column_letter, ...}
            updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            updated_by TEXT NOT NULL DEFAULT ''
        );

        -- Catalogue of every Telegram message we've sent that the operator
        -- might want to recall later (tracking texts and the packing-list
        -- files that follow them). Populated by record_sent_telegram_message
        -- and consumed by /api/batches/<id>/recall-tracking.
        --
        -- bl_id is NULLABLE because once a BL row is deleted we still want
        -- to be able to recall its messages by chat_id+message_id. batch_id
        -- is also NULLABLE for the same reason.
        CREATE TABLE IF NOT EXISTS tracking_sent_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bl_id INTEGER REFERENCES bl_codes(id) ON DELETE SET NULL,
            batch_id INTEGER REFERENCES batches(id) ON DELETE SET NULL,
            chat_id TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            kind TEXT NOT NULL DEFAULT 'tracking',  -- 'tracking' | 'file'
            sent_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            recalled_at TEXT DEFAULT '',
            recall_error TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_tsm_batch_sent
            ON tracking_sent_messages(batch_id, sent_at);
        CREATE INDEX IF NOT EXISTS idx_tsm_bl_sent
            ON tracking_sent_messages(bl_id, sent_at);
        CREATE INDEX IF NOT EXISTS idx_tsm_recalled
            ON tracking_sent_messages(recalled_at);

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

        CREATE TABLE IF NOT EXISTS telegram_track_cooldowns (
            chat_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            last_pressed_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY(chat_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS telegram_chats (
            chat_id TEXT PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            chat_type TEXT NOT NULL DEFAULT 'group',
            username TEXT DEFAULT '',
            moderator_tg_id TEXT DEFAULT '',
            sales_manager_tg_id TEXT DEFAULT '',
            ai_enabled INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            last_seen_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS telegram_chat_members (
            chat_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            display_name TEXT NOT NULL DEFAULT '',
            username TEXT NOT NULL DEFAULT '',
            is_admin INTEGER NOT NULL DEFAULT 0,
            first_seen_at TEXT DEFAULT (datetime('now','localtime')),
            last_seen_at TEXT DEFAULT (datetime('now','localtime')),
            PRIMARY KEY(chat_id, user_id)
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
            message_id TEXT NOT NULL DEFAULT '',
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

        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL DEFAULT '',
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS ai_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL DEFAULT '',
            group_title TEXT NOT NULL DEFAULT '',
            user_id TEXT NOT NULL DEFAULT '',
            username TEXT NOT NULL DEFAULT '',
            original_text TEXT NOT NULL DEFAULT '',
            detected_intent TEXT NOT NULL DEFAULT '',
            bl_code TEXT NOT NULL DEFAULT '',
            ai_response TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS analytics_sales_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file_id TEXT NOT NULL DEFAULT '',
            source_sheet TEXT NOT NULL DEFAULT '',
            source_row INTEGER NOT NULL DEFAULT 0,
            reys_number TEXT NOT NULL DEFAULT '',
            invoice_date TEXT NOT NULL DEFAULT '',
            sale_date TEXT NOT NULL DEFAULT '',
            shipping_mark TEXT NOT NULL DEFAULT '',
            brand_name TEXT NOT NULL DEFAULT '',
            client_name TEXT NOT NULL DEFAULT '',
            phone TEXT NOT NULL DEFAULT '',
            client_status TEXT NOT NULL DEFAULT '',
            cargo_name TEXT NOT NULL DEFAULT '',
            quantity REAL NOT NULL DEFAULT 0,
            ctn REAL NOT NULL DEFAULT 0,
            cbm REAL NOT NULL DEFAULT 0,
            net_weight REAL NOT NULL DEFAULT 0,
            gross_weight REAL NOT NULL DEFAULT 0,
            customs_payment REAL NOT NULL DEFAULT 0,
            company_expense REAL NOT NULL DEFAULT 0,
            certificate_expense REAL NOT NULL DEFAULT 0,
            client_price REAL NOT NULL DEFAULT 0,
            sale_amount REAL NOT NULL DEFAULT 0,
            correction_amount REAL NOT NULL DEFAULT 0,
            discount_amount REAL NOT NULL DEFAULT 0,
            final_sale_amount REAL NOT NULL DEFAULT 0,
            salesperson TEXT NOT NULL DEFAULT '',
            sales_kpi_amount REAL NOT NULL DEFAULT 0,
            customs_kpi_amount REAL NOT NULL DEFAULT 0,
            raw_data_json TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS analytics_cashflow_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file_id TEXT NOT NULL DEFAULT '',
            source_sheet TEXT NOT NULL DEFAULT '',
            source_row INTEGER NOT NULL DEFAULT 0,
            created_date TEXT NOT NULL DEFAULT '',
            operation_date TEXT NOT NULL DEFAULT '',
            wallet TEXT NOT NULL DEFAULT '',
            flow_type TEXT NOT NULL DEFAULT '',
            currency TEXT NOT NULL DEFAULT '',
            comment TEXT NOT NULL DEFAULT '',
            category TEXT NOT NULL DEFAULT '',
            amount REAL NOT NULL DEFAULT 0,
            rate REAL NOT NULL DEFAULT 0,
            department TEXT NOT NULL DEFAULT '',
            reys_number TEXT NOT NULL DEFAULT '',
            counterparty TEXT NOT NULL DEFAULT '',
            auto_confirm TEXT NOT NULL DEFAULT '',
            amount_usd REAL NOT NULL DEFAULT 0,
            raw_data_json TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS analytics_currency_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file_id TEXT NOT NULL DEFAULT '',
            rate_date TEXT NOT NULL DEFAULT '',
            currency TEXT NOT NULL DEFAULT '',
            rate_to_usd REAL NOT NULL DEFAULT 0,
            raw_data_json TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS analytics_logist_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file_id TEXT NOT NULL DEFAULT '',
            reys_number TEXT NOT NULL DEFAULT '',
            logist_name TEXT NOT NULL DEFAULT '',
            position INTEGER NOT NULL DEFAULT 0,
            warehouse_no_extra_days INTEGER NOT NULL DEFAULT 0,
            no_damage_or_missing INTEGER NOT NULL DEFAULT 0,
            raw_data_json TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS analytics_shipment_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file_id TEXT NOT NULL DEFAULT '',
            agent TEXT NOT NULL DEFAULT '',
            reys_number TEXT NOT NULL DEFAULT '',
            logist_summa REAL NOT NULL DEFAULT 0,
            rate REAL NOT NULL DEFAULT 0,
            usd REAL NOT NULL DEFAULT 0,
            loaded_date TEXT NOT NULL DEFAULT '',
            china_truck_number TEXT NOT NULL DEFAULT '',
            container_or_truck TEXT NOT NULL DEFAULT '',
            container_type TEXT NOT NULL DEFAULT '',
            agent_given_date TEXT NOT NULL DEFAULT '',
            agent_fact_days REAL NOT NULL DEFAULT 0,
            horgos_date TEXT NOT NULL DEFAULT '',
            zhongshan_horgos_days REAL NOT NULL DEFAULT 0,
            kazakh_truck_date TEXT NOT NULL DEFAULT '',
            driver_name TEXT NOT NULL DEFAULT '',
            driver_phone TEXT NOT NULL DEFAULT '',
            loaded_to_truck_days REAL NOT NULL DEFAULT 0,
            kazakh_truck_number TEXT NOT NULL DEFAULT '',
            tashkent_date TEXT NOT NULL DEFAULT '',
            zhongshan_tashkent_days REAL NOT NULL DEFAULT 0,
            customs_date TEXT NOT NULL DEFAULT '',
            distributed_date TEXT NOT NULL DEFAULT '',
            distribution_days REAL NOT NULL DEFAULT 0,
            raw_data_json TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS analytics_shipment_statuses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file_id TEXT NOT NULL DEFAULT '',
            reys_number TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            status_date TEXT NOT NULL DEFAULT '',
            truck_number TEXT NOT NULL DEFAULT '',
            driver_name TEXT NOT NULL DEFAULT '',
            driver_phone TEXT NOT NULL DEFAULT '',
            raw_data_json TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS analytics_sync_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL DEFAULT '',
            source_name TEXT NOT NULL DEFAULT '',
            started_at TEXT NOT NULL DEFAULT '',
            finished_at TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            rows_imported INTEGER NOT NULL DEFAULT 0,
            rows_skipped INTEGER NOT NULL DEFAULT 0,
            error_message TEXT NOT NULL DEFAULT '',
            details_json TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS analytics_sales_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL DEFAULT '',
            period_start TEXT NOT NULL DEFAULT '',
            period_end TEXT NOT NULL DEFAULT '',
            target_amount_usd REAL NOT NULL DEFAULT 0,
            target_metric TEXT NOT NULL DEFAULT 'amount_usd',
            target_value REAL NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS moderator_response_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            chat_title TEXT NOT NULL DEFAULT '',
            request_message_id TEXT NOT NULL,
            request_user_id TEXT NOT NULL DEFAULT '',
            request_user_name TEXT NOT NULL DEFAULT '',
            request_username TEXT NOT NULL DEFAULT '',
            request_text TEXT NOT NULL DEFAULT '',
            bl_id INTEGER REFERENCES bl_codes(id) ON DELETE SET NULL,
            batch_id INTEGER REFERENCES batches(id) ON DELETE SET NULL,
            batch_name TEXT NOT NULL DEFAULT '',
            requested_at TEXT NOT NULL DEFAULT '',
            assigned_moderator_id TEXT NOT NULL DEFAULT '',
            assigned_sales_manager_id TEXT NOT NULL DEFAULT '',
            responded_at TEXT NOT NULL DEFAULT '',
            responder_user_id TEXT NOT NULL DEFAULT '',
            responder_name TEXT NOT NULL DEFAULT '',
            responder_username TEXT NOT NULL DEFAULT '',
            response_text TEXT NOT NULL DEFAULT '',
            response_seconds INTEGER NOT NULL DEFAULT 0,
            response_role TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'open',
            UNIQUE(chat_id, request_message_id)
        );

        CREATE TABLE IF NOT EXISTS sales_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT DEFAULT '',
            bl_code TEXT DEFAULT '',
            client_name TEXT DEFAULT '',
            manager_name TEXT DEFAULT '',
            service_type TEXT DEFAULT '',
            amount REAL NOT NULL DEFAULT 0,
            cost REAL NOT NULL DEFAULT 0,
            profit REAL NOT NULL DEFAULT 0,
            currency TEXT DEFAULT '',
            paid_amount REAL NOT NULL DEFAULT 0,
            debt_amount REAL NOT NULL DEFAULT 0,
            payment_status TEXT DEFAULT '',
            source TEXT DEFAULT '',
            source_sheet TEXT DEFAULT '',
            raw_data_json TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS cashflow_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT DEFAULT '',
            type TEXT DEFAULT '',
            category TEXT DEFAULT '',
            amount REAL NOT NULL DEFAULT 0,
            currency TEXT DEFAULT '',
            bank_or_cash TEXT DEFAULT '',
            contractor TEXT DEFAULT '',
            bl_code TEXT DEFAULT '',
            reys_number TEXT DEFAULT '',
            comment TEXT DEFAULT '',
            source_sheet TEXT DEFAULT '',
            raw_data_json TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS shipments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bl_code TEXT DEFAULT '',
            client_name TEXT DEFAULT '',
            reys_number TEXT DEFAULT '',
            fura_number TEXT DEFAULT '',
            container_type TEXT DEFAULT '',
            station TEXT DEFAULT '',
            agent TEXT DEFAULT '',
            logist_name TEXT DEFAULT '',
            sales_manager_name TEXT DEFAULT '',
            warehouse TEXT DEFAULT '',
            status TEXT DEFAULT '',
            loaded_date TEXT DEFAULT '',
            arrived_date TEXT DEFAULT '',
            expected_date TEXT DEFAULT '',
            cargo_type TEXT DEFAULT '',
            weight_kg REAL NOT NULL DEFAULT 0,
            volume_m3 REAL NOT NULL DEFAULT 0,
            places INTEGER NOT NULL DEFAULT 0,
            places_breakdown TEXT DEFAULT '',
            description TEXT DEFAULT '',
            source_sheet TEXT DEFAULT '',
            raw_data_json TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS analytics_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT DEFAULT '',
            total_sales REAL NOT NULL DEFAULT 0,
            total_income REAL NOT NULL DEFAULT 0,
            total_expense REAL NOT NULL DEFAULT 0,
            profit REAL NOT NULL DEFAULT 0,
            debt REAL NOT NULL DEFAULT 0,
            active_bl_count INTEGER NOT NULL DEFAULT 0,
            arrived_shipments_count INTEGER NOT NULL DEFAULT 0,
            delayed_shipments_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS sheet_sync_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT DEFAULT '',
            finished_at TEXT DEFAULT '',
            status TEXT DEFAULT '',
            rows_imported INTEGER NOT NULL DEFAULT 0,
            rows_skipped INTEGER NOT NULL DEFAULT 0,
            error_message TEXT DEFAULT '',
            details_json TEXT DEFAULT ''
        );
        """
    )

    batch_columns = [
        ("incident_note", "TEXT DEFAULT ''"),
        # привязка партии к блоку плана погрузки (авто-синхронизация)
        ("plan_tab", "TEXT DEFAULT ''"),
        ("plan_title", "TEXT DEFAULT ''"),
        ("plan_date", "TEXT DEFAULT ''"),
        ("plan_kind", "TEXT DEFAULT ''"),
        ("status", "TEXT DEFAULT 'Xitoy'"),
        ("expected_date", "TEXT DEFAULT ''"),
        ("actual_date", "TEXT DEFAULT ''"),
        ("eta_to_toshkent", "TEXT DEFAULT ''"),
        ("eta_destination", "TEXT DEFAULT 'Toshkent'"),
        ("client_delivery_date", "TEXT DEFAULT ''"),
        ("route_started_at", "TEXT DEFAULT ''"),
        ("toshkent_arrived_at", "TEXT DEFAULT ''"),
        ("status_updated_at", "TEXT DEFAULT ''"),
    ]
    for column_name, column_def in batch_columns:
        if not _table_has_column(conn, "batches", column_name):
            conn.execute(f"ALTER TABLE batches ADD COLUMN {column_name} {column_def}")

    _migrate_drop_batches_name_unique(conn)

    legacy_columns = [
        ("merged_codes", "TEXT DEFAULT ''"),
        ("cargo_type", "TEXT DEFAULT ''"),
        ("weight_kg", "REAL NOT NULL DEFAULT 0"),
        ("volume_cbm", "REAL NOT NULL DEFAULT 0"),
        ("quantity_places", "INTEGER NOT NULL DEFAULT 0"),
        ("quantity_places_breakdown", "TEXT DEFAULT ''"),
        ("cargo_description", "TEXT DEFAULT ''"),
        ("message_language", "TEXT DEFAULT 'uz_latn'"),
        ("moderator_tg_id", "TEXT DEFAULT ''"),
        ("sales_manager_tg_id", "TEXT DEFAULT ''"),
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
        # Telegram-side file_id cache: after the first successful sendDocument
        # we store the returned file_id and reuse it on every subsequent tap
        # so Telegram serves the file from its own CDN instead of forcing us
        # to re-upload from disk (10×+ faster, no webhook timeouts).
        ("tg_file_id", "TEXT NOT NULL DEFAULT ''"),
    ]
    for column_name, column_def in file_columns:
        if not _table_has_column(conn, "files", column_name):
            conn.execute(f"ALTER TABLE files ADD COLUMN {column_name} {column_def}")

    # кто дал ход трекингу: filled_by — кто обновил данные в форме,
    # confirmed_by — кто нажал TASDIQLASH/✅, sent_via — каким путём ушло
    # (форма-группа / ассистент / авто). Нужно, чтобы на вопрос «кто
    # обновил/разрешил трекинг» бот отвечал фактом, а не «автоматически».
    # «Horgos (Qozoq)» → «Horgos» (владелец 24.08.2026): переименование
    # статуса и точки ETA. Значение хранится строкой в нескольких местах,
    # поэтому переводим все разом — иначе старые партии выпадут из
    # сравнений вида status == HORGOS_STATUS.
    for table, column in (
        ("batches", "status"), ("bl_codes", "status"), ("batches", "eta_destination"),
    ):
        if _table_has_column(conn, table, column):
            conn.execute(
                f"UPDATE {table} SET {column} = ? WHERE {column} = ?",
                (HORGOS_STATUS, LEGACY_HORGOS_STATUS),
            )

    # источник исключения из рассылки: 'manual' — человек, 'plan' — бот
    # (груза нет ни в одном казахском плане). Автоматически снимать можно
    # ТОЛЬКО свои, чтобы не отменить решение человека.
    exclusion_columns = [("source", "TEXT NOT NULL DEFAULT 'manual'")]
    for column_name, column_def in exclusion_columns:
        if not _table_has_column(conn, "batch_send_exclusions", column_name):
            conn.execute(f"ALTER TABLE batch_send_exclusions ADD COLUMN {column_name} {column_def}")

    send_log_columns = [
        ("filled_by", "TEXT NOT NULL DEFAULT ''"),
        ("confirmed_by", "TEXT NOT NULL DEFAULT ''"),
        ("sent_via", "TEXT NOT NULL DEFAULT ''"),
    ]
    for column_name, column_def in send_log_columns:
        if not _table_has_column(conn, "send_logs", column_name):
            conn.execute(f"ALTER TABLE send_logs ADD COLUMN {column_name} {column_def}")

    communication_rating_columns = [
        ("voter_user_id", "TEXT NOT NULL DEFAULT ''"),
        ("voter_name", "TEXT NOT NULL DEFAULT ''"),
        ("voter_username", "TEXT NOT NULL DEFAULT ''"),
    ]
    for column_name, column_def in communication_rating_columns:
        if not _table_has_column(conn, "communication_ratings", column_name):
            conn.execute(f"ALTER TABLE communication_ratings ADD COLUMN {column_name} {column_def}")

    communication_dispatch_columns = [
        ("message_id", "TEXT NOT NULL DEFAULT ''"),
    ]
    for column_name, column_def in communication_dispatch_columns:
        if not _table_has_column(conn, "communication_survey_dispatches", column_name):
            conn.execute(f"ALTER TABLE communication_survey_dispatches ADD COLUMN {column_name} {column_def}")

    moderator_request_columns = [
        ("assigned_moderator_id", "TEXT NOT NULL DEFAULT ''"),
        ("assigned_sales_manager_id", "TEXT NOT NULL DEFAULT ''"),
        ("response_role", "TEXT NOT NULL DEFAULT ''"),
    ]
    for column_name, column_def in moderator_request_columns:
        if not _table_has_column(conn, "moderator_response_requests", column_name):
            conn.execute(f"ALTER TABLE moderator_response_requests ADD COLUMN {column_name} {column_def}")

    telegram_chat_columns = [
        ("moderator_tg_id", "TEXT DEFAULT ''"),
        ("sales_manager_tg_id", "TEXT DEFAULT ''"),
        ("ai_enabled", "INTEGER NOT NULL DEFAULT 0"),
        ("updated_at", "TEXT DEFAULT ''"),
    ]
    for column_name, column_def in telegram_chat_columns:
        if not _table_has_column(conn, "telegram_chats", column_name):
            conn.execute(f"ALTER TABLE telegram_chats ADD COLUMN {column_name} {column_def}")

    shipment_columns = [
        ("places_breakdown", "TEXT DEFAULT ''"),
        ("source_sheet", "TEXT DEFAULT ''"),
        ("raw_data_json", "TEXT DEFAULT ''"),
        ("created_at", "TEXT DEFAULT ''"),
        ("updated_at", "TEXT DEFAULT ''"),
    ]
    if _table_exists(conn, "shipments"):
        for column_name, column_def in shipment_columns:
            if not _table_has_column(conn, "shipments", column_name):
                conn.execute(f"ALTER TABLE shipments ADD COLUMN {column_name} {column_def}")

    conn.execute(
        """
        UPDATE telegram_chats
        SET
            moderator_tg_id = COALESCE(
                NULLIF(moderator_tg_id, ''),
                (
                    SELECT NULLIF(TRIM(bl.moderator_tg_id), '')
                    FROM bl_codes bl
                    WHERE bl.chat_id = telegram_chats.chat_id
                      AND NULLIF(TRIM(bl.moderator_tg_id), '') IS NOT NULL
                    ORDER BY bl.created_at DESC
                    LIMIT 1
                ),
                ''
            ),
            sales_manager_tg_id = COALESCE(
                NULLIF(sales_manager_tg_id, ''),
                (
                    SELECT NULLIF(TRIM(bl.sales_manager_tg_id), '')
                    FROM bl_codes bl
                    WHERE bl.chat_id = telegram_chats.chat_id
                      AND NULLIF(TRIM(bl.sales_manager_tg_id), '') IS NOT NULL
                    ORDER BY bl.created_at DESC
                    LIMIT 1
                ),
                ''
            ),
            ai_enabled = COALESCE(ai_enabled, 0),
            updated_at = COALESCE(
                NULLIF(updated_at, ''),
                NULLIF(last_seen_at, ''),
                NULLIF(created_at, ''),
                datetime('now','localtime')
            )
        """
    )

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
            ),
            client_delivery_date = COALESCE(NULLIF(client_delivery_date, ''), NULLIF(actual_date, ''), ''),
            route_started_at = COALESCE(
                NULLIF(route_started_at, ''),
                CASE
                    WHEN status IN ('Yiwu', 'Zhongshan')
                        THEN COALESCE(NULLIF(status_updated_at, ''), created_at, datetime('now','localtime'))
                    ELSE ''
                END
            ),
            toshkent_arrived_at = COALESCE(
                NULLIF(toshkent_arrived_at, ''),
                CASE
                    WHEN status = 'Toshkent(Chuqursoy ULS da)'
                        THEN COALESCE(NULLIF(status_updated_at, ''), created_at, datetime('now','localtime'))
                    ELSE ''
                END
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

    # Seed the permanent BL↔chat link memory from every linked BL that is
    # still in the database. INSERT OR IGNORE keeps rows already recorded
    # (and their newer linked_at) intact, so this is safe on every start.
    conn.execute(
        """
        INSERT OR IGNORE INTO bl_link_history(
            code_norm, code_raw, chat_id, chat_title, batch_id, batch_name, linked_at
        )
        SELECT
            UPPER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(bl.code), '-', ''), '_', ''), ' ', ''), '.', '')),
            TRIM(bl.code),
            TRIM(bl.chat_id),
            COALESCE(tc.title, ''),
            bl.batch_id,
            COALESCE(b.name, ''),
            COALESCE(NULLIF(bl.created_at, ''), datetime('now','localtime'))
        FROM bl_codes bl
        LEFT JOIN batches b ON b.id = bl.batch_id
        LEFT JOIN telegram_chats tc ON tc.chat_id = TRIM(bl.chat_id)
        WHERE TRIM(COALESCE(bl.chat_id, '')) != ''
          AND TRIM(COALESCE(bl.code, '')) != ''
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
        # Шаблоны «дефолтной семьи» (с этим приветствием) всё равно
        # рендерятся кодовым DEFAULT_TEMPLATE — синхронизируем и копию
        # в БД, чтобы раздел «Шаблон сообщения» показывал актуальный вид.
        if (
            "Assalomu alaykum hurmatli mijoz" in current_template
            and current_template != DEFAULT_TEMPLATE.strip()
        ):
            current_template = ""
        if not current_template or current_template == LEGACY_DEFAULT_TEMPLATE.strip():
            cursor.execute(
                """
                UPDATE message_template
                SET content = ?, updated_at = datetime('now','localtime')
                WHERE id = 1
                """,
                (DEFAULT_TEMPLATE,),
            )
        else:
            # Strip the legacy contact block from any saved template (the
            # admin may have customized other parts of the template, so we
            # can't just overwrite it).
            stripped = _strip_contact_block(current_template).strip()
            if stripped and stripped != current_template:
                cursor.execute(
                    """
                    UPDATE message_template
                    SET content = ?, updated_at = datetime('now','localtime')
                    WHERE id = 1
                    """,
                    (stripped,),
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
        "Хоргос": "Horgos",
        "Алматы": "Almata",
        "В пути до Ташкента": "Yallama",
        "Ташкент": "Toshkent(Chuqursoy ULS da)",
        "Toshkent": "Toshkent(Chuqursoy ULS da)",
        "Altynko'l": "Nurjo'li",
        "Chuqur": "Dostlik",
        "Chuqursoy": "Dostlik",
        DELIVERED_STATUS: LEGACY_DELIVERED_STATUS,
    }
    for old_status, new_status in legacy_status_map.items():
        cursor.execute("UPDATE batches SET status = ? WHERE status = ?", (new_status, old_status))
        cursor.execute("UPDATE bl_codes SET status = ? WHERE status = ?", (new_status, old_status))

    # Migration: ombor sheet config per sales plan
    for _col_def in [
        "ombor_sheet_id TEXT NOT NULL DEFAULT ''",
        "ombor_sheet_name TEXT NOT NULL DEFAULT 'Ombor'",
        "ombor_cbm_col TEXT NOT NULL DEFAULT 'W'",
        "ombor_date_col TEXT NOT NULL DEFAULT 'AA'",
        "ombor_seller_col TEXT NOT NULL DEFAULT 'AH'",
        "ombor_header_rows INTEGER NOT NULL DEFAULT 2",
        "ombor_logist_col TEXT NOT NULL DEFAULT 'AH'",          # NEW: logist names (Ombor col AH)
        # FTL (full-truckload) second sheet config per plan
        "ftl_sheet_id TEXT NOT NULL DEFAULT ''",
        "ftl_sheet_gid TEXT NOT NULL DEFAULT ''",
        "ftl_type_col TEXT NOT NULL DEFAULT 'J'",
        "ftl_date_col TEXT NOT NULL DEFAULT 'L'",
        "ftl_seller_col TEXT NOT NULL DEFAULT 'AB'",
        "ftl_header_rows INTEGER NOT NULL DEFAULT 1",
        "ftl_cbm_per_truck REAL NOT NULL DEFAULT 10",
    ]:
        try:
            conn.execute(f"ALTER TABLE analytics_sales_plans ADD COLUMN {_col_def}")
        except sqlite3.OperationalError:
            pass

    conn.commit()
    conn.close()


def create_batch(
    name,
    status="Xitoy",
    eta_to_toshkent="",
    eta_destination="Toshkent",
    client_delivery_date="",
):
    conn = get_conn()
    try:
        status = _normalize_status(status)
        cur = conn.execute(
            "INSERT INTO batches(name, status, expected_date, actual_date, eta_to_toshkent, eta_destination, client_delivery_date, route_started_at, toshkent_arrived_at, status_updated_at) VALUES(?, ?, '', '', ?, ?, ?, ?, ?, datetime('now','localtime'))",
            (
                (name or "").strip(),
                status,
                (eta_to_toshkent or "").strip(),
                _normalize_eta_destination(eta_destination),
                (client_delivery_date or "").strip(),
                current_ts() if status in ("Yiwu", "Zhongshan") else "",
                current_ts() if status == "Toshkent(Chuqursoy ULS da)" else "",
            ),
        )
        conn.commit()
        # id новой партии (truthy) — старые вызовы, ждавшие bool, не ломаются
        return cur.lastrowid
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def update_batch(
    batch_id,
    name,
    status="Xitoy",
    eta_to_toshkent="",
    eta_destination="Toshkent",
    client_delivery_date="",
):
    conn = get_conn()
    try:
        new_status = _normalize_status(status)
        conn.execute(
            """
            UPDATE batches
            SET
                name = ?,
                status = ?,
                eta_to_toshkent = ?,
                eta_destination = ?,
                client_delivery_date = ?,
                route_started_at = CASE
                    WHEN COALESCE(route_started_at, '') != '' THEN route_started_at
                    WHEN ? IN ('Yiwu', 'Zhongshan') THEN datetime('now','localtime')
                    ELSE COALESCE(route_started_at, '')
                END,
                toshkent_arrived_at = CASE
                    WHEN COALESCE(toshkent_arrived_at, '') != '' THEN toshkent_arrived_at
                    WHEN ? = 'Toshkent(Chuqursoy ULS da)' THEN datetime('now','localtime')
                    ELSE COALESCE(toshkent_arrived_at, '')
                END,
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
                (client_delivery_date or "").strip(),
                new_status,
                new_status,
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


def update_bl_figures(bl_id, ctn, breakdown: str, cbm, kg) -> None:
    """Обновить места/кубы/вес BL из плана погрузки (точечно, не трогая
    остальные поля)."""
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE bl_codes
            SET quantity_places = ?, quantity_places_breakdown = ?,
                volume_cbm = ?, weight_kg = ?
            WHERE id = ?
            """,
            (int(round(float(ctn or 0))), str(breakdown or ""), float(cbm or 0), float(kg or 0), int(bl_id)),
        )
        conn.commit()
    finally:
        conn.close()


def set_batch_plan_ref(batch_id, tab: str, title: str, plan_date: str, kind: str) -> None:
    """Запомнить, из какого блока плана погрузки живёт эта партия —
    чтобы ежедневная сверка не гадала, когда на одну дату два плана."""
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE batches SET plan_tab = ?, plan_title = ?, plan_date = ?, plan_kind = ? WHERE id = ?",
            (str(tab or "")[:60], str(title or "")[:120], str(plan_date or "")[:20],
             str(kind or "")[:16], int(batch_id)),
        )
        conn.commit()
    finally:
        conn.close()


def set_batch_incident(batch_id, note: str) -> None:
    """Kutilmagan vaziyat (поломка фуры, доп. досмотр на границе и т.п.).
    Пустая строка = инцидента нет."""
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE batches SET incident_note = ? WHERE id = ?",
            (str(note or "").strip()[:200], int(batch_id)),
        )
        conn.commit()
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
            CASE WHEN COALESCE(b.client_delivery_date, '') != '' THEN 1 ELSE 0 END AS is_inactive,
            (
                SELECT COUNT(DISTINCT p.bl_id)
                FROM problems p
                WHERE p.batch_id = b.id AND p.status = 'open'
            ) AS problem_count,
            (
                SELECT MAX(sent_at) FROM (
                    SELECT MAX(sl.sent_at) AS sent_at
                    FROM send_logs sl
                    JOIN bl_codes bl ON bl.id = sl.bl_id
                    WHERE bl.batch_id = b.id
                      AND sl.success = 1
                    UNION ALL
                    SELECT MAX(c.sent_at) AS sent_at
                    FROM tracking_delivery_coverage c
                    WHERE c.batch_id = b.id
                )
            ) AS last_tracking_at
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
            CASE WHEN COALESCE(b.client_delivery_date, '') != '' THEN 1 ELSE 0 END AS is_inactive,
            (
                SELECT MAX(sent_at) FROM (
                    SELECT MAX(sl.sent_at) AS sent_at
                    FROM send_logs sl
                    JOIN bl_codes bl ON bl.id = sl.bl_id
                    WHERE bl.batch_id = b.id
                      AND sl.success = 1
                    UNION ALL
                    SELECT MAX(c.sent_at) AS sent_at
                    FROM tracking_delivery_coverage c
                    WHERE c.batch_id = b.id
                )
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
    # send_logs.bl_id is the only bl_codes reference without an ON DELETE
    # clause, so detach those rows first — otherwise deleting any batch
    # that ever had a successful send fails with a FK error. The log rows
    # themselves survive (they keep bl_code/batch_name as plain text).
    conn.execute(
        "UPDATE send_logs SET bl_id = NULL WHERE bl_id IN (SELECT id FROM bl_codes WHERE batch_id = ?)",
        (batch_id,),
    )
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


def _sum_quantity_breakdown(value, fallback=0):
    text = str(value or "").strip()
    if not text:
        return _to_int(fallback)
    parts = re.findall(r"\d+(?:[.,]\d+)?", text)
    if not parts:
        return _to_int(fallback)
    total = 0
    for part in parts:
        try:
            total += int(float(part.replace(",", ".")))
        except (TypeError, ValueError):
            continue
    return total or _to_int(fallback)


def _split_quantity_breakdown_parts(value, fallback=0):
    text = str(value or "").strip()
    if text:
        return [part.replace(",", ".").strip() for part in re.findall(r"\d+(?:[.,]\d+)?", text) if part.strip()]
    fallback_value = _to_int(fallback)
    return [str(fallback_value)] if fallback_value else []


def _merge_quantity_breakdowns(*pairs):
    parts = []
    for value, fallback in pairs:
        parts.extend(_split_quantity_breakdown_parts(value, fallback))
    cleaned = [part for part in parts if str(part or "").strip()]
    return " + ".join(cleaned)


def _split_merged_codes(value):
    text = str(value or "").strip()
    if not text:
        return []
    return [part.strip().upper() for part in text.split(",") if part.strip()]


def _merge_code_aliases(primary_code, primary_aliases, source_code, source_aliases):
    primary_value = str(primary_code or "").strip().upper()
    values = []
    seen = set()
    for candidate in [source_code, *(_split_merged_codes(primary_aliases) + _split_merged_codes(source_aliases))]:
        normalized = str(candidate or "").strip().upper()
        if not normalized or normalized == primary_value or normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalized)
    return ", ".join(values)


def _merge_unique_texts(*values, separator=" / "):
    seen = set()
    result = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return separator.join(result)


def _display_bl_code(value, merged_codes=""):
    primary = str(value or "").strip().upper()
    aliases = _split_merged_codes(merged_codes)
    if not primary:
        return " / ".join(aliases)
    return " / ".join([primary, *aliases]) if aliases else primary


def current_ts():
    return datetime.now(TASHKENT_TZ).strftime("%Y-%m-%d %H:%M:%S")


def parse_local_ts(value: str):
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TASHKENT_TZ)
    except ValueError:
        return None


def _tracking_signature_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value).strip()


def get_tracking_payload_signature(bl: dict, batch: dict | None = None) -> str:
    batch_row = batch or (get_batch(bl.get("batch_id")) if bl.get("batch_id") else {})
    places_breakdown = str(bl.get("quantity_places_breakdown") or "").strip()
    places_value = places_breakdown or _to_int(bl.get("quantity_places"))
    parts = [
        _tracking_signature_value((batch_row or {}).get("name", "")),
        _tracking_signature_value((batch_row or {}).get("eta_to_toshkent", "")),
        _tracking_signature_value((batch_row or {}).get("eta_destination", "")),
        _tracking_signature_value((batch_row or {}).get("client_delivery_date", "")),
        _tracking_signature_value(bl.get("code", "")),
        _tracking_signature_value(bl.get("chat_id", "")),
        _tracking_signature_value(bl.get("status", "")),
        _tracking_signature_value(bl.get("message_language", "")),
        _tracking_signature_value(bl.get("cargo_type", "")),
        _tracking_signature_value(bl.get("weight_kg", 0)),
        _tracking_signature_value(bl.get("volume_cbm", 0)),
        _tracking_signature_value(places_value),
        _tracking_signature_value(bl.get("cargo_description", "")),
    ]
    return hashlib.sha1("\x1f".join(parts).encode("utf-8", errors="ignore")).hexdigest()


def format_response_duration(seconds) -> str:
    total = _to_int(seconds)
    if total <= 0:
        return "0 min"
    days, remainder = divmod(total, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, _ = divmod(remainder, 60)
    parts = []
    if days:
        parts.append(f"{days} kun")
    if hours:
        parts.append(f"{hours} soat")
    if minutes or not parts:
        parts.append(f"{minutes} min")
    return " ".join(parts)


def _telegram_actor_name(user_name="", username="", user_id="") -> str:
    display_name = (user_name or "").strip()
    if display_name:
        return display_name
    username = (username or "").strip()
    if username:
        return f"@{username}"
    return str(user_id or "").strip()


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


def record_moderator_request(
    chat_id,
    chat_title="",
    request_message_id="",
    request_user_id="",
    request_user_name="",
    request_username="",
    request_text="",
    bl_id=None,
    batch_id=None,
    batch_name="",
    requested_at="",
    assigned_moderator_id="",
    assigned_sales_manager_id="",
):
    if not chat_id or request_message_id in (None, ""):
        return False
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO moderator_response_requests(
                chat_id, chat_title, request_message_id,
                request_user_id, request_user_name, request_username, request_text,
                bl_id, batch_id, batch_name, requested_at,
                assigned_moderator_id, assigned_sales_manager_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(chat_id).strip(),
                (chat_title or "").strip(),
                str(request_message_id).strip(),
                str(request_user_id or "").strip(),
                (request_user_name or "").strip(),
                (request_username or "").strip().lstrip("@"),
                (request_text or "").strip(),
                bl_id,
                batch_id,
                (batch_name or "").strip(),
                (requested_at or current_ts()).strip(),
                str(assigned_moderator_id or "").strip(),
                str(assigned_sales_manager_id or "").strip(),
            ),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def mark_moderator_response(
    chat_id,
    request_message_id,
    responder_user_id="",
    responder_name="",
    responder_username="",
    response_text="",
    responded_at="",
    response_role="",
):
    if not chat_id or request_message_id in (None, ""):
        return False
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT id, requested_at, status
            FROM moderator_response_requests
            WHERE chat_id = ? AND request_message_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(chat_id).strip(), str(request_message_id).strip()),
        ).fetchone()
        if not row or row["status"] == "answered":
            return False

        responded_value = (responded_at or current_ts()).strip()
        requested_dt = parse_local_ts(row["requested_at"])
        responded_dt = parse_local_ts(responded_value)
        response_seconds = 0
        if requested_dt and responded_dt:
            response_seconds = max(0, int((responded_dt - requested_dt).total_seconds()))

        conn.execute(
            """
            UPDATE moderator_response_requests
            SET
                responded_at = ?,
                responder_user_id = ?,
                responder_name = ?,
                responder_username = ?,
                response_text = ?,
                response_seconds = ?,
                response_role = ?,
                status = 'answered'
            WHERE id = ?
            """,
            (
                responded_value,
                str(responder_user_id or "").strip(),
                (responder_name or "").strip(),
                (responder_username or "").strip().lstrip("@"),
                (response_text or "").strip(),
                response_seconds,
                (response_role or "").strip(),
                row["id"],
            ),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def get_moderator_response_stats(status="", date_from="", date_to="", role="", limit=300):
    conn = get_conn()
    filters = []
    params = []

    normalized_status = (status or "").strip().lower()
    if normalized_status in {"open", "answered"}:
        filters.append("mr.status = ?")
        params.append(normalized_status)
    if (date_from or "").strip():
        filters.append("date(mr.requested_at) >= date(?)")
        params.append((date_from or "").strip())
    if (date_to or "").strip():
        filters.append("date(mr.requested_at) <= date(?)")
        params.append((date_to or "").strip())

    normalized_role = (role or "").strip().lower()
    role_assignment_sql = ""
    if normalized_role == "moderator":
        role_assignment_sql = "COALESCE(mr.assigned_moderator_id, '') != ''"
    elif normalized_role == "sales_manager":
        role_assignment_sql = "COALESCE(mr.assigned_sales_manager_id, '') != ''"

    if role_assignment_sql:
        filters.append(
            f"((mr.status = 'open' AND {role_assignment_sql}) OR mr.response_role = ?)"
        )
        params.append(normalized_role)

    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""

    summary = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total_requests,
            SUM(CASE WHEN mr.status = 'answered' THEN 1 ELSE 0 END) AS answered_requests,
            SUM(CASE WHEN mr.status = 'open' THEN 1 ELSE 0 END) AS open_requests,
            ROUND(AVG(CASE WHEN mr.status = 'answered' THEN mr.response_seconds END), 1) AS avg_response_seconds,
            MIN(CASE WHEN mr.status = 'answered' THEN mr.response_seconds END) AS fastest_response_seconds,
            MAX(CASE WHEN mr.status = 'answered' THEN mr.response_seconds END) AS slowest_response_seconds
        FROM moderator_response_requests mr
        {where_sql}
        """,
        params,
    ).fetchone()

    rows = conn.execute(
        f"""
        SELECT
            mr.*,
            COALESCE(
                NULLIF(mr.chat_title, ''),
                (
                    SELECT tc.title
                    FROM telegram_chats tc
                    WHERE tc.chat_id = mr.chat_id
                    LIMIT 1
                ),
                mr.chat_id
            ) AS resolved_chat_title,
            COALESCE(NULLIF(req_member.display_name, ''), NULLIF(mr.request_user_name, ''), '') AS resolved_request_user_name,
            COALESCE(NULLIF(req_member.username, ''), NULLIF(mr.request_username, ''), '') AS resolved_request_username,
            COALESCE(NULLIF(resp_member.display_name, ''), NULLIF(mr.responder_name, ''), '') AS resolved_responder_name,
            COALESCE(NULLIF(resp_member.username, ''), NULLIF(mr.responder_username, ''), '') AS resolved_responder_username
        FROM moderator_response_requests mr
        LEFT JOIN telegram_chat_members req_member
            ON req_member.chat_id = mr.chat_id AND req_member.user_id = mr.request_user_id
        LEFT JOIN telegram_chat_members resp_member
            ON resp_member.chat_id = mr.chat_id AND resp_member.user_id = mr.responder_user_id
        {where_sql}
        ORDER BY mr.requested_at DESC, mr.id DESC
        LIMIT ?
        """,
        [*params, _to_int(limit) or 300],
    ).fetchall()
    conn.close()

    row_dicts = []
    for row in rows:
        item = dict(row)
        item["requester_display"] = _telegram_actor_name(
            item.get("resolved_request_user_name"),
            item.get("resolved_request_username"),
            item.get("request_user_id"),
        )
        item["responder_display"] = _telegram_actor_name(
            item.get("resolved_responder_name"),
            item.get("resolved_responder_username"),
            item.get("responder_user_id"),
        )
        item["response_duration"] = format_response_duration(item.get("response_seconds"))
        row_dicts.append(item)

    return {
        "summary": {
            "total_requests": _to_int(summary["total_requests"]) if summary else 0,
            "answered_requests": _to_int(summary["answered_requests"]) if summary else 0,
            "open_requests": _to_int(summary["open_requests"]) if summary else 0,
            "avg_response_seconds": _to_float(summary["avg_response_seconds"]) if summary and summary["avg_response_seconds"] is not None else 0,
            "avg_response_label": format_response_duration(summary["avg_response_seconds"]) if summary and summary["avg_response_seconds"] is not None else "0 min",
            "fastest_response_seconds": _to_int(summary["fastest_response_seconds"]) if summary and summary["fastest_response_seconds"] is not None else 0,
            "fastest_response_label": format_response_duration(summary["fastest_response_seconds"]) if summary and summary["fastest_response_seconds"] is not None else "—",
            "slowest_response_seconds": _to_int(summary["slowest_response_seconds"]) if summary and summary["slowest_response_seconds"] is not None else 0,
            "slowest_response_label": format_response_duration(summary["slowest_response_seconds"]) if summary and summary["slowest_response_seconds"] is not None else "—",
        },
        "role": normalized_role,
        "rows": row_dicts,
    }


def clear_moderator_response_requests():
    conn = get_conn()
    try:
        deleted = conn.execute("SELECT COUNT(*) AS cnt FROM moderator_response_requests").fetchone()
        conn.execute("DELETE FROM moderator_response_requests")
        conn.execute("DELETE FROM sqlite_sequence WHERE name = 'moderator_response_requests'")
        conn.commit()
        return _to_int(deleted["cnt"]) if deleted else 0
    finally:
        conn.close()


def _cargo_labels(language: str) -> dict:
    lang = _normalize_message_language(language)
    if lang == "uz_cyrl":
        return {
            "cargo_type": "Tovar turi",
            "weight": "Оғирлиги",
            "volume": "Ҳажми",
            "places": "Жой сони",
            "description": "Тавсиф",
        }
    if lang == "ru":
        return {
            "cargo_type": "Тип товара",
            "weight": "Вес",
            "volume": "Объём",
            "places": "CTN/件数",
            "description": "Описание",
        }
    return {
        "cargo_type": "Tovar turi",
        "weight": "Og'irligi",
        "volume": "Hajmi",
        "places": "Joy soni",
        "description": "Tavsif",
    }


def format_cargo_info(bl: dict, language: str = DEFAULT_MESSAGE_LANGUAGE) -> str:
    labels = _cargo_labels(language)
    parts = []
    cargo_type = (bl.get("cargo_type") or "").strip()
    if cargo_type:
        parts.append(f"• {labels['cargo_type']}: <b>{html.escape(cargo_type, quote=False)}</b>")

    weight = _to_float(bl.get("weight_kg"))
    if weight:
        parts.append(f"• {labels['weight']}: <b>{weight:g} kg (Umumiy)</b>")

    volume = _to_float(bl.get("volume_cbm"))
    if volume:
        parts.append(f"• {labels['volume']}: <b>{volume:g} m³ (Umumiy)</b>")

    quantity_breakdown = str(bl.get("quantity_places_breakdown") or "").strip()
    if quantity_breakdown:
        parts.append(f"• {labels['places']}: <b>{html.escape(quantity_breakdown, quote=False)}</b>")
    else:
        quantity = _to_int(bl.get("quantity_places"))
        if quantity:
            parts.append(f"• {labels['places']}: <b>{quantity}</b>")

    description = (bl.get("cargo_description") or "").strip()
    if description:
        parts.append(f"• {labels['description']}: <b>{html.escape(description, quote=False)}</b>")

    return "\n".join(parts)


def _cargo_labels(language: str) -> dict:
    lang = _normalize_message_language(language)
    if lang == "uz_cyrl":
        return {
            "cargo_type": "Tovar turi",
            "weight": "Оғирлиги",
            "volume": "Ҳажми",
            "places": "Жой сони",
            "description": "Тавсиф",
        }
    if lang == "ru":
        return {
            "cargo_type": "Тип товара",
            "weight": "Вес",
            "volume": "Объём",
            "places": "Количество мест",
            "description": "Описание",
        }
    return {
        "cargo_type": "Tovar turi",
        "weight": "Og'irligi",
        "volume": "Hajmi",
        "places": "Joy soni",
        "description": "Tavsif",
    }


def _cargo_total_suffix(language: str) -> str:
    lang = _normalize_message_language(language)
    if lang == "uz_cyrl":
        return " (Умумий)"
    if lang == "ru":
        return " (Общий)"
    return " (Umumiy)"


def format_cargo_info(bl: dict, language: str = DEFAULT_MESSAGE_LANGUAGE) -> str:
    labels = _cargo_labels(language)
    total_suffix = _cargo_total_suffix(language)
    parts = []
    cargo_type = (bl.get("cargo_type") or "").strip()
    if cargo_type:
        parts.append(f"• {labels['cargo_type']}: <b>{html.escape(cargo_type, quote=False)}</b>")

    weight = _to_float(bl.get("weight_kg"))
    if weight:
        parts.append(f"• {labels['weight']}: <b>{weight:g} kg{total_suffix}</b>")

    volume = _to_float(bl.get("volume_cbm"))
    if volume:
        parts.append(f"• {labels['volume']}: <b>{volume:g} m³{total_suffix}</b>")

    quantity_breakdown = str(bl.get("quantity_places_breakdown") or "").strip()
    if quantity_breakdown:
        parts.append(f"• {labels['places']}: <b>{html.escape(quantity_breakdown, quote=False)}</b>")
    else:
        quantity = _to_int(bl.get("quantity_places"))
        if quantity:
            parts.append(f"• {labels['places']}: <b>{quantity}</b>")

    description = (bl.get("cargo_description") or "").strip()
    if description:
        parts.append(f"• {labels['description']}: <b>{html.escape(description, quote=False)}</b>")

    return "\n".join(parts)


def _cargo_labels(language: str) -> dict:
    lang = _normalize_message_language(language)
    if lang == "uz_cyrl":
        return {
            "cargo_type": "Товар тури",
            "weight": "Оғирлиги",
            "volume": "Ҳажми",
            "places": "Жой сони",
            "description": "Тавсиф",
        }
    if lang == "ru":
        return {
            "cargo_type": "Тип товара",
            "weight": "Вес",
            "volume": "Объём",
            "places": "Количество мест",
            "description": "Описание",
        }
    if lang == "en":
        return {
            "cargo_type": "Cargo type",
            "weight": "Weight",
            "volume": "Volume",
            "places": "Places",
            "description": "Description",
        }
    return {
        "cargo_type": "Tovar turi",
        "weight": "Og'irligi",
        "volume": "Hajmi",
        "places": "Joy soni",
        "description": "Tavsif",
    }


def _cargo_total_suffix(language: str) -> str:
    lang = _normalize_message_language(language)
    if lang == "uz_cyrl":
        return " (Умумий)"
    if lang == "ru":
        return " (Общий)"
    if lang == "en":
        return " (Total)"
    return " (Umumiy)"


def format_cargo_info(bl: dict, language: str = DEFAULT_MESSAGE_LANGUAGE) -> str:
    labels = _cargo_labels(language)
    total_suffix = _cargo_total_suffix(language)
    parts = []

    cargo_type = (bl.get("cargo_type") or "").strip()
    if cargo_type:
        parts.append(f"• {labels['cargo_type']}: <b>{html.escape(cargo_type, quote=False)}</b>")

    weight = _to_float(bl.get("weight_kg"))
    if weight:
        parts.append(f"• {labels['weight']}: <b>{weight:g} kg{total_suffix}</b>")

    volume = _to_float(bl.get("volume_cbm"))
    if volume:
        parts.append(f"• {labels['volume']}: <b>{volume:g} m³{total_suffix}</b>")

    quantity_breakdown = str(bl.get("quantity_places_breakdown") or "").strip()
    if quantity_breakdown:
        parts.append(f"• {labels['places']}: <b>{html.escape(quantity_breakdown, quote=False)}</b>")
    else:
        quantity = _to_int(bl.get("quantity_places"))
        if quantity:
            parts.append(f"• {labels['places']}: <b>{quantity}</b>")

    description = (bl.get("cargo_description") or "").strip()
    if description:
        parts.append(f"• {labels['description']}: <b>{html.escape(description, quote=False)}</b>")

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


def _inject_today_date_placeholder(template: str) -> str:
    # Строка «Bugungi sana» убрана по решению владельца: ничего не
    # добавляем, а существующую строку с {today_date} вырезаем.
    template = (template or "").replace("\r\n", "\n")
    return re.sub(r"\n?🗓[^\n]*\{today_date\}[^\n]*", "", template)


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
    capture_following = False
    packing_label_variants = {
        "🖇Tovar bo'yicha packing list⤵️",
        "🖇 Tovar bo'yicha packing list ⤵️",
        "📎 Tovar bo'yicha: Packing list",
        "📎Tovar bo'yicha: Packing list",
        "📎 Tovar bo'yicha packing list",
        "📎Tovar bo'yicha packing list",
    }

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()
        if stripped in packing_label_variants:
            capture_following = True
            continue
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

    kept_lines.append("🖇Tovar bo'yicha packing list⤵️")
    kept_lines.append("{packing_list}")
    return "\n".join(kept_lines)


def _normalize_template_value(value):
    if value is None:
        return ""
    return html.escape(str(value), quote=False)


# ──────────────────────────────────────────────────────────────────────────
# BL ↔ Telegram-group link memory (bl_link_history)
# ──────────────────────────────────────────────────────────────────────────
def _normalize_link_code(value) -> str:
    """Canonical key for the link memory: uppercase, alnum only —
    "BL-190" / "bl_190" / "BL 190" all collapse to "BL190"."""
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


# Латинские буквы-двойники кириллицы → их звучание: одно и то же имя
# пишут и по начертанию (CBETKO), и по звучанию (SVETKO).
_LOOKALIKE_TO_SOUND = str.maketrans({"C": "S", "B": "V", "H": "N", "P": "R", "X": "H", "Y": "U"})


def _link_sound_key(value) -> str:
    return _normalize_link_code(value).translate(_LOOKALIKE_TO_SOUND)


def record_bl_link(code, chat_id, batch_id=0):
    """Remember that this BL code was linked to this Telegram group.

    Called from every path that sets a non-empty chat_id on a BL, so the
    memory keeps growing regardless of how the link was made (manual,
    title auto-match or history match)."""
    code_norm = _normalize_link_code(code)
    chat_id = str(chat_id or "").strip()
    if not code_norm or not chat_id:
        return
    conn = get_conn()
    try:
        title_row = conn.execute(
            "SELECT title FROM telegram_chats WHERE chat_id = ?", (chat_id,)
        ).fetchone()
        batch_row = conn.execute(
            "SELECT name FROM batches WHERE id = ?", (int(batch_id or 0),)
        ).fetchone()
        conn.execute(
            """
            INSERT INTO bl_link_history(code_norm, code_raw, chat_id, chat_title, batch_id, batch_name)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(code_norm, chat_id) DO UPDATE SET
                code_raw = excluded.code_raw,
                chat_title = CASE
                    WHEN excluded.chat_title != '' THEN excluded.chat_title
                    ELSE chat_title
                END,
                batch_id = excluded.batch_id,
                batch_name = excluded.batch_name,
                linked_at = datetime('now','localtime')
            """,
            (
                code_norm,
                str(code or "").strip(),
                chat_id,
                (title_row["title"] if title_row else "") or "",
                int(batch_id or 0),
                (batch_row["name"] if batch_row else "") or "",
            ),
        )
        conn.commit()
    except Exception:
        pass  # memory bookkeeping must never break the main flow
    finally:
        conn.close()


def lookup_bl_link_history(code) -> dict | None:
    """Most recent group this BL code was ever linked to (any batch,
    including deleted ones). Tries the BL-prefixed and bare-digit
    variants the same way title matching does."""
    code_norm = _normalize_link_code(code)
    if not code_norm:
        return None
    variants = [code_norm]
    if code_norm.startswith("BL") and code_norm[2:].isdigit():
        variants.append(code_norm[2:])
    elif code_norm.isdigit():
        variants.append(f"BL{code_norm}")
    conn = get_conn()
    try:
        for variant in variants:
            row = conn.execute(
                """
                SELECT code_norm, code_raw, chat_id, chat_title, batch_name, linked_at
                FROM bl_link_history
                WHERE code_norm = ?
                ORDER BY linked_at DESC, id DESC
                LIMIT 1
                """,
                (variant,),
            ).fetchone()
            if row:
                return dict(row)
        # Написание могло смениться: «SVETKO» ↔ «CBETKO» — это один клиент.
        # Ищем по звучанию и принимаем ТОЛЬКО однозначное совпадение,
        # иначе рискуем связать разных клиентов.
        key = _link_sound_key(code_norm)
        if key and not key.isdigit() and len(key) >= 4:
            rows = conn.execute(
                "SELECT code_norm, code_raw, chat_id, chat_title, batch_name, linked_at "
                "FROM bl_link_history ORDER BY linked_at DESC, id DESC"
            ).fetchall()
            hits = [r for r in rows if _link_sound_key(r["code_norm"]) == key]
            if hits and len({r["chat_id"] for r in hits}) == 1:
                return dict(hits[0])
        return None
    finally:
        conn.close()


def language_for_chat(chat_id) -> str:
    """На каком языке эта группа уже получает сообщения — «» если неизвестно.

    Учитываются только ЯВНО выбранные языки: uz_latn стоит по умолчанию у
    каждого нового груза и неотличим от «не выбирали», поэтому голосом не
    считается — иначе автоматика вечно перебивала бы решение человека.
    Берём самый частый, при равенстве — самый свежий.
    """
    chat_id = str(chat_id or "").strip()
    if not chat_id:
        return ""
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT message_language AS lang FROM bl_codes
            WHERE chat_id = ? AND COALESCE(message_language, '') NOT IN ('', ?)
            ORDER BY id DESC
            """,
            (chat_id, DEFAULT_MESSAGE_LANGUAGE),
        ).fetchall()
    finally:
        conn.close()
    counts: dict[str, int] = {}
    order: list[str] = []
    for row in rows:
        lang = _normalize_message_language(row["lang"])
        if lang == DEFAULT_MESSAGE_LANGUAGE:
            continue
        if lang not in counts:
            order.append(lang)
        counts[lang] = counts.get(lang, 0) + 1
    if not counts:
        return ""
    # самый частый; при равенстве — тот, что встретился первым (свежий)
    return max(order, key=lambda l: (counts[l], -order.index(l)))


def set_bl_language(bl_id, language: str) -> bool:
    lang = _normalize_message_language(language)
    conn = get_conn()
    try:
        cur = conn.execute(
            "UPDATE bl_codes SET message_language = ? WHERE id = ?", (lang, int(bl_id))
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_bl_chat_id(bl_id, chat_id) -> bool:
    """Set chat_id on a single BL and record the link in the memory."""
    chat_id = str(chat_id or "").strip()
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT code, batch_id FROM bl_codes WHERE id = ?", (bl_id,)
        ).fetchone()
        if not row:
            return False
        conn.execute("UPDATE bl_codes SET chat_id = ? WHERE id = ?", (chat_id, bl_id))
        conn.commit()
    finally:
        conn.close()
    if chat_id:
        record_bl_link(row["code"], chat_id, row["batch_id"])
    return True


def add_bl(
    batch_id,
    code,
    client_name="",
    chat_id="",
    moderator_tg_id="",
    sales_manager_tg_id="",
    cargo_type="",
    weight_kg=0,
    volume_cbm=0,
    quantity_places=0,
    quantity_places_breakdown="",
    cargo_description="",
    message_language=DEFAULT_MESSAGE_LANGUAGE,
):
    conn = get_conn()
    try:
        normalized_breakdown = str(quantity_places_breakdown or "").strip()
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
                batch_id, code, client_name, chat_id, status, message_language, moderator_tg_id, sales_manager_tg_id,
                cargo_type, weight_kg, volume_cbm, quantity_places, quantity_places_breakdown, cargo_description, expected_date, actual_date, status_updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                batch_id,
                code.upper().strip(),
                client_name.strip(),
                chat_id.strip(),
                batch_status,
                _normalize_message_language(message_language),
                str(moderator_tg_id or "").strip(),
                str(sales_manager_tg_id or "").strip(),
                (cargo_type or "").strip(),
                _to_float(weight_kg),
                _to_float(volume_cbm),
                _sum_quantity_breakdown(normalized_breakdown, quantity_places),
                normalized_breakdown,
                (cargo_description or "").strip(),
                (expected_date or "").strip(),
                (actual_date or "").strip(),
                status_updated_at,
            ),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()
    if chat_id.strip():
        record_bl_link(code, chat_id, batch_id)
    return True


def get_bls_for_packing_list_picker():
    """Every BL in still-active (not client-delivered) batches.

    Used by the bulk packing-list resolver so files can auto-attach and
    be manually pickable across the entire active workload, not just the
    batch the admin happens to have open. The list is sorted by batch
    recency so the current batch's BLs surface at the top.
    """
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            bl.id,
            bl.code,
            bl.merged_codes,
            bl.client_name,
            bl.chat_id,
            bl.batch_id,
            b.name AS batch_name,
            b.created_at AS batch_created_at
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE COALESCE(b.client_delivery_date, '') = ''
        ORDER BY b.created_at DESC, bl.created_at DESC
        """
    ).fetchall()
    conn.close()
    out = []
    for row in rows:
        item = dict(row)
        item["display_code"] = _display_bl_code(item.get("code"), item.get("merged_codes"))
        out.append(item)
    return out


def get_bl_by_batch(batch_id):
    conn = get_conn()
    try:
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
    finally:
        conn.close()

    batch_row = get_batch(batch_id) or {}
    coverage_map = get_tracking_delivery_coverage_map(batch_id)
    exclusion_map = get_batch_send_exclusion_map(batch_id)
    items = [dict(row) for row in rows]
    duplicate_map = get_cross_batch_duplicate_chat_map(
        batch_id,
        [str(item.get("chat_id") or "").strip() for item in items if str(item.get("chat_id") or "").strip()],
    )
    for item in items:
        item_id = _to_int(item.get("id"))
        coverage = coverage_map.get(item_id) or {}
        current_signature = get_tracking_payload_signature(item, batch_row)
        tracking_sent_current = bool(
            coverage and str(coverage.get("tracking_signature") or "") == current_signature
        )
        source_batch_id = _to_int(coverage.get("last_source_batch_id"))
        item["tracking_sent_current"] = tracking_sent_current
        item["tracking_sent_at"] = (coverage.get("sent_at") or "") if tracking_sent_current else ""
        item["tracking_sent_via_batch_id"] = source_batch_id or 0
        item["tracking_sent_via_batch_name"] = (
            (coverage.get("last_source_batch_name") or "") if tracking_sent_current else ""
        )
        item["tracking_sent_via_other_batch"] = bool(
            tracking_sent_current and source_batch_id and source_batch_id != _to_int(batch_id)
        )
        item["send_excluded"] = bool(exclusion_map.get(item_id, False))
        duplicate_info = duplicate_map.get(str(item.get("chat_id") or "").strip()) or {}
        item["has_cross_batch_duplicate"] = bool(duplicate_info)
        item["duplicate_batch_names"] = duplicate_info.get("batch_names") or []
        item["duplicate_codes"] = duplicate_info.get("codes") or []
        item["duplicate_count"] = int(duplicate_info.get("count") or 0)
        item["display_code"] = _display_bl_code(item.get("code"), item.get("merged_codes"))
        item["merged_code_list"] = _split_merged_codes(item.get("merged_codes"))
        item["has_merged_codes"] = bool(item["merged_code_list"])
    return items


def get_bl_by_id(bl_id):
    conn = get_conn()
    row = conn.execute("SELECT * FROM bl_codes WHERE id = ?", (bl_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def find_bl_by_code(code):
    conn = get_conn()
    normalized_code = str(code or "").strip().upper()
    row = conn.execute(
        """
        SELECT bl.*, b.name AS batch_name
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE UPPER(bl.code) = ?
           OR (',' || REPLACE(UPPER(COALESCE(bl.merged_codes, '')), ' ', '') || ',') LIKE '%,' || REPLACE(?, ' ', '') || ',%'
        ORDER BY bl.created_at DESC
        LIMIT 1
        """,
        (normalized_code, normalized_code),
    ).fetchone()
    conn.close()
    item = dict(row) if row else None
    if item:
        item["display_code"] = _display_bl_code(item.get("code"), item.get("merged_codes"))
        item["merged_code_list"] = _split_merged_codes(item.get("merged_codes"))
    return item


def find_latest_bl_by_chat(chat_id):
    conn = get_conn()
    row = conn.execute(
        """
        SELECT bl.*, b.name AS batch_name
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE bl.chat_id = ?
        ORDER BY bl.created_at DESC
        LIMIT 1
        """,
        (str(chat_id),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


# Statuses meaning the cargo is NO LONGER "on the way" (yo'lda): it has
# reached the Tashkent (Chuqursoy ULS) warehouse or was delivered. The
# in-group "Yuk holati" button treats these as "no cargo in transit", so it
# stops replaying tracking for cargo that has already arrived. Deduped,
# ordered tuple so it can be spread into a parametrized SQL IN (...).
ARRIVED_STATUSES = tuple(dict.fromkeys([
    "Toshkent(Chuqursoy ULS da)",
    DELIVERED_STATUS,
    LEGACY_DELIVERED_STATUS,
]))

_ARRIVED_STATUS_PLACEHOLDERS = ", ".join("?" for _ in ARRIVED_STATUSES)

# A batch counts as "still on the way" for the Yuk holati button only if:
#   1. its own status is not an arrived one, AND
#   2. no CLEARLY NEWER batch of the same chat has already arrived.
# Rule 2 catches stale batches operators forgot to advance (e.g. a May
# batch stuck at "Shimkent" while July batches already reached Tashkent —
# that cargo obviously isn't on the road anymore). The grace window keeps
# legitimate overtaking (a slow Kyrgyz-route batch passed by a fast Kazakh
# one) visible: only batches created 14+ days BEFORE an arrived batch are
# considered superseded.
_SUPERSEDED_GRACE_DAYS = 14

_IN_TRANSIT_FILTER_SQL = f"""
          AND COALESCE(b.status, '') NOT IN ({_ARRIVED_STATUS_PLACEHOLDERS})
          AND NOT EXISTS (
              SELECT 1
              FROM bl_codes bl2
              JOIN batches b2 ON b2.id = bl2.batch_id
              WHERE bl2.chat_id = bl.chat_id
                AND (
                    COALESCE(b2.status, '') IN ({_ARRIVED_STATUS_PLACEHOLDERS})
                    OR COALESCE(b2.toshkent_arrived_at, '') != ''
                    OR COALESCE(b2.client_delivery_date, '') != ''
                )
                AND julianday(b2.created_at) - julianday(b.created_at) > {_SUPERSEDED_GRACE_DAYS}
          )
"""

_IN_TRANSIT_FILTER_PARAMS = (*ARRIVED_STATUSES, *ARRIVED_STATUSES)


def find_latest_active_bl_by_chat(chat_id):
    conn = get_conn()
    row = conn.execute(
        """
        SELECT bl.*, b.name AS batch_name
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE bl.chat_id = ?
          AND COALESCE(b.client_delivery_date, '') = ''
        ORDER BY bl.created_at DESC
        LIMIT 1
        """,
        (str(chat_id),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def find_latest_in_transit_bl_by_chat(chat_id):
    """Latest active BL for this chat whose cargo is still on the way.

    Excludes batches that already reached the Tashkent warehouse (or were
    delivered) and stale batches superseded by newer arrived ones — the
    Yuk holati button reports "no cargo in transit" for those.
    """
    conn = get_conn()
    row = conn.execute(
        f"""
        SELECT bl.*, b.name AS batch_name
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE bl.chat_id = ?
          AND COALESCE(b.client_delivery_date, '') = ''
          {_IN_TRANSIT_FILTER_SQL}
        ORDER BY bl.created_at DESC
        LIMIT 1
        """,
        (str(chat_id), *_IN_TRANSIT_FILTER_PARAMS),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def find_active_bls_by_chat(chat_id, in_transit_only: bool = False):
    conn = get_conn()
    status_filter = _IN_TRANSIT_FILTER_SQL if in_transit_only else ""
    params = [str(chat_id)]
    if in_transit_only:
        params.extend(_IN_TRANSIT_FILTER_PARAMS)
    rows = conn.execute(
        f"""
        SELECT bl.*, b.name AS batch_name
        FROM bl_codes bl
        JOIN batches b ON b.id = bl.batch_id
        WHERE bl.chat_id = ?
          AND COALESCE(b.client_delivery_date, '') = ''
          {status_filter}
        ORDER BY b.created_at DESC, bl.created_at DESC, bl.id DESC
        """,
        params,
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_tracking_bundle_bls(primary_bl: dict, include_related_batches: bool = True, in_transit_only: bool = False) -> list[dict]:
    if not primary_bl:
        return []
    primary_id = primary_bl.get("id")
    chat_id = str(primary_bl.get("chat_id") or "").strip()
    if not primary_id or not chat_id:
        return [dict(primary_bl)]
    if not include_related_batches:
        return [dict(primary_bl)]

    ordered = [dict(primary_bl)]
    seen_ids = {primary_id}
    exclusion_cache: dict[int, dict[int, bool]] = {}
    # in_transit_only drops sibling batches that already arrived at the
    # Tashkent warehouse, so the Yuk holati button doesn't drag an arrived
    # cargo back into a "still on the way" reply.
    related_rows = find_active_bls_by_chat(chat_id, in_transit_only=in_transit_only)
    coverage_map = get_tracking_delivery_coverage_for_bl_ids(
        [_to_int(item.get("id")) for item in related_rows if _to_int(item.get("id"))]
    )
    batch_cache: dict[int, dict] = {}
    for related in related_rows:
        related_id = related.get("id")
        if not related_id or related_id in seen_ids:
            continue
        related_batch_id = _to_int(related.get("batch_id"))
        if related_batch_id:
            if related_batch_id not in exclusion_cache:
                exclusion_cache[related_batch_id] = get_batch_send_exclusion_map(related_batch_id)
            if exclusion_cache[related_batch_id].get(_to_int(related_id), False):
                continue
            batch_row = batch_cache.get(related_batch_id)
            if batch_row is None:
                batch_row = get_batch(related_batch_id) or {}
                batch_cache[related_batch_id] = batch_row
            related_signature = get_tracking_payload_signature(related, batch_row)
            current_coverage = coverage_map.get(_to_int(related_id)) or {}
            if str(current_coverage.get("tracking_signature") or "") == related_signature:
                continue
        ordered.append(dict(related))
        seen_ids.add(related_id)
    return ordered


def get_tracking_delivery_coverage_map(batch_id: int) -> dict[int, dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                c.bl_id,
                c.batch_id,
                c.chat_id,
                c.last_source_batch_id,
                c.last_source_bl_id,
                c.tracking_signature,
                c.sent_at,
                sb.name AS last_source_batch_name
            FROM tracking_delivery_coverage c
            LEFT JOIN batches sb ON sb.id = c.last_source_batch_id
            WHERE c.batch_id = ?
            """,
            (batch_id,),
        ).fetchall()
        return {int(row["bl_id"]): dict(row) for row in rows}
    finally:
        conn.close()


def get_tracking_delivery_coverage_for_bl_ids(bl_ids: list[int]) -> dict[int, dict]:
    cleaned = [int(bl_id) for bl_id in (bl_ids or []) if _to_int(bl_id)]
    if not cleaned:
        return {}
    placeholders = ",".join("?" for _ in cleaned)
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT
                c.bl_id,
                c.batch_id,
                c.chat_id,
                c.last_source_batch_id,
                c.last_source_bl_id,
                c.tracking_signature,
                c.sent_at,
                sb.name AS last_source_batch_name
            FROM tracking_delivery_coverage c
            LEFT JOIN batches sb ON sb.id = c.last_source_batch_id
            WHERE c.bl_id IN ({placeholders})
            """,
            tuple(cleaned),
        ).fetchall()
        return {int(row["bl_id"]): dict(row) for row in rows}
    finally:
        conn.close()


def get_cross_batch_duplicate_chat_map(batch_id: int, chat_ids: list[str]) -> dict[str, dict]:
    cleaned = [str(chat_id).strip() for chat_id in (chat_ids or []) if str(chat_id).strip()]
    if not cleaned:
        return {}
    placeholders = ",".join("?" for _ in cleaned)
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT
                bl.chat_id,
                bl.code,
                bl.batch_id,
                b.name AS batch_name
            FROM bl_codes bl
            JOIN batches b ON b.id = bl.batch_id
            WHERE bl.chat_id IN ({placeholders})
              AND bl.batch_id != ?
              AND COALESCE(b.client_delivery_date, '') = ''
            ORDER BY b.created_at DESC, bl.created_at DESC, bl.id DESC
            """,
            (*cleaned, batch_id),
        ).fetchall()
    finally:
        conn.close()

    result: dict[str, dict] = {}
    for row in rows:
        chat_id = str(row["chat_id"] or "").strip()
        if not chat_id:
            continue
        entry = result.setdefault(chat_id, {"count": 0, "batch_names": [], "codes": []})
        entry["count"] += 1
        batch_name = str(row["batch_name"] or "").strip()
        code = str(row["code"] or "").strip()
        if batch_name and batch_name not in entry["batch_names"]:
            entry["batch_names"].append(batch_name)
        if code and code not in entry["codes"]:
            entry["codes"].append(code)
    return result


def get_batch_send_exclusion_map(batch_id: int) -> dict[int, bool]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT bl_id, is_excluded
            FROM batch_send_exclusions
            WHERE batch_id = ?
            """,
            (batch_id,),
        ).fetchall()
        return {int(row["bl_id"]): bool(row["is_excluded"]) for row in rows}
    finally:
        conn.close()


def get_batch_send_exclusion_sources(batch_id: int) -> dict[int, str]:
    """bl_id → 'manual' | 'plan' для действующих исключений партии —
    отчёту рассылки нужно объяснять, ПОЧЕМУ BL пропущен."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT bl_id, source
            FROM batch_send_exclusions
            WHERE batch_id = ? AND is_excluded = 1
            """,
            (batch_id,),
        ).fetchall()
        return {int(row["bl_id"]): str(row["source"] or "manual") for row in rows}
    finally:
        conn.close()


def set_batch_send_exclusion(bl_id: int, excluded: bool, source: str = "manual") -> dict:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, batch_id, code FROM bl_codes WHERE id = ? LIMIT 1",
            (bl_id,),
        ).fetchone()
        if not row:
            raise ValueError("BL не найден")
        batch_id = int(row["batch_id"])
        if excluded:
            conn.execute(
                """
                INSERT INTO batch_send_exclusions(batch_id, bl_id, is_excluded, source, updated_at)
                VALUES (?, ?, 1, ?, ?)
                ON CONFLICT(batch_id, bl_id) DO UPDATE SET
                    is_excluded = 1,
                    source = excluded.source,
                    updated_at = excluded.updated_at
                """,
                (batch_id, bl_id, str(source or "manual"), current_ts()),
            )
        else:
            conn.execute(
                "DELETE FROM batch_send_exclusions WHERE batch_id = ? AND bl_id = ?",
                (batch_id, bl_id),
            )
        conn.commit()
        return {
            "bl_id": bl_id,
            "batch_id": batch_id,
            "code": row["code"],
            "excluded": bool(excluded),
        }
    finally:
        conn.close()


def record_tracking_delivery(primary_bl: dict, include_related_batches: bool = True) -> list[dict]:
    bundle = get_tracking_bundle_bls(primary_bl, include_related_batches=include_related_batches)
    if not bundle:
        return []

    chat_id = str(primary_bl.get("chat_id") or "").strip()
    source_batch_id = _to_int(primary_bl.get("batch_id"))
    source_bl_id = _to_int(primary_bl.get("id"))
    sent_at = current_ts()
    batch_cache: dict[int, dict] = {}

    conn = get_conn()
    try:
        for item in bundle:
            batch_id = _to_int(item.get("batch_id"))
            if not batch_id:
                continue
            batch_row = batch_cache.get(batch_id)
            if batch_row is None:
                batch_row = get_batch(batch_id) or {}
                batch_cache[batch_id] = batch_row
            signature = get_tracking_payload_signature(item, batch_row)
            conn.execute(
                """
                INSERT INTO tracking_delivery_coverage(
                    bl_id,
                    batch_id,
                    chat_id,
                    last_source_batch_id,
                    last_source_bl_id,
                    tracking_signature,
                    sent_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(bl_id) DO UPDATE SET
                    batch_id = excluded.batch_id,
                    chat_id = excluded.chat_id,
                    last_source_batch_id = excluded.last_source_batch_id,
                    last_source_bl_id = excluded.last_source_bl_id,
                    tracking_signature = excluded.tracking_signature,
                    sent_at = excluded.sent_at
                """,
                (
                    item.get("id"),
                    batch_id,
                    chat_id,
                    source_batch_id or None,
                    source_bl_id or None,
                    signature,
                    sent_at,
                ),
            )
        conn.commit()
        return bundle
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────
# Sent-message catalogue for the "recall last tracking" feature.
# Stores every chat_id + message_id we've sent so the operator can later
# ask the bot to delete them via Telegram's deleteMessage API.
# ─────────────────────────────────────────────────────────────────────────
def record_sent_telegram_message(
    *,
    bl_id: int | None,
    batch_id: int | None,
    chat_id: str | int,
    message_id: int | None,
    kind: str = "tracking",
) -> None:
    """Remember a freshly-sent Telegram message so we can recall it later.

    Safe to call from background threads — opens its own connection. Silent
    no-op on bad inputs (a missing message_id just means we'd have no way
    to call deleteMessage anyway, so there's nothing to record).
    """
    try:
        msg_id = int(message_id) if message_id is not None else 0
    except (TypeError, ValueError):
        msg_id = 0
    if not msg_id:
        return
    chat_value = str(chat_id or "").strip()
    if not chat_value:
        return
    try:
        bl_value = int(bl_id) if bl_id is not None else None
    except (TypeError, ValueError):
        bl_value = None
    try:
        batch_value = int(batch_id) if batch_id is not None else None
    except (TypeError, ValueError):
        batch_value = None
    safe_kind = "file" if str(kind).lower() == "file" else "tracking"

    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO tracking_sent_messages(
                bl_id, batch_id, chat_id, message_id, kind, sent_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (bl_value, batch_value, chat_value, msg_id, safe_kind, current_ts()),
        )
        conn.commit()
    except sqlite3.OperationalError:
        # Disk-IO hiccup — don't crash the send path. The Telegram message
        # already went out; we just lose the ability to recall this one.
        pass
    finally:
        conn.close()


def get_recallable_messages(
    *,
    batch_id: int | None = None,
    bl_ids: list[int] | None = None,
    only_unrecalled: bool = True,
) -> list[dict]:
    """List messages eligible for recall, newest first.

    Filters:
      - batch_id  : restrict to one batch's messages.
      - bl_ids    : restrict to a list of BLs (useful for per-row recall).
      - only_unrecalled (default True): skip rows already deleted.
    """
    where = []
    params: list = []
    if batch_id is not None:
        where.append("batch_id = ?")
        params.append(int(batch_id))
    if bl_ids:
        cleaned = [int(b) for b in bl_ids if b is not None]
        if cleaned:
            placeholders = ",".join("?" for _ in cleaned)
            where.append(f"bl_id IN ({placeholders})")
            params.extend(cleaned)
    if only_unrecalled:
        where.append("COALESCE(recalled_at, '') = ''")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""
            SELECT id, bl_id, batch_id, chat_id, message_id, kind,
                   sent_at, recalled_at, recall_error
            FROM tracking_sent_messages
            {where_sql}
            ORDER BY sent_at DESC, id DESC
            """,
            tuple(params),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_message_recalled(message_row_id: int, error: str = "") -> None:
    """Tag a single tracking_sent_messages row as recalled (or note its error)."""
    try:
        rid = int(message_row_id)
    except (TypeError, ValueError):
        return
    conn = get_conn()
    try:
        if error:
            conn.execute(
                """
                UPDATE tracking_sent_messages
                SET recall_error = ?
                WHERE id = ?
                """,
                (str(error)[:500], rid),
            )
        else:
            conn.execute(
                """
                UPDATE tracking_sent_messages
                SET recalled_at = ?, recall_error = ''
                WHERE id = ?
                """,
                (current_ts(), rid),
            )
        conn.commit()
    finally:
        conn.close()


def clear_tracking_delivery_coverage(bl_ids: list[int]) -> int:
    """After a successful recall, wipe the delivery-coverage row so the BL
    is treated as "needs to send tracking again" on the next batch send."""
    cleaned = [int(b) for b in (bl_ids or []) if b is not None]
    if not cleaned:
        return 0
    placeholders = ",".join("?" for _ in cleaned)
    conn = get_conn()
    try:
        cur = conn.execute(
            f"DELETE FROM tracking_delivery_coverage WHERE bl_id IN ({placeholders})",
            tuple(cleaned),
        )
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()


def update_bl(
    bl_id,
    code,
    client_name,
    chat_id,
    status=None,
    moderator_tg_id="",
    sales_manager_tg_id="",
    cargo_type="",
    weight_kg=0,
    volume_cbm=0,
    quantity_places=0,
    quantity_places_breakdown="",
    cargo_description="",
    message_language=DEFAULT_MESSAGE_LANGUAGE,
):
    conn = get_conn()
    normalized_breakdown = str(quantity_places_breakdown or "").strip()
    current = conn.execute(
        "SELECT batch_id, status FROM bl_codes WHERE id = ?",
        (bl_id,),
    ).fetchone()
    if not current:
        conn.close()
        raise ValueError("BL не найден")

    normalized_code = (code or "").strip().upper()
    if not normalized_code:
        conn.close()
        raise ValueError("BL код обязателен")

    duplicate = conn.execute(
        """
        SELECT 1
        FROM bl_codes
        WHERE batch_id = ? AND UPPER(code) = UPPER(?) AND id != ?
        LIMIT 1
        """,
        (current["batch_id"], normalized_code, bl_id),
    ).fetchone()
    if duplicate:
        conn.close()
        raise ValueError("В этой партии уже есть такой BL код")

    effective_status = (status if status is not None else (current["status"] if current else "Xitoy")) or "Xitoy"
    conn.execute(
        """
        UPDATE bl_codes
        SET
            code = ?,
            client_name = ?,
            chat_id = ?,
            status = ?,
            message_language = ?,
            moderator_tg_id = ?,
            sales_manager_tg_id = ?,
            cargo_type = ?,
            weight_kg = ?,
            volume_cbm = ?,
            quantity_places = ?,
            quantity_places_breakdown = ?,
            cargo_description = ?,
            status_updated_at = CASE
                WHEN status != ? THEN datetime('now','localtime')
                ELSE status_updated_at
            END
        WHERE id = ?
        """,
        (
            normalized_code,
            client_name.strip(),
            chat_id.strip(),
            effective_status,
            _normalize_message_language(message_language),
            str(moderator_tg_id or "").strip(),
            str(sales_manager_tg_id or "").strip(),
            (cargo_type or "").strip(),
            _to_float(weight_kg),
            _to_float(volume_cbm),
            _sum_quantity_breakdown(normalized_breakdown, quantity_places),
            normalized_breakdown,
            (cargo_description or "").strip(),
            effective_status,
            bl_id,
        ),
    )
    conn.commit()
    conn.close()
    if chat_id.strip():
        record_bl_link(normalized_code, chat_id, current["batch_id"])


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
        # Груз переезжает ЦЕЛИКОМ: исключение из рассылки и учёт последней
        # отправки трекинга следуют за BL (иначе замолчанный клиент снова
        # получит сообщения, а у партии-донора «последний трекинг» будет
        # считать чужой груз).
        try:
            conn.execute(
                "DELETE FROM batch_send_exclusions WHERE batch_id = ? AND bl_id = ?",
                (target_batch_id, bl_id),
            )
            conn.execute(
                "UPDATE batch_send_exclusions SET batch_id = ? WHERE bl_id = ? AND batch_id = ?",
                (target_batch_id, bl_id, current_batch_id),
            )
        except sqlite3.Error:
            pass
        try:
            conn.execute(
                "UPDATE tracking_delivery_coverage SET batch_id = ? WHERE bl_id = ? AND batch_id = ?",
                (target_batch_id, bl_id, current_batch_id),
            )
        except sqlite3.Error:
            pass

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


def merge_bl_into_target(source_bl_id, target_bl_id):
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        source = conn.execute("SELECT * FROM bl_codes WHERE id = ?", (source_bl_id,)).fetchone()
        target = conn.execute("SELECT * FROM bl_codes WHERE id = ?", (target_bl_id,)).fetchone()
        if not source or not target:
            raise ValueError("BL не найден")
        if _to_int(source["id"]) == _to_int(target["id"]):
            raise ValueError("Нельзя объединить BL сам с собой")
        if _to_int(source["batch_id"]) != _to_int(target["batch_id"]):
            raise ValueError("Объединение возможно только внутри одной партии")

        merged_breakdown = _merge_quantity_breakdowns(
            (target["quantity_places_breakdown"], target["quantity_places"]),
            (source["quantity_places_breakdown"], source["quantity_places"]),
        )
        merged_codes = _merge_code_aliases(
            target["code"],
            target["merged_codes"],
            source["code"],
            source["merged_codes"],
        )
        merged_cargo_type = _merge_unique_texts(target["cargo_type"], source["cargo_type"])
        merged_description = _merge_unique_texts(
            target["cargo_description"],
            source["cargo_description"],
            separator=" | ",
        )
        merged_client_name = str(target["client_name"] or "").strip() or str(source["client_name"] or "").strip()
        merged_chat_id = str(target["chat_id"] or "").strip() or str(source["chat_id"] or "").strip()
        merged_language = _normalize_message_language(
            str(target["message_language"] or "").strip() or str(source["message_language"] or "").strip()
        )
        merged_moderator = str(target["moderator_tg_id"] or "").strip() or str(source["moderator_tg_id"] or "").strip()
        merged_sales = str(target["sales_manager_tg_id"] or "").strip() or str(source["sales_manager_tg_id"] or "").strip()

        conn.execute(
            """
            UPDATE bl_codes
            SET
                merged_codes = ?,
                client_name = ?,
                chat_id = ?,
                message_language = ?,
                moderator_tg_id = ?,
                sales_manager_tg_id = ?,
                cargo_type = ?,
                weight_kg = ?,
                volume_cbm = ?,
                quantity_places = ?,
                quantity_places_breakdown = ?,
                cargo_description = ?,
                status_updated_at = datetime('now','localtime')
            WHERE id = ?
            """,
            (
                merged_codes,
                merged_client_name,
                merged_chat_id,
                merged_language,
                merged_moderator,
                merged_sales,
                merged_cargo_type,
                _to_float(target["weight_kg"]) + _to_float(source["weight_kg"]),
                _to_float(target["volume_cbm"]) + _to_float(source["volume_cbm"]),
                _sum_quantity_breakdown(
                    merged_breakdown,
                    _to_int(target["quantity_places"]) + _to_int(source["quantity_places"]),
                ),
                merged_breakdown,
                merged_description,
                target_bl_id,
            ),
        )

        for table_name in ("files", "problems"):
            conn.execute(f"UPDATE {table_name} SET bl_id = ? WHERE bl_id = ?", (target_bl_id, source_bl_id))

        for table_name in (
            "send_logs",
            "communication_survey_sends",
            "communication_survey_dispatches",
            "communication_ratings",
            "communication_rating_events",
        ):
            conn.execute(f"UPDATE {table_name} SET bl_id = ? WHERE bl_id = ?", (target_bl_id, source_bl_id))

        target_excluded = conn.execute(
            "SELECT is_excluded FROM batch_send_exclusions WHERE batch_id = ? AND bl_id = ?",
            (target["batch_id"], target_bl_id),
        ).fetchone()
        source_excluded = conn.execute(
            "SELECT is_excluded FROM batch_send_exclusions WHERE batch_id = ? AND bl_id = ?",
            (source["batch_id"], source_bl_id),
        ).fetchone()
        if (target_excluded and _to_int(target_excluded["is_excluded"])) or (source_excluded and _to_int(source_excluded["is_excluded"])):
            conn.execute(
                """
                INSERT INTO batch_send_exclusions(batch_id, bl_id, is_excluded, updated_at)
                VALUES (?, ?, 1, datetime('now','localtime'))
                ON CONFLICT(batch_id, bl_id) DO UPDATE SET
                    is_excluded = 1,
                    updated_at = datetime('now','localtime')
                """,
                (target["batch_id"], target_bl_id),
            )
        conn.execute("DELETE FROM batch_send_exclusions WHERE batch_id = ? AND bl_id = ?", (source["batch_id"], source_bl_id))

        conn.execute("DELETE FROM tracking_delivery_coverage WHERE bl_id IN (?, ?)", (source_bl_id, target_bl_id))
        conn.execute("DELETE FROM bl_codes WHERE id = ?", (source_bl_id,))
        conn.commit()
        return {
            "source_bl_id": int(source_bl_id),
            "target_bl_id": int(target_bl_id),
            "target_code": target["code"],
            "display_code": _display_bl_code(target["code"], merged_codes),
            "batch_id": int(target["batch_id"]),
        }
    finally:
        conn.close()


def delete_bl(bl_id):
    file_paths = []
    last_exc = None
    for attempt in range(8):
        conn = get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
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

            file_paths = [row["file_path"] for row in file_rows if row["file_path"]]
            last_exc = None
            break
        except sqlite3.OperationalError as exc:
            conn.rollback()
            last_exc = exc
            if not _is_locked_error(exc) or attempt == 7:
                raise
            time.sleep(0.35 * (attempt + 1))
        finally:
            conn.close()

    if last_exc:
        raise last_exc

    for file_path in file_paths:
        try:
            os.remove(file_path)
        except OSError:
            pass


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


def move_file_to_bl(file_id, target_bl_id) -> dict:
    """Перепривязать файл к другому BL (ошиблись при авто-прикреплении).
    Сам файл на диске остаётся тем же — меняется только владелец."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, bl_id, filename FROM files WHERE id = ?", (int(file_id),)
        ).fetchone()
        if not row:
            raise ValueError("Файл не найден")
        target = conn.execute(
            "SELECT id, code, batch_id FROM bl_codes WHERE id = ?", (int(target_bl_id),)
        ).fetchone()
        if not target:
            raise ValueError("Целевой BL не найден")
        if int(row["bl_id"] or 0) == int(target_bl_id):
            return {"moved": False, "reason": "already", "filename": row["filename"]}
        dup = conn.execute(
            "SELECT 1 FROM files WHERE bl_id = ? AND LOWER(filename) = LOWER(?) LIMIT 1",
            (int(target_bl_id), row["filename"]),
        ).fetchone()
        if dup:
            raise ValueError("У целевого BL уже есть файл с таким именем")
        conn.execute(
            "UPDATE files SET bl_id = ? WHERE id = ?", (int(target_bl_id), int(file_id))
        )
        conn.commit()
        return {
            "moved": True, "filename": row["filename"],
            "from_bl_id": int(row["bl_id"] or 0), "to_bl_id": int(target_bl_id),
            "to_code": target["code"],
        }
    finally:
        conn.close()


def get_file_by_public_token(public_token):
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM files WHERE public_token = ? LIMIT 1",
        ((public_token or "").strip(),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def set_file_tg_file_id(file_id: int, tg_file_id: str) -> None:
    """Remember Telegram's CDN file_id so future taps skip the re-upload."""
    try:
        fid = int(file_id)
    except (TypeError, ValueError):
        return
    value = (tg_file_id or "").strip()
    if not value:
        return
    conn = get_conn()
    try:
        conn.execute("UPDATE files SET tg_file_id = ? WHERE id = ?", (value, fid))
        conn.commit()
    finally:
        conn.close()


def clear_file_tg_file_id(file_id: int) -> None:
    """Invalidate a stale Telegram file_id (e.g. Telegram dropped the CDN copy)."""
    try:
        fid = int(file_id)
    except (TypeError, ValueError):
        return
    conn = get_conn()
    try:
        conn.execute("UPDATE files SET tg_file_id = '' WHERE id = ?", (fid,))
        conn.commit()
    finally:
        conn.close()


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


def format_packing_list(bl_id, language: str = DEFAULT_MESSAGE_LANGUAGE) -> str:
    normalized_language = _normalize_message_language(language)
    files = get_files(bl_id)
    if not files:
        if normalized_language == "uz_cyrl":
            return "Packing list бириктирилмаган"
        if normalized_language == "ru":
            return "Packing list не прикреплён"
        return "Packing list biriktirilmagan"
    items = []
    for file_info in files:
        name = (file_info.get("filename") or "").strip()
        if not name:
            continue
        items.append(f"• {html.escape(prettify_file_name(name))}")
    if not items:
        if normalized_language == "uz_cyrl":
            return "Packing list бириктирилмаган"
        if normalized_language == "ru":
            return "Packing list не прикреплён"
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


def add_log(bl_id, bl_code, batch_name, chat_id, status, success, error_msg="",
            filled_by="", confirmed_by="", sent_via=""):
    conn = get_conn()
    sent_at = current_ts()
    conn.execute(
        """
        INSERT INTO send_logs(bl_id, bl_code, batch_name, chat_id, status, success,
                              error_msg, filled_by, confirmed_by, sent_via, sent_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (bl_id, bl_code, batch_name, chat_id, status, 1 if success else 0, error_msg,
         str(filled_by or ""), str(confirmed_by or ""), str(sent_via or ""), sent_at),
    )
    conn.commit()
    conn.close()


def drive_files_seen(file_ids) -> set:
    """Какие файлы Drive бот уже обрабатывал (чтобы не качать и не
    переспрашивать по кругу при каждом автосканировании)."""
    ids = [str(x) for x in file_ids if x]
    if not ids:
        return set()
    conn = get_conn()
    try:
        out = set()
        for i in range(0, len(ids), 400):
            chunk = ids[i:i + 400]
            marks = ", ".join("?" for _ in chunk)
            rows = conn.execute(
                f"SELECT file_id FROM drive_seen_files WHERE file_id IN ({marks})", chunk
            ).fetchall()
            out.update(str(r["file_id"]) for r in rows)
        return out
    finally:
        conn.close()


def mark_drive_file_seen(file_id: str, name: str = "", folder_name: str = "") -> None:
    conn = get_conn()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO drive_seen_files(file_id, name, folder_name) VALUES (?, ?, ?)",
            (str(file_id), str(name or ""), str(folder_name or "")),
        )
        conn.commit()
    finally:
        conn.close()


def add_packing_question(filename: str, file_path: str, chat_id: str) -> int:
    """Файл, для которого бот не нашёл BL: откладываем и спрашиваем людей."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO packing_file_questions(filename, file_path, chat_id) VALUES (?, ?, ?)",
            (str(filename), str(file_path), str(chat_id)),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def set_packing_question_message(question_id: int, message_id) -> None:
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE packing_file_questions SET message_id = ? WHERE id = ?",
            (str(message_id), int(question_id)),
        )
        conn.commit()
    finally:
        conn.close()


def find_packing_question_by_message(chat_id, message_id):
    """Ожидающий ответа вопрос по сообщению, на которое ответили reply."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM packing_file_questions "
            "WHERE chat_id = ? AND message_id = ? AND status = 'pending' LIMIT 1",
            (str(chat_id), str(message_id)),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def list_packing_questions(status: str = "pending", limit: int = 50) -> list:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM packing_file_questions WHERE status = ? ORDER BY id DESC LIMIT ?",
            (str(status), int(limit)),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def resolve_packing_question(question_id: int, status: str, answer: str = "", bl_id=None) -> bool:
    """CAS: закрыть вопрос может только тот, кто успел первым."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "UPDATE packing_file_questions SET status = ?, answer = ?, bl_id = ?, "
            "resolved_at = datetime('now','localtime') WHERE id = ? AND status = 'pending'",
            (str(status), str(answer or ""), bl_id, int(question_id)),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def clear_plan_send_exclusion(bl_id: int) -> bool:
    """Снять исключение, поставленное БОТОМ (source='plan'), когда груз
    снова появился в казахском плане. Ручные исключения не трогаем."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "DELETE FROM batch_send_exclusions WHERE bl_id = ? AND source = 'plan'",
            (int(bl_id),),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def get_last_tracking_send(bl_id=None, batch_name=None):
    """Последняя УСПЕШНАЯ отправка трекинга: когда и кто дал ей ход
    (filled_by/confirmed_by/sent_via). Для ответа «когда/кто обновил
    трекинг» — это НЕ status_updated_at (то — когда менялся статус)."""
    conn = get_conn()
    try:
        if bl_id is not None:
            row = conn.execute(
                "SELECT sent_at, filled_by, confirmed_by, sent_via, status "
                "FROM send_logs WHERE bl_id = ? AND success = 1 ORDER BY id DESC LIMIT 1",
                (int(bl_id),),
            ).fetchone()
        elif batch_name is not None:
            row = conn.execute(
                "SELECT sent_at, filled_by, confirmed_by, sent_via, status "
                "FROM send_logs WHERE batch_name = ? AND success = 1 ORDER BY id DESC LIMIT 1",
                (str(batch_name),),
            ).fetchone()
        else:
            row = None
    finally:
        conn.close()
    return dict(row) if row else None


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


def clear_dashboard_history():
    conn = get_conn()
    try:
        deleted = conn.execute(
            "SELECT COUNT(*) AS cnt FROM send_logs"
        ).fetchone()
        conn.execute("DELETE FROM send_logs")
        conn.execute("DELETE FROM sqlite_sequence WHERE name = 'send_logs'")
        conn.commit()
        return _to_int(deleted["cnt"]) if deleted else 0
    finally:
        conn.close()


def get_setting(key: str, default: str = "") -> str:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = ?",
            (str(key or "").strip(),),
        ).fetchone()
        if not row:
            return default
        return str(row["value"] or "")
    finally:
        conn.close()


def set_setting(key: str, value: str) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO app_settings(key, value, updated_at)
            VALUES (?, ?, datetime('now','localtime'))
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = datetime('now','localtime')
            """,
            (str(key or "").strip(), str(value or "").strip()),
        )
        conn.commit()
    finally:
        conn.close()


def get_global_ai_enabled() -> bool:
    return get_setting("global_ai_enabled", "0") == "1"


def set_global_ai_enabled(enabled: bool) -> bool:
    set_setting("global_ai_enabled", "1" if enabled else "0")
    return bool(enabled)


def toggle_global_ai_enabled() -> bool:
    new_value = not get_global_ai_enabled()
    set_global_ai_enabled(new_value)
    return new_value


def get_chat_ai_enabled(chat_id) -> bool:
    chat_value = str(chat_id or "").strip()
    if not chat_value:
        return False
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT ai_enabled FROM telegram_chats WHERE chat_id = ? LIMIT 1",
            (chat_value,),
        ).fetchone()
        return bool(row and _to_int(row["ai_enabled"]))
    finally:
        conn.close()


def set_chat_ai_enabled(chat_id, enabled: bool):
    chat_value = str(chat_id or "").strip()
    if not chat_value:
        return False
    enabled_value = 1 if enabled else 0
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO telegram_chats(
                chat_id, title, chat_type, username, moderator_tg_id, sales_manager_tg_id,
                ai_enabled, is_active, created_at, last_seen_at, updated_at
            )
            VALUES (?, '', 'group', '', '', '', ?, 1, datetime('now','localtime'), datetime('now','localtime'), datetime('now','localtime'))
            ON CONFLICT(chat_id) DO UPDATE SET
                ai_enabled = excluded.ai_enabled,
                updated_at = datetime('now','localtime')
            """,
            (chat_value, enabled_value),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def toggle_chat_ai_enabled(chat_id) -> bool:
    current = get_chat_ai_enabled(chat_id)
    set_chat_ai_enabled(chat_id, not current)
    return not current


def get_announcement_template() -> str:
    return get_setting("announcement_template", DEFAULT_ANNOUNCEMENT_TEMPLATE) or DEFAULT_ANNOUNCEMENT_TEMPLATE


def save_announcement_template(content: str) -> None:
    set_setting("announcement_template", (content or "").strip())


def _normalize_announcement_attachment_kind(kind: str) -> str:
    value = (kind or "").strip().lower()
    if value in ("photo", "animation", "video"):
        return value
    return "document"


def get_announcement_send_options() -> dict:
    """How announcement text + media should be delivered.

    delivery_mode: "together" (one message when Telegram allows) or
                   "separate" (always two messages).
    media_order:   "media_first" | "text_first" — order of the
                   two-message form (also used as the fallback order
                   when "together" doesn't fit the caption limits).
    """
    mode = (get_setting("announcement_delivery_mode", "together") or "").strip().lower()
    order = (get_setting("announcement_media_order", "media_first") or "").strip().lower()
    return {
        "delivery_mode": mode if mode in ("together", "separate") else "together",
        "media_order": order if order in ("media_first", "text_first") else "media_first",
    }


def save_announcement_send_options(delivery_mode: str, media_order: str) -> None:
    mode = (delivery_mode or "").strip().lower()
    order = (media_order or "").strip().lower()
    set_setting(
        "announcement_delivery_mode",
        mode if mode in ("together", "separate") else "together",
    )
    set_setting(
        "announcement_media_order",
        order if order in ("media_first", "text_first") else "media_first",
    )


def get_announcement_attachment() -> dict:
    filename = get_setting(ANNOUNCEMENT_ATTACHMENT_NAME_KEY, "").strip()
    file_path = get_setting(ANNOUNCEMENT_ATTACHMENT_PATH_KEY, "").strip()
    kind = _normalize_announcement_attachment_kind(get_setting(ANNOUNCEMENT_ATTACHMENT_KIND_KEY, "document"))
    if not filename or not file_path or not os.path.exists(file_path):
        return {}
    return {
        "filename": filename,
        "file_path": file_path,
        "kind": kind,
    }


def save_announcement_attachment(filename: str, file_path: str, kind: str = "document") -> None:
    previous = get_announcement_attachment()
    previous_path = (previous.get("file_path") or "").strip()
    next_path = (file_path or "").strip()
    set_setting(ANNOUNCEMENT_ATTACHMENT_NAME_KEY, (filename or "").strip())
    set_setting(ANNOUNCEMENT_ATTACHMENT_PATH_KEY, next_path)
    set_setting(ANNOUNCEMENT_ATTACHMENT_KIND_KEY, _normalize_announcement_attachment_kind(kind))
    if previous_path and previous_path != next_path and os.path.exists(previous_path):
        try:
            os.remove(previous_path)
        except OSError:
            pass


def clear_announcement_attachment() -> None:
    previous = get_announcement_attachment()
    previous_path = (previous.get("file_path") or "").strip()
    set_setting(ANNOUNCEMENT_ATTACHMENT_NAME_KEY, "")
    set_setting(ANNOUNCEMENT_ATTACHMENT_PATH_KEY, "")
    set_setting(ANNOUNCEMENT_ATTACHMENT_KIND_KEY, "")
    if previous_path and os.path.exists(previous_path):
        try:
            os.remove(previous_path)
        except OSError:
            pass


def mark_announcement_last_sent(sent_at: str | None = None) -> str:
    value = (sent_at or current_ts()).strip()
    set_setting(ANNOUNCEMENT_LAST_SENT_AT_KEY, value)
    return value


def get_announcement_last_sent_at() -> str:
    return get_setting(ANNOUNCEMENT_LAST_SENT_AT_KEY, "").strip()


# ─────────────────────────────────────────────────────────────────────────
# Announcement scheduling — server-side queue + background firing.
# The fire decision is "scheduled_at <= current_ts()" using string-lexical
# comparison (safe because both sides are "YYYY-MM-DD HH:MM:SS" in the
# same timezone — Tashkent local).
# ─────────────────────────────────────────────────────────────────────────
def _json_dump_safe(value) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return ""


def _json_load_safe(value, default=None):
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def create_announcement_schedule(
    *,
    content: str,
    chat_ids: list[str],
    attachment_snapshot: dict | None,
    scheduled_at: str,
    created_by: str = "",
) -> int:
    """Persist a pending broadcast that the scheduler thread will fire later.

    scheduled_at MUST be "YYYY-MM-DD HH:MM:SS" in Tashkent local time.
    Returns the new schedule id.
    """
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            INSERT INTO announcement_schedules(
                content, chat_ids, attachment_snapshot, scheduled_at,
                created_at, created_by, status
            ) VALUES (?, ?, ?, ?, ?, ?, 'pending')
            """,
            (
                str(content or ""),
                _json_dump_safe([str(c).strip() for c in (chat_ids or []) if str(c).strip()]),
                _json_dump_safe(attachment_snapshot or {}),
                scheduled_at.strip(),
                current_ts(),
                str(created_by or "").strip(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def list_announcement_schedules(limit: int = 50) -> list[dict]:
    """Recent schedules — newest first. Used by the UI panel."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT * FROM announcement_schedules
            ORDER BY
              CASE status WHEN 'pending' THEN 0 WHEN 'sending' THEN 0 ELSE 1 END,
              scheduled_at DESC,
              id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["chat_ids"] = _json_load_safe(d.get("chat_ids"), []) or []
            d["attachment_snapshot"] = _json_load_safe(d.get("attachment_snapshot"), {}) or {}
            out.append(d)
        return out
    finally:
        conn.close()


def due_announcement_schedules(now_ts: str | None = None) -> list[dict]:
    """All pending rows whose scheduled_at <= now (Tashkent local).
    Atomically flips them to 'sending' so a concurrent run can't pick them up."""
    cutoff = (now_ts or current_ts()).strip()
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            """
            SELECT * FROM announcement_schedules
            WHERE status = 'pending' AND scheduled_at <= ?
            ORDER BY scheduled_at ASC, id ASC
            """,
            (cutoff,),
        ).fetchall()
        ids = [int(r["id"]) for r in rows]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            conn.execute(
                f"UPDATE announcement_schedules SET status = 'sending' "
                f"WHERE id IN ({placeholders})",
                tuple(ids),
            )
        conn.commit()
        out = []
        for r in rows:
            d = dict(r)
            d["chat_ids"] = _json_load_safe(d.get("chat_ids"), []) or []
            d["attachment_snapshot"] = _json_load_safe(d.get("attachment_snapshot"), {}) or {}
            out.append(d)
        return out
    finally:
        conn.close()


def mark_announcement_schedule_result(
    schedule_id: int,
    *,
    status: str,
    sent: int = 0,
    failed: int = 0,
    error: str = "",
) -> None:
    """Update a schedule row after firing — sent / failed counts + status."""
    if status not in {"sent", "failed", "cancelled", "pending"}:
        status = "failed"
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE announcement_schedules
            SET status = ?, sent_at = ?, result_sent = ?, result_failed = ?, last_error = ?
            WHERE id = ?
            """,
            (
                status,
                current_ts() if status in {"sent", "failed"} else "",
                int(sent or 0),
                int(failed or 0),
                str(error or "")[:500],
                int(schedule_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def cancel_announcement_schedule(schedule_id: int) -> bool:
    """Soft-cancel a pending schedule. No-op on already-fired rows."""
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            UPDATE announcement_schedules
            SET status = 'cancelled', last_error = ''
            WHERE id = ? AND status IN ('pending')
            """,
            (int(schedule_id),),
        )
        conn.commit()
        return (cur.rowcount or 0) > 0
    finally:
        conn.close()


def reset_orphaned_sending_announcements() -> int:
    """If the process crashed mid-fire, any rows left in 'sending' state stay
    that way forever. Called at boot: flip them back to 'pending' so the next
    scheduler tick retries them. Returns the count revived."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "UPDATE announcement_schedules SET status = 'pending' WHERE status = 'sending'"
        )
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()


def get_announcement_recipients():
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                c.chat_id,
                COALESCE(NULLIF(TRIM(c.title), ''), NULLIF(TRIM(c.username), ''), c.chat_id) AS title,
                c.chat_type,
                c.username,
                c.last_seen_at,
                COUNT(bl.id) AS linked_bl_count,
                -- Batches this chat belongs to that are still IN TRANSIT
                -- (no client_delivery_date yet) — feeds the announcements
                -- "по партиям в пути" filter. NULL-safe: chats without BL
                -- links simply get an empty list.
                GROUP_CONCAT(DISTINCT CASE
                    WHEN bl.id IS NOT NULL
                     AND COALESCE(b.client_delivery_date, '') = ''
                    THEN bl.batch_id
                END) AS active_batch_ids
            FROM telegram_chats c
            LEFT JOIN bl_codes bl ON bl.chat_id = c.chat_id
            LEFT JOIN batches b ON b.id = bl.batch_id
            WHERE c.chat_id != ''
              AND c.is_active = 1
            GROUP BY c.chat_id
            ORDER BY COALESCE(NULLIF(TRIM(c.title), ''), NULLIF(TRIM(c.username), ''), c.chat_id) COLLATE NOCASE ASC
            """
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            raw = item.get("active_batch_ids") or ""
            item["active_batch_ids"] = [int(x) for x in str(raw).split(",") if str(x).strip().isdigit()]
            result.append(item)
        return result
    finally:
        conn.close()


def get_announcement_batches():
    """Active (in-transit) batches that have at least one Telegram group
    linked through their BL codes — the choices for the announcements
    "по партиям в пути" filter. In transit = client_delivery_date empty
    (same rule the Партии page uses for 'Активные партии')."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                b.id,
                b.name,
                b.status,
                COUNT(DISTINCT bl.chat_id) AS group_count,
                COUNT(bl.id) AS bl_count
            FROM batches b
            JOIN bl_codes bl
              ON bl.batch_id = b.id
             AND TRIM(COALESCE(bl.chat_id, '')) != ''
            JOIN telegram_chats c
              ON c.chat_id = bl.chat_id
             AND c.is_active = 1
             AND c.chat_id != ''
            WHERE COALESCE(b.client_delivery_date, '') = ''
            GROUP BY b.id
            ORDER BY b.created_at DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def remember_chat_member(chat_id, user_id, display_name="", username="", is_admin=False):
    chat_value = str(chat_id or "").strip()
    user_value = str(user_id or "").strip()
    if not chat_value or not user_value:
        return
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO telegram_chat_members(
                chat_id, user_id, display_name, username, is_admin, first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))
            ON CONFLICT(chat_id, user_id) DO UPDATE SET
                display_name = CASE
                    WHEN excluded.display_name != '' THEN excluded.display_name
                    ELSE telegram_chat_members.display_name
                END,
                username = CASE
                    WHEN excluded.username != '' THEN excluded.username
                    ELSE telegram_chat_members.username
                END,
                is_admin = CASE
                    WHEN excluded.is_admin = 1 THEN 1
                    ELSE telegram_chat_members.is_admin
                END,
                last_seen_at = datetime('now','localtime')
            """,
            (
                chat_value,
                user_value,
                (display_name or "").strip(),
                (username or "").strip().lstrip("@"),
                1 if is_admin else 0,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_chat_response_assignments(chat_id):
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT
                c.chat_id,
                c.title,
                c.moderator_tg_id,
                c.sales_manager_tg_id,
                mod.display_name AS moderator_name,
                mod.username AS moderator_username,
                sales.display_name AS sales_manager_name,
                sales.username AS sales_manager_username
            FROM telegram_chats c
            LEFT JOIN telegram_chat_members mod
                ON mod.chat_id = c.chat_id AND mod.user_id = c.moderator_tg_id
            LEFT JOIN telegram_chat_members sales
                ON sales.chat_id = c.chat_id AND sales.user_id = c.sales_manager_tg_id
            WHERE c.chat_id = ?
            LIMIT 1
            """,
            (str(chat_id or "").strip(),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def set_chat_response_assignments(chat_id, moderator_tg_id="", sales_manager_tg_id=""):
    chat_value = str(chat_id or "").strip()
    if not chat_value:
        return False
    moderator_value = str(moderator_tg_id or "").strip()
    sales_value = str(sales_manager_tg_id or "").strip()
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO telegram_chats(
                chat_id, title, chat_type, username, moderator_tg_id, sales_manager_tg_id, is_active, created_at, last_seen_at, updated_at
            )
            VALUES (?, '', 'group', '', ?, ?, 1, datetime('now','localtime'), datetime('now','localtime'), datetime('now','localtime'))
            ON CONFLICT(chat_id) DO UPDATE SET
                moderator_tg_id = excluded.moderator_tg_id,
                sales_manager_tg_id = excluded.sales_manager_tg_id,
                last_seen_at = datetime('now','localtime'),
                updated_at = datetime('now','localtime')
            """,
            (chat_value, moderator_value, sales_value),
        )
        conn.execute(
            """
            UPDATE moderator_response_requests
            SET
                assigned_moderator_id = ?,
                assigned_sales_manager_id = ?
            WHERE chat_id = ?
              AND status = 'open'
            """,
            (moderator_value, sales_value, chat_value),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def get_moderator_response_assignment_groups(include_inactive=False):
    conn = get_conn()
    try:
        where = "" if include_inactive else "WHERE c.is_active = 1"
        rows = conn.execute(
            f"""
            SELECT
                c.chat_id,
                c.title,
                c.chat_type,
                c.username,
                c.is_active,
                c.last_seen_at,
                c.moderator_tg_id,
                c.sales_manager_tg_id,
                COUNT(DISTINCT bl.id) AS linked_bl_count
            FROM telegram_chats c
            LEFT JOIN bl_codes bl ON bl.chat_id = c.chat_id
            {where}
            GROUP BY c.chat_id
            ORDER BY COALESCE(NULLIF(TRIM(c.title), ''), NULLIF(TRIM(c.username), ''), c.chat_id) COLLATE NOCASE ASC
            """
        ).fetchall()
        chats = [dict(row) for row in rows]
        chat_ids = [item["chat_id"] for item in chats if item.get("chat_id")]
        members_by_chat = {chat_id: [] for chat_id in chat_ids}
        if chat_ids:
            placeholders = ",".join("?" for _ in chat_ids)
            member_rows = conn.execute(
                f"""
                SELECT chat_id, user_id, display_name, username, is_admin, last_seen_at
                FROM telegram_chat_members
                WHERE chat_id IN ({placeholders})
                ORDER BY
                    CASE WHEN is_admin = 1 THEN 0 ELSE 1 END,
                    COALESCE(NULLIF(TRIM(display_name), ''), NULLIF(TRIM(username), ''), user_id) COLLATE NOCASE ASC
                """,
                chat_ids,
            ).fetchall()
            for row in member_rows:
                item = dict(row)
                item["label"] = _telegram_actor_name(
                    item.get("display_name"),
                    item.get("username"),
                    item.get("user_id"),
                )
                members_by_chat.setdefault(item["chat_id"], []).append(item)

        for chat in chats:
            members = members_by_chat.get(chat["chat_id"], [])
            chat["members"] = members
            moderator_id = str(chat.get("moderator_tg_id") or "").strip()
            sales_id = str(chat.get("sales_manager_tg_id") or "").strip()
            chat["moderator_display"] = next(
                (
                    member["label"]
                    for member in members
                    if str(member.get("user_id") or "").strip() == moderator_id
                ),
                moderator_id,
            )
            chat["sales_manager_display"] = next(
                (
                    member["label"]
                    for member in members
                    if str(member.get("user_id") or "").strip() == sales_id
                ),
                sales_id,
            )
        return chats
    finally:
        conn.close()


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


def _message_status_label(status: str, language: str = DEFAULT_MESSAGE_LANGUAGE) -> str:
    value = (status or "").strip()
    normalized_language = _normalize_message_language(language)
    if value in {"Yiwu", "Zhongshan"}:
        if normalized_language == "uz_cyrl":
            return f"{'Иву' if value == 'Yiwu' else 'Жонгшан'} омборимиздан йўлга чиқиб кетди"
        if normalized_language == "ru":
            return f"Груз выехал с нашего склада в {'Иу' if value == 'Yiwu' else 'Чжуншань'}"
        return f"{value} omborimizdan yo'lga chiqib ketdi"
    if _is_delivered_status(value):
        value = DELIVERED_STATUS
    localized = STATUS_MESSAGE_LABELS.get(normalized_language, {}).get(value)
    if localized:
        return localized
    return value


def _localize_template(template: str, language: str) -> str:
    normalized_language = _normalize_message_language(language)
    if normalized_language == DEFAULT_MESSAGE_LANGUAGE:
        return template
    localized = template
    for source, target in TEMPLATE_LOCALIZATION.get(normalized_language, {}).items():
        localized = localized.replace(source, target)
    return localized


def _apply_customer_delivery_note(rendered: str, show_note: bool, language: str = DEFAULT_MESSAGE_LANGUAGE) -> str:
    if not show_note:
        return rendered
    normalized_language = _normalize_message_language(language)
    if normalized_language == "uz_cyrl":
        note = (
            "❗️Eslatma\n"
            "-<b>Ҳурматли мижоз, юкни қабул қилиб олганингиздан сўнг 2–3 кун ичида уни текширишингизни сўраймиз.\n"
            "Агарда шикаст етган юкингиз бўлса кўрсатилган муддатдан кечикмаган ҳолда хабар беришингизни сўраймиз, акс ҳолда компенсация жараёни чўзилиши мумкин.</b>\n"
        )
    elif normalized_language == "ru":
        note = (
            "❗️Примечание\n"
            "-<b>Уважаемый клиент, просим проверить груз в течение 2–3 дней после получения.\n"
            "Если у вас есть повреждённый груз, просим сообщить об этом без задержки в указанный срок, иначе процесс компенсации может затянуться.</b>\n"
        )
    else:
        note = (
            "❗️Eslatma\n"
            "-<b>Hurmatli mijoz, yukni qabul qilib olganingizdan so‘ng 2–3 kun ichida uni tekshirishingizni so‘raymiz.\n"
            "Agarda shikast yetkan yukingiz bo'lsa ko‘rsatilgan muddatdan kechikmagan holda habar berishingiz so'raymiz,aks xolda kompensatsiya jarayoni cho‘zilishi mumkin.</b>\n"
        )
    if re.search(r"(⏳[^\n]*:\n-[^\n]+\n?)", rendered):
        return re.sub(
            r"(⏳[^\n]*:\n-[^\n]+\n?)",
            r"\1\n" + note,
            rendered,
            count=1,
        )
    return re.sub(
        r"(📍\s*Joriy holati:\n-[^\n]+\n?)",
        r"\1\n" + note,
        rendered,
        count=1,
    )


def _split_tracking_rendered_message(rendered: str) -> tuple[str, str, str]:
    text = (rendered or "").strip()
    if not text:
        return "", "", ""

    cargo_match = re.search(r"(?m)^🚛\s", text)
    if not cargo_match:
        return "", text, ""

    cargo_start = cargo_match.start()
    footer_start = None
    footer_markers = (
        "Aloqa uchun:",
        "Для связи:",
        "Алоқа учун:",
        "Tovar bo'yicha packing list",
        "Packing list по товару",
        "Товар бўйича packing list",
    )
    for marker in footer_markers:
        marker_index = text.find(marker, cargo_start)
        if marker_index == -1:
            continue
        line_start = text.rfind("\n", 0, marker_index)
        line_start = 0 if line_start == -1 else line_start + 1
        if footer_start is None or line_start < footer_start:
            footer_start = line_start

    if footer_start is None:
        return text[:cargo_start].rstrip(), text[cargo_start:].strip(), ""

    return (
        text[:cargo_start].rstrip(),
        text[cargo_start:footer_start].strip(),
        text[footer_start:].strip(),
    )


def _render_single_message(bl: dict, batch_name: str) -> str:
    # По умолчанию — первоначальный шаблон сайта (раздел «Шаблон
    # сообщения»); TRACKING_TEMPLATE_STYLE=v7 включает колонку-«вариант 6».
    if (os.environ.get("TRACKING_TEMPLATE_STYLE", "legacy").strip().lower() == "v7"):
        return render_message_v7(bl, batch_name)
    language = _normalize_message_language(bl.get("message_language"))
    template = _inject_packing_list_placeholder(
        _inject_arrival_eta_placeholder(
            _inject_cargo_info_placeholder(
                _inject_today_date_placeholder(
                    _inject_bl_code_placeholder(
                    _localize_template(_normalize_client_template(get_template()), language)
                    )
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
    places_breakdown = str(bl.get("quantity_places_breakdown") or "").strip()
    description = (bl.get("cargo_description") or "").strip()
    expected_date = (bl.get("expected_date") or "").strip()
    actual_date = (bl.get("actual_date") or "").strip()
    batch = get_batch(bl.get("batch_id")) if bl.get("batch_id") else None
    eta_destination = ((batch or {}).get("eta_destination") or "").strip()
    is_customer_delivery = is_customer_delivery_eta(eta_destination)
    packing_list = "" if is_customer_delivery else format_packing_list(bl.get("id"), language)
    packing_list_text = ""
    arrival_eta = ((batch or {}).get("eta_to_toshkent") or "").strip()
    arrival_eta_value = _localize_eta_value(arrival_eta or expected_date, language)
    arrival_eta_label = _eta_destination_label(eta_destination, language)
    today_date = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y")

    context = _TemplateContext(
        batch_name=_normalize_template_value(batch_name),
        batch_date=_normalize_template_value(batch_name),
        today_date=_normalize_template_value(today_date),
        bl_code=_normalize_template_value(_display_bl_code(bl.get("code", ""), bl.get("merged_codes", ""))),
        client_name=_normalize_template_value(bl.get("client_name", "")),
        status=_normalize_template_value(_message_status_label(status, language)),
        cargo_info=format_cargo_info(bl, language),
        cargo_type=_normalize_template_value(cargo_type),
        weight_kg=_normalize_template_value(f"{weight_value:g}" if weight_value else ""),
        volume_cbm=_normalize_template_value(f"{volume_value:g}" if volume_value else ""),
        volume_m3=_normalize_template_value(f"{volume_value:g}" if volume_value else ""),
        quantity_places=_normalize_template_value(places_breakdown or (places_value if places_value else "")),
        places=_normalize_template_value(places_breakdown or (places_value if places_value else "")),
        cargo_description=_normalize_template_value(description),
        description=_normalize_template_value(description),
        expected_date=_normalize_template_value(arrival_eta_value),
        arrival_eta=_normalize_template_value(arrival_eta_value),
        arrival_eta_label=_normalize_template_value(arrival_eta_label),
        actual_date=_normalize_template_value(actual_date),
        packing_list=_normalize_template_value(packing_list_text),
        bl_files=_normalize_template_value(packing_list_text),
        status_detail="",
    )
    rendered = template.format_map(context)
    # Belt-and-suspenders: even if a customized template still has the
    # contact block, strip it at render time so messages on the wire
    # don't show it.
    rendered = _strip_contact_block(rendered)
    rendered = re.sub(
        r"🇺🇿\s*Yetib kelish vaqti\s*:",
        f"⏳ {arrival_eta_label}:",
        rendered,
        count=1,
    )
    rendered = _apply_customer_delivery_note(rendered, is_customer_delivery, language)
    rendered = re.sub(r"\n{3,}", "\n\n", rendered)
    rendered = rendered.replace("━━━━━━━━━━━━━━━━━━━", "━━━━━━━━━━━━━━━")
    rendered = rendered.replace("📦 Sizning yukingiz bo‘yicha yangilangan treking ma’lumotlari:\n\n", "📦 Sizning yukingiz bo‘yicha yangilangan treking ma’lumotlari:\n")
    rendered = rendered.replace("━━━━━━━━━━━━━━━\n\n🚛 Partiya:", "━━━━━━━━━━━━━━━\n🚛 Partiya:")
    rendered = rendered.replace("📲 @Ziyodilla_Tracking_Manager\n\n━━━━━━━━━━━━━━━", "📲 @Ziyodilla_Tracking_Manager\n━━━━━━━━━━━━━━━")
    rendered = rendered.replace("━━━━━━━━━━━━━━━\n\n🖇Tovar bo'yicha packing list⤵️", "━━━━━━━━━━━━━━━\n🖇Tovar bo'yicha packing list⤵️")
    rendered = re.sub(r"\n*🖇Tovar bo'yicha packing list⤵️(?:\n*🖇Tovar bo'yicha packing list⤵️)+", "\n🖇Tovar bo'yicha packing list⤵️", rendered)
    if is_customer_delivery:
        rendered = re.sub(r"\n*🖇Tovar bo'yicha packing list⤵️.*$", "", rendered, flags=re.S)
        rendered = re.sub(r"\n━━━━━━━━━━━━━━━\s*$", "", rendered)
    else:
        packing_label = _packing_list_label(language)
        rendered = re.sub(
            r"\n*🖇[^\n]*packing list[^\n]*(?:\n(?:• .*|Packing list[^\n]*))*\s*$",
            "",
            rendered,
            flags=re.I | re.M,
        )
        rendered = rendered.rstrip() + f"\n{packing_label}"
    return rendered.strip()


def render_message(bl: dict, batch_name: str, include_related_batches: bool = True) -> str:
    # v7/«вариант 6» рендерит один BL (слияние блоков связанных партий —
    # механика первоначального шаблона, который теперь и есть дефолт).
    if (os.environ.get("TRACKING_TEMPLATE_STYLE", "legacy").strip().lower() == "v7"):
        return render_message_v7(dict(bl or {}), batch_name)
    primary_bl = dict(bl or {})
    chat_id = str(primary_bl.get("chat_id") or "").strip()
    primary_language = _normalize_message_language(primary_bl.get("message_language"))
    primary_rendered = _render_single_message(primary_bl, batch_name)

    if not include_related_batches or not chat_id or not primary_bl.get("id"):
        return primary_rendered

    related_bls = []
    for related in get_tracking_bundle_bls(primary_bl, include_related_batches=include_related_batches):
        if related.get("id") == primary_bl.get("id"):
            continue
        related_copy = dict(related)
        related_copy["message_language"] = primary_language
        related_bls.append(related_copy)

    if not related_bls:
        return primary_rendered

    header, primary_cargo_block, footer = _split_tracking_rendered_message(primary_rendered)
    if not primary_cargo_block:
        return primary_rendered

    cargo_blocks = [primary_cargo_block]
    for related in related_bls:
        related_rendered = _render_single_message(related, related.get("batch_name") or batch_name)
        _, related_cargo_block, _ = _split_tracking_rendered_message(related_rendered)
        if related_cargo_block:
            cargo_blocks.append(related_cargo_block)

    parts = []
    if header:
        parts.append(header.strip())
    parts.append("\n\n".join(block.strip() for block in cargo_blocks if block.strip()))
    if footer:
        parts.append(footer.strip())
    return "\n\n".join(part for part in parts if part).strip()


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


def reserve_track_button_request(chat_id, user_id, cooldown_seconds=60):
    chat_value = str(chat_id or "").strip()
    user_value = str(user_id or "").strip()
    if not chat_value or not user_value:
        return 0

    now_dt = datetime.now(TASHKENT_TZ)
    now_value = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT last_pressed_at
            FROM telegram_track_cooldowns
            WHERE chat_id = ? AND user_id = ?
            """,
            (chat_value, user_value),
        ).fetchone()
        if row and row["last_pressed_at"]:
            last_dt = parse_local_ts(row["last_pressed_at"])
            if last_dt:
                elapsed = max(0, int((now_dt - last_dt).total_seconds()))
                remaining = int(cooldown_seconds) - elapsed
                if remaining > 0:
                    return remaining

        conn.execute(
            """
            INSERT INTO telegram_track_cooldowns(chat_id, user_id, last_pressed_at)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET
                last_pressed_at = excluded.last_pressed_at
            """,
            (chat_value, user_value, now_value),
        )
        conn.commit()
        return 0
    finally:
        conn.close()


def upsert_telegram_chat(chat_id, title, chat_type, username="", is_active=True):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO telegram_chats(chat_id, title, chat_type, username, is_active, created_at, last_seen_at, updated_at)
        VALUES (?, ?, ?, ?, ?, datetime('now','localtime'), datetime('now','localtime'), datetime('now','localtime'))
        ON CONFLICT(chat_id) DO UPDATE SET
            title = excluded.title,
            chat_type = excluded.chat_type,
            username = excluded.username,
            is_active = excluded.is_active,
            last_seen_at = datetime('now','localtime'),
            updated_at = datetime('now','localtime')
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


def record_ai_log(
    *,
    chat_id,
    group_title="",
    user_id="",
    username="",
    original_text="",
    detected_intent="",
    bl_code="",
    ai_response="",
):
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO ai_logs(
                chat_id, group_title, user_id, username,
                original_text, detected_intent, bl_code, ai_response, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(chat_id or "").strip(),
                str(group_title or "").strip(),
                str(user_id or "").strip(),
                str(username or "").strip(),
                str(original_text or "").strip(),
                str(detected_intent or "").strip(),
                str(bl_code or "").strip(),
                str(ai_response or "").strip(),
                current_ts(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


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


def delete_problem(problem_id) -> bool:
    """Remove one problem record (e.g. filed against the wrong BL)."""
    conn = get_conn()
    try:
        cur = conn.execute("DELETE FROM problems WHERE id = ?", (problem_id,))
        conn.commit()
        return (cur.rowcount or 0) > 0
    finally:
        conn.close()


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
    content = row["content"] if row else DEFAULT_COMMUNICATION_RATE_TEMPLATE
    return content or DEFAULT_COMMUNICATION_RATE_TEMPLATE


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
                month_key, chat_id, client_name, bl_id, batch_id, batch_name, message_id, sent_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                month_key,
                str(recipient.get("chat_id", "")),
                recipient.get("client_name", ""),
                recipient.get("bl_id"),
                recipient.get("batch_id"),
                recipient.get("batch_name", ""),
                "",
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


def save_communication_survey_dispatch_message_id(dispatch_id, message_id):
    if not dispatch_id:
        return
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE communication_survey_dispatches
            SET message_id = ?
            WHERE id = ?
            """,
            (str(message_id or "").strip(), dispatch_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_communication_survey_dispatch(dispatch_id):
    if not dispatch_id:
        return None
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT id, month_key, chat_id, client_name, bl_id, batch_id, batch_name, message_id, sent_at
            FROM communication_survey_dispatches
            WHERE id = ?
            """,
            (dispatch_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_communication_survey_dispatch_for_event(event_id):
    if not event_id:
        return None
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT d.id, d.month_key, d.chat_id, d.client_name, d.bl_id, d.batch_id, d.batch_name, d.message_id, d.sent_at
            FROM communication_rating_events e
            JOIN communication_survey_dispatches d ON d.id = e.dispatch_id
            WHERE e.id = ?
            LIMIT 1
            """,
            (event_id,),
        ).fetchone()
        return dict(row) if row else None
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
    _refresh_communication_rating_snapshot(conn, month_key, chat_id)

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


def _refresh_communication_rating_snapshot(conn, month_key, chat_id):
    latest = conn.execute(
        """
        SELECT
            month_key,
            chat_id,
            client_name,
            bl_id,
            batch_id,
            voter_user_id,
            voter_name,
            voter_username,
            score,
            submitted_at
        FROM communication_rating_events
        WHERE month_key = ? AND chat_id = ?
        ORDER BY submitted_at DESC, id DESC
        LIMIT 1
        """,
        (month_key, str(chat_id)),
    ).fetchone()
    if latest:
        conn.execute(
            """
            INSERT INTO communication_ratings(
                month_key, chat_id, client_name, bl_id, batch_id,
                voter_user_id, voter_name, voter_username, score, submitted_at
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
                latest["month_key"],
                latest["chat_id"],
                latest["client_name"],
                latest["bl_id"],
                latest["batch_id"],
                latest["voter_user_id"],
                latest["voter_name"],
                latest["voter_username"],
                latest["score"],
                latest["submitted_at"],
            ),
        )
    else:
        conn.execute(
            "DELETE FROM communication_ratings WHERE month_key = ? AND chat_id = ?",
            (month_key, str(chat_id)),
        )


def delete_communication_rating_event(event_id):
    if not event_id:
        return
    conn = get_conn()
    row = conn.execute(
        "SELECT month_key, chat_id, dispatch_id FROM communication_rating_events WHERE id = ?",
        (event_id,),
    ).fetchone()
    if not row:
        conn.close()
        return
    month_key = row["month_key"]
    chat_id = row["chat_id"]
    dispatch_id = row["dispatch_id"]
    conn.execute("DELETE FROM communication_rating_events WHERE id = ?", (event_id,))

    if dispatch_id:
        remaining_dispatch_events = conn.execute(
            "SELECT COUNT(*) FROM communication_rating_events WHERE dispatch_id = ?",
            (dispatch_id,),
        ).fetchone()[0]
        if remaining_dispatch_events == 0:
            conn.execute("DELETE FROM communication_survey_dispatches WHERE id = ?", (dispatch_id,))
            latest_send = conn.execute(
                """
                SELECT month_key, chat_id, client_name, bl_id, batch_id, batch_name, sent_at
                FROM communication_survey_dispatches
                WHERE month_key = ? AND chat_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (month_key, chat_id),
            ).fetchone()
            if latest_send:
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
                        latest_send["month_key"],
                        latest_send["chat_id"],
                        latest_send["client_name"],
                        latest_send["bl_id"],
                        latest_send["batch_id"],
                        latest_send["batch_name"],
                        latest_send["sent_at"],
                    ),
                )
            else:
                conn.execute(
                    "DELETE FROM communication_survey_sends WHERE month_key = ? AND chat_id = ?",
                    (month_key, chat_id),
                )
    _refresh_communication_rating_snapshot(conn, month_key, chat_id)
    conn.commit()
    conn.close()


def save_communication_rating(dispatch_id, month_key, chat_id, score, voter=None):
    score_value = _to_int(score)
    if score_value < 0 or score_value > 10:
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
            e.id AS event_id,
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


# ──────────────────────────────────────────────────────────────────────────
# Director dashboard config (3 sections: savdo / ombor / agentlar)
# ──────────────────────────────────────────────────────────────────────────
DIRECTOR_SECTIONS = ("savdo_seliy", "savdo_sborniy", "ombor", "agentlar", "sotuv_bazasi")

_SHEET_ID_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9_-]+)")
_SHEET_GID_RE = re.compile(r"[#&?]gid=(\d+)")


def _parse_sheet_url(url: str) -> tuple[str, str]:
    """Extract sheet_id and gid from a Google Sheets URL. Returns ('', '') if not parseable."""
    if not url:
        return "", ""
    m_id = _SHEET_ID_RE.search(url)
    m_gid = _SHEET_GID_RE.search(url)
    return (m_id.group(1) if m_id else ""), (m_gid.group(1) if m_gid else "")


def get_director_config(section: str = "") -> dict | list[dict]:
    """Return one section config dict (when section given) or list of all 3."""
    conn = get_conn()
    try:
        if section:
            row = conn.execute(
                "SELECT section, sheet_url, sheet_id, sheet_gid, sheet_name, header_rows, columns_json, updated_at, updated_by FROM director_dashboard_config WHERE section = ?",
                (section,),
            ).fetchone()
            return _director_row_to_dict(row, section)
        rows = conn.execute(
            "SELECT section, sheet_url, sheet_id, sheet_gid, sheet_name, header_rows, columns_json, updated_at, updated_by FROM director_dashboard_config"
        ).fetchall()
        by_section = {r[0]: r for r in rows}
        return [_director_row_to_dict(by_section.get(s), s) for s in DIRECTOR_SECTIONS]
    finally:
        conn.close()


def _director_row_to_dict(row, section: str) -> dict:
    if not row:
        return {
            "section": section,
            "sheet_url": "",
            "sheet_id": "",
            "sheet_gid": "",
            "sheet_name": "",
            "header_rows": 1,
            "columns": {},
            "updated_at": "",
            "updated_by": "",
        }
    try:
        cols = json.loads(row[6] or "{}")
    except json.JSONDecodeError:
        cols = {}
    return {
        "section": row[0],
        "sheet_url": row[1] or "",
        "sheet_id": row[2] or "",
        "sheet_gid": row[3] or "",
        "sheet_name": row[4] or "",
        "header_rows": int(row[5] or 1),
        "columns": cols if isinstance(cols, dict) else {},
        "updated_at": row[7] or "",
        "updated_by": row[8] or "",
    }


def save_director_config(
    section: str,
    sheet_url: str,
    sheet_name: str,
    header_rows: int,
    columns: dict,
    updated_by: str = "",
) -> dict:
    if section not in DIRECTOR_SECTIONS:
        raise ValueError(f"Unknown director section: {section}")
    sheet_id, sheet_gid = _parse_sheet_url(sheet_url or "")
    columns_json = json.dumps(columns or {}, ensure_ascii=False)
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO director_dashboard_config(section, sheet_url, sheet_id, sheet_gid, sheet_name, header_rows, columns_json, updated_at, updated_by)
            VALUES(?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'), ?)
            ON CONFLICT(section) DO UPDATE SET
                sheet_url   = excluded.sheet_url,
                sheet_id    = excluded.sheet_id,
                sheet_gid   = excluded.sheet_gid,
                sheet_name  = excluded.sheet_name,
                header_rows = excluded.header_rows,
                columns_json= excluded.columns_json,
                updated_at  = excluded.updated_at,
                updated_by  = excluded.updated_by
            """,
            (
                section,
                (sheet_url or "").strip(),
                sheet_id,
                sheet_gid,
                (sheet_name or "").strip(),
                max(0, int(header_rows or 1)),
                columns_json,
                (updated_by or "").strip(),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return get_director_config(section)



# ═══════════════════════════════════════════════════════════════
# AI ASSISTANT (DeepSeek) — conversation memory + approval queue
# ═══════════════════════════════════════════════════════════════

AI_HISTORY_LIMIT = 30


def ai_add_message(tg_user_id, role, content):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO ai_assistant_messages (tg_user_id, role, content) VALUES (?, ?, ?)",
            (str(tg_user_id), str(role), str(content or "")),
        )
        # Keep the history bounded per user.
        conn.execute(
            """
            DELETE FROM ai_assistant_messages
            WHERE tg_user_id = ?
              AND id NOT IN (
                  SELECT id FROM ai_assistant_messages
                  WHERE tg_user_id = ?
                  ORDER BY id DESC LIMIT ?
              )
            """,
            (str(tg_user_id), str(tg_user_id), AI_HISTORY_LIMIT * 2),
        )
        conn.commit()
    finally:
        conn.close()


def ai_get_history(tg_user_id, limit=AI_HISTORY_LIMIT):
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT role, content FROM ai_assistant_messages
            WHERE tg_user_id = ?
            ORDER BY id DESC LIMIT ?
            """,
            (str(tg_user_id), int(limit)),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in reversed(rows)]


def ai_clear_history(tg_user_id):
    conn = get_conn()
    try:
        conn.execute(
            "DELETE FROM ai_assistant_messages WHERE tg_user_id = ?",
            (str(tg_user_id),),
        )
        conn.commit()
    finally:
        conn.close()


def ai_create_pending_action(tg_user_id, kind, params_json, summary):
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            INSERT INTO ai_pending_actions (tg_user_id, kind, params_json, summary)
            VALUES (?, ?, ?, ?)
            """,
            (str(tg_user_id), str(kind), str(params_json), str(summary or "")),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def ai_get_pending_action(action_id):
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM ai_pending_actions WHERE id = ?",
            (int(action_id),),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def ai_claim_pending_action(action_id, new_status) -> bool:
    """Атомарно перевести заявку из pending в new_status (compare-and-set).
    False — заявку уже забрал кто-то другой (человек нажал кнопку, пока
    планировщик шёл к автоприменению, или наоборот)."""
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            UPDATE ai_pending_actions
            SET status = ?, decided_at = datetime('now','localtime')
            WHERE id = ? AND status = 'pending'
            """,
            (str(new_status), int(action_id)),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def ai_finish_pending_action(action_id, status, result=""):
    """status: approved / rejected / executed / failed"""
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE ai_pending_actions
            SET status = ?, result = ?, decided_at = datetime('now','localtime')
            WHERE id = ?
            """,
            (str(status), str(result or ""), int(action_id)),
        )
        conn.commit()
    finally:
        conn.close()


# ── AI scheduled tasks (владельческий режим ассистента) ────────────

def ai_create_scheduled_task(created_by, run_at, recurrence, kind, params_json):
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            INSERT INTO ai_scheduled_tasks (created_by, run_at, recurrence, kind, params_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (str(created_by), str(run_at), str(recurrence or "once"), str(kind), str(params_json)),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def ai_list_scheduled_tasks(status="pending"):
    conn = get_conn()
    try:
        if status:
            rows = conn.execute(
                "SELECT * FROM ai_scheduled_tasks WHERE status = ? ORDER BY run_at",
                (status,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM ai_scheduled_tasks ORDER BY id DESC LIMIT 50"
            ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def ai_due_scheduled_tasks(now_str):
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM ai_scheduled_tasks WHERE status = 'pending' AND run_at <= ? ORDER BY run_at",
            (str(now_str),),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def ai_mark_scheduled_task(task_id, status, next_run_at=None):
    conn = get_conn()
    try:
        if next_run_at:
            conn.execute(
                """
                UPDATE ai_scheduled_tasks
                SET status = ?, run_at = ?, last_run_at = datetime('now','localtime')
                WHERE id = ?
                """,
                (str(status), str(next_run_at), int(task_id)),
            )
        else:
            conn.execute(
                """
                UPDATE ai_scheduled_tasks
                SET status = ?, last_run_at = datetime('now','localtime')
                WHERE id = ?
                """,
                (str(status), int(task_id)),
            )
        conn.commit()
    finally:
        conn.close()


def ai_cancel_scheduled_task(task_id) -> bool:
    conn = get_conn()
    try:
        cur = conn.execute(
            "UPDATE ai_scheduled_tasks SET status = 'cancelled' WHERE id = ? AND status = 'pending'",
            (int(task_id),),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def ai_find_pending_action(tg_user_id, kind):
    """Последняя PENDING-заявка данного вида для данного ключа (корзина
    множественного подтверждения трекинга)."""
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT * FROM ai_pending_actions
            WHERE tg_user_id = ? AND kind = ? AND status = 'pending'
            ORDER BY id DESC LIMIT 1
            """,
            (str(tg_user_id), str(kind)),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def ai_update_pending_action_params(action_id, params_json):
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE ai_pending_actions SET params_json = ? WHERE id = ?",
            (str(params_json), int(action_id)),
        )
        conn.commit()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# АЛЬТЕРНАТИВНЫЙ ШАБЛОН ТРЕКИНГА (вариант 6, «колонка») — НЕ дефолт
# ═══════════════════════════════════════════════════════════════
# Дефолт — первоначальный шаблон сайта (message_template); включение
# колонки: TRACKING_TEMPLATE_STYLE=v7. tracking_view_data ниже нужен
# всегда (is_customer_delivery и данные для цитаты-альбома).

_V7_ROUTE_KAZAKH = [
    "Xitoy", "Horgos", "Nurjo'li", "Jarkent", "Almata", "Taraz",
    "Shimkent", "Qonusbay", "Saryagash", "Yallama", "Toshkent(Chuqursoy ULS da)",
]
_V7_ROUTE_KYRGYZ = [
    "Xitoy", "Kashgar (Qirg'iz)", "Irkeshtam", "Osh", "Dostlik", "Andijon",
    "Toshkent(Chuqursoy ULS da)",
]
_V7_CHINA_ALIASES = {"Xitoy", "Yiwu", "Zhongshan"}

_V7_STATUS_COUNTRY = {
    "Xitoy": "cn", "Yiwu": "cn", "Zhongshan": "cn",
    "Horgos": "border_kz", "Nurjo'li": "kz", "Jarkent": "kz",
    "Almata": "kz", "Taraz": "kz", "Shimkent": "kz", "Qonusbay": "kz",
    "Saryagash": "kz", "Yallama": "border_uz",
    "Kashgar (Qirg'iz)": "cn", "Irkeshtam": "border_kg", "Osh": "kg",
    "Dostlik": "border_uz", "Andijon": "uz",
    "Toshkent(Chuqursoy ULS da)": "uz",
}

_V7_COUNTRY_NAMES = {
    "uz_latn": {"cn": "Xitoy", "kz": "Qozog'iston", "kg": "Qirg'iziston", "uz": "O'zbekiston",
                "border_kz": "Xitoy–Qozoq chegarasi", "border_kg": "Xitoy–Qirg'iz chegarasi",
                "border_uz": "O'zbekiston chegarasi"},
    "uz_cyrl": {"cn": "Хитой", "kz": "Қозоғистон", "kg": "Қирғизистон", "uz": "Ўзбекистон",
                "border_kz": "Хитой–Қозоқ чегараси", "border_kg": "Хитой–Қирғиз чегараси",
                "border_uz": "Ўзбекистон чегараси"},
    "ru": {"cn": "Китай", "kz": "Казахстан", "kg": "Кыргызстан", "uz": "Узбекистан",
           "border_kz": "граница Китай–Казахстан", "border_kg": "граница Китай–Кыргызстан",
           "border_uz": "граница Узбекистана"},
    "en": {"cn": "China", "kz": "Kazakhstan", "kg": "Kyrgyzstan", "uz": "Uzbekistan",
           "border_kz": "China–Kazakhstan border", "border_kg": "China–Kyrgyzstan border",
           "border_uz": "Uzbekistan border"},
}

_V7_STRINGS = {
    "uz_latn": {
        "greeting": "👋Assalomu alaykum hurmatli mijoz!",
        "holat": "📍 YUK HOLATI",
        "batch_label": "Partiya",
        "now": "Hozir",
        "arrived_note": "yetib keldi",
        "joy": "joy",
        "packing": "🖇 Tovar bo'yicha packing list⤵️",
        "eta_empty": "aniqlashtirilmoqda",
    },
    "uz_cyrl": {
        "greeting": "👋Ассалому алайкум ҳурматли мижоз!",
        "holat": "📍 ЮК ҲОЛАТИ",
        "batch_label": "Партия",
        "now": "Ҳозир",
        "arrived_note": "етиб келди",
        "joy": "жой",
        "packing": "🖇 Товар бўйича packing list⤵️",
        "eta_empty": "аниқлаштирилмоқда",
    },
    "ru": {
        "greeting": "👋Здравствуйте, уважаемый клиент!",
        "holat": "📍 СТАТУС ГРУЗА",
        "batch_label": "Партия",
        "now": "Сейчас",
        "arrived_note": "груз прибыл",
        "joy": "мест",
        "packing": "🖇 Packing list по товару⤵️",
        "eta_empty": "уточняется",
    },
    "en": {
        "greeting": "👋Hello, dear client!",
        "holat": "📍 CARGO STATUS",
        "batch_label": "Batch",
        "now": "Now",
        "arrived_note": "arrived",
        "joy": "pcs",
        "packing": "🖇 Packing list below⤵️",
        "eta_empty": "to be confirmed",
    },
}


def _v7_route_progress(status: str):
    """(progress 0..1, arrived) по позиции статуса на его маршруте."""
    normalized = _normalize_status(status or "Xitoy")
    if normalized in (DELIVERED_STATUS, LEGACY_DELIVERED_STATUS, "Toshkent(Chuqursoy ULS da)"):
        return 1.0, True
    if normalized in _V7_CHINA_ALIASES:
        return 0.0, False
    route = _V7_ROUTE_KYRGYZ if normalized in _V7_ROUTE_KYRGYZ else _V7_ROUTE_KAZAKH
    try:
        idx = route.index(normalized)
    except ValueError:
        return 0.0, False
    return idx / (len(route) - 1), False


def _v7_route_line(status: str) -> str:
    progress, arrived = _v7_route_progress(status)
    if arrived:
        nodes = ["✅"] * 5 + ["📦"]
    else:
        pos = min(5, max(0, round(progress * 5)))
        nodes = []
        for i in range(6):
            if i < pos:
                nodes.append("✅")
            elif i == pos:
                nodes.append("🚚")
            else:
                nodes.append("○")
    return "🇨🇳 " + "──".join(nodes) + " 🇺🇿"


def _v7_place_label(status: str, language: str) -> str:
    normalized = _normalize_status(status or "Xitoy")
    label = _message_status_label(normalized, language)
    country_key = _V7_STATUS_COUNTRY.get(normalized)
    if not country_key:
        return label
    country = _V7_COUNTRY_NAMES.get(language, _V7_COUNTRY_NAMES["uz_latn"]).get(country_key, "")
    if not country or country.lower() in label.lower():
        return label
    return f"{label} ({country})"


def tracking_view_data(bl: dict, batch_name: str) -> dict:
    """Структурированные данные трекинга для текста v7 и карточки-PNG."""
    language = _normalize_message_language(bl.get("message_language"))
    strings = _V7_STRINGS.get(language, _V7_STRINGS["uz_latn"])

    status = bl.get("status", "Xitoy")
    batch = get_batch(bl.get("batch_id")) if bl.get("batch_id") else None
    eta_destination = ((batch or {}).get("eta_destination") or "").strip()
    arrival_eta = ((batch or {}).get("eta_to_toshkent") or "").strip()
    eta_value = _localize_eta_value(arrival_eta, language) or strings["eta_empty"]
    eta_label = _eta_destination_label(eta_destination, language)
    is_customer_delivery = is_customer_delivery_eta(eta_destination)

    progress, arrived = _v7_route_progress(status)
    place = _v7_place_label(status, language)
    if arrived:
        toshkent_names = {"uz_latn": "Toshkent", "uz_cyrl": "Тошкент", "ru": "Ташкент", "en": "Tashkent"}
        uz_country = _V7_COUNTRY_NAMES.get(language, _V7_COUNTRY_NAMES["uz_latn"])["uz"]
        place = f"{toshkent_names.get(language, 'Toshkent')} ({uz_country})"

    weight_value = _to_float(bl.get("weight_kg"))
    volume_value = _to_float(bl.get("volume_cbm"))
    places_breakdown = str(bl.get("quantity_places_breakdown") or "").strip()
    places_value = places_breakdown or (_to_int(bl.get("quantity_places")) or "")
    bl_code = _display_bl_code(bl.get("code", ""), bl.get("merged_codes", ""))

    req_parts_plain = [str(bl_code), str(batch_name)]
    if places_value:
        req_parts_plain.append(f"{places_value} {strings['joy']}")
    if weight_value:
        req_parts_plain.append(f"{weight_value:g} kg")
    if volume_value:
        req_parts_plain.append(f"{volume_value:g} m³")

    country_names = _V7_COUNTRY_NAMES.get(language, _V7_COUNTRY_NAMES["uz_latn"])
    return {
        "language": language,
        "strings": strings,
        "eta_label": eta_label,
        "eta_value": eta_value,
        "place": place,
        "progress": progress,
        "arrived": arrived,
        "bl_code": str(bl_code),
        "batch_name": str(batch_name),
        "places_value": str(places_value),
        "weight_kg": f"{weight_value:g}" if weight_value else "",
        "volume_m3": f"{volume_value:g}" if volume_value else "",
        "requisites_plain": " · ".join(req_parts_plain),
        "is_customer_delivery": is_customer_delivery,
        "endpoint_left": country_names["cn"],
        "endpoint_right": {"uz_latn": "Toshkent", "uz_cyrl": "Тошкент", "ru": "Ташкент", "en": "Tashkent"}.get(language, "Toshkent"),
    }


def render_message_v7(bl: dict, batch_name: str) -> str:
    """Шаблон «вариант 6» (колонка строк с эмодзи-метками) — включается
    только через TRACKING_TEMPLATE_STYLE=v7; дефолт — шаблон сайта."""
    view = tracking_view_data(bl, batch_name)
    strings = view["strings"]

    place_text = view["place"]
    if view["arrived"]:
        place_text = f"{place_text} — {strings['arrived_note']} ✅"

    lines = [
        strings["greeting"],
        "━━━━━━━━━━━━━━━",
        f"📦 BL: <b>{html.escape(view['bl_code'])}</b>",
        f"🚛 {strings['batch_label']}: <b>{html.escape(view['batch_name'])}</b>",
        f"📍 {strings['now']}: <b>{html.escape(place_text)}</b>",
    ]
    if not view["arrived"]:
        lines.append(f"⏳ {view['eta_label']}: <b>{html.escape(view['eta_value'])}</b>")
    measures = []
    if view["places_value"]:
        measures.append(f"{html.escape(view['places_value'])} {strings['joy']}")
    if view["weight_kg"]:
        measures.append(f"{view['weight_kg']} kg")
    if view["volume_m3"]:
        measures.append(f"{view['volume_m3']} m³")
    if measures:
        lines.append("⚖️ " + " · ".join(measures))
    if not view["is_customer_delivery"]:
        lines.append("━━━━━━━━━━━━━━━")
        lines.append(strings["packing"])
    return "\n".join(lines)
