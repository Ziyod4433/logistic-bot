# -*- coding: utf-8 -*-
"""BURAQ Logistics AI assistant (DeepSeek).

A private-chat Telegram assistant for the owner. It knows every process
of the admin panel (batches, tracking, Yuk holati, packing lists,
problems, announcements, late cargo, sales monitor) and can:

  - answer questions using READ tools (executed immediately);
  - PROPOSE actions (status change, tracking send, group message) —
    proposals are queued in ai_pending_actions and the owner confirms
    them with inline buttons. NOTHING is changed or sent without an
    explicit confirmation. This is a hard rule enforced in code, not
    just in the prompt: the assistant has no direct write tools.

Only Telegram user ids listed in AI_ASSISTANT_ADMIN_IDS may talk to it
(default: the owner, 303114354).
"""

import json
import os
import re
import threading

import requests as req

import database as db

DEEPSEEK_BASE_URL = (os.getenv("DEEPSEEK_BASE_URL") or "https://api.deepseek.com").rstrip("/")
FALLBACK_MODEL = "deepseek-chat"
MAX_TOOL_ROUNDS = 6
REQUEST_TIMEOUT = 90

_history_lock = threading.Lock()


def _api_key() -> str:
    return (os.getenv("DEEPSEEK_API_KEY") or "").strip()


def _model() -> str:
    return (os.getenv("DEEPSEEK_MODEL") or "deepseek-v4-pro").strip()


def admin_ids() -> set:
    raw = os.getenv("AI_ASSISTANT_ADMIN_IDS", "303114354")
    return {part.strip() for part in str(raw).split(",") if part.strip()}


def is_admin(tg_user_id) -> bool:
    return str(tg_user_id) in admin_ids()


def readonly_ids() -> set:
    """Users allowed to TALK to the assistant in private, but with READ
    tools only — no propose_action, nothing can be changed or sent."""
    raw = os.getenv("AI_ASSISTANT_READONLY_IDS", "7713376668")
    return {part.strip() for part in str(raw).split(",") if part.strip()}


def is_readonly_user(tg_user_id) -> bool:
    return str(tg_user_id) in readonly_ids() and not is_admin(tg_user_id)


def control_group_id() -> str:
    """The ONE staff group that has full rights over the bot. All other
    groups are client groups where the bot stays silent (for now)."""
    return (os.getenv("AI_CONTROL_GROUP_ID") or "-1002653438323").strip()


def is_control_chat(chat_id) -> bool:
    return str(chat_id).strip() == control_group_id()


def confidential_chat_ids() -> set:
    """Chats the bot must NEVER message or reveal anything about."""
    raw = os.getenv("CONFIDENTIAL_CHAT_IDS", "-1002687342009")
    return {part.strip() for part in str(raw).split(",") if part.strip()}


def _mask_chat_id(chat_id) -> str:
    value = str(chat_id or "").strip()
    return "🔒 конфиденциально" if value in confidential_chat_ids() else value


def get_runtime_status() -> dict:
    return {
        "deepseek_api_key_present": bool(_api_key()),
        "deepseek_model": _model(),
        "admin_ids": sorted(admin_ids()),
    }


# ═══════════════════════════════════════════════════════════════
# SYSTEM PROMPT — the full process knowledge base
# ═══════════════════════════════════════════════════════════════

def _system_prompt() -> str:
    from datetime import datetime

    statuses = " → ".join(db.STATUSES)
    ask_hour = (os.getenv("TRACKING_ASK_HOUR", "7") or "7").strip()
    packing_hour = (os.getenv("PACKING_REMINDER_HOUR", "9") or "9").strip()
    digest_hour = (os.getenv("TRACKING_DIGEST_HOUR", "9") or "9").strip()
    digest_id = (os.getenv("TRACKING_DIGEST_TG_ID", "7713376668") or "").strip()
    return f"""Ты — AI-ассистент логистической компании BURAQ Logistics (карго Китай → Узбекистан).
Ты работаешь внутри админ-панели (Flask-сайт) и общаешься с ВЛАДЕЛЬЦЕМ компании в личном чате Telegram.
Сегодня: {datetime.now().strftime('%d.%m.%Y %H:%M')}.

════════ КАК УСТРОЕНА СИСТЕМА ════════

1. ПАРТИИ (batches). Партия — это рейс/отправка, её имя обычно дата (например «01.08.2026»).
   У партии есть: статус (точка маршрута), ETA до Ташкента, конечная точка (Toshkent / Qozog'istonga o'tish и др.),
   дата выдачи клиенту (client_delivery_date). Партия АКТИВНА, пока дата выдачи пуста; после заполнения уходит в архив.
   Цепочка статусов маршрута: {statuses}.
   Два маршрута: через Казахстан (Horgos → Nurjo'li → Jarkent → Almata → Taraz → Shimkent → Qonusbay → Saryagash → Yallama)
   и через Кыргызстан (Kashgar → Irkeshtam → Osh → Dostlik → Andijon).
   «Toshkent(Chuqursoy ULS da)» = груз прибыл на склад в Ташкенте. «{db.DELIVERED_STATUS}» = выдан клиенту.

2. BL КОДЫ. Внутри партии — BL коды (например BL-171). У BL: код, имя клиента, телефон, привязанная
   Telegram-группа клиента (chat_id), файлы (packing list), флаги «исключён из рассылки» и «трекинг уже отправлен».
   Один клиент может иметь несколько BL в разных партиях. Привязка BL↔группа запоминается навсегда
   в истории (bl_link_history) — при импорте новой партии из шитса группы подставляются автоматически.

3. ЕЖЕДНЕВНЫЙ ТРЕКИНГ (главный процесс).
   Утром в {ask_hour}:00 по Ташкенту бот САМ присылает в группы с включённой формой (/formon; по умолчанию —
   управляющая группа) список активных партий и кнопку Mini App-формы «Параметры партии»
   (в группах — прямая ссылка t.me/бот/form, в личке — синяя кнопка-меню «📝 Forma» и /form).
   В форме логисты обновляют: статус (точка маршрута), ETA-текст, Qaysi nuqtaga и Holat (kutilmagan vaziyat —
   поломка фуры, доп. досмотр на границе и т.п.; это внутренняя пометка, клиентам НЕ отправляется).
   «Сохранить» — тихое сохранение (уведомление в группу-источник); «TASDIQLASH» — бот шлёт в группу карточку
   «Hammasi to'g'rimi?» и ТОЛЬКО после второго ✅ рассылает трекинг по клиентским группам.
   КЛИЕНТСКОЕ ТРЕКИНГ-СООБЩЕНИЕ: текст шаблона сайта (раздел «Шаблон сообщения»; порядок: Joriy
   holati → срок прибытия → Partiya → BL-kod → данные груза; строки «Bugungi sana» больше НЕТ;
   точный текст — только через get_message_templates, не выдумывай), где приветствие и строка
   «🖇 packing list» обычные, а всё между ними — нативной цитатой-«окном» (blockquote). СРАЗУ ПОД
   текстом отдельным сообщением — файлы packing list НАСТОЯЩИМИ документами Telegram (альбом, без
   ответов/цитат/ссылок — владелец явно отверг файлы-ссылки). Фото, PNG и геолокаций НЕТ.
   Язык = язык группы (uz латиница / uz кириллица / ru / en).
   Индикация в панели: зелёная строка = трекинг отправлен сегодня, жёлтая = 1 день, красная = 2+ дней.
   Логи отправок хранятся (send_logs).

4. КНОПКА «YUK HOLATI» в группах клиентов: клиент сам запрашивает статус. Бот показывает ТОЛЬКО партии
   В ПУТИ. Если груз уже в статусе «Toshkent(Chuqursoy ULS da)» или доставлен — бот отвечает
   «Hozirgi vaqtda yo'lda kelayotgan yukingiz mavjud emas» (строгое правило).

5. PACKING LISTS. Файлы упаковочных листов прикрепляются к BL массово: авто-сопоставление по имени файла
   (BL код или имя клиента), ручной выбор с поиском. СТРОГО в рамках текущей партии — файл не может уйти в чужую партию.
   Клиент может запросить свой packing list кнопкой в группе.

6. ПРОБЛЕМЫ. По BL фиксируются проблемы: Shikastlanish (повреждение), Kechikish (задержка), Kamomad (недостача),
   Qadoq buzilishi (нарушение упаковки), Boshqa (другое). Есть просмотр, фильтры, удаление ошибочных.

7. ОБЪЯВЛЕНИЯ. Массовая рассылка по всем группам клиентов: текст + фото/видео/GIF, настраиваемый порядок
   (медиа первым или текст первым, вместе или отдельно).

8. ОПАЗДЫВАЮЩИЕ ГРУЗЫ (Dashboard). Отдельный Google Sheets статусов с 3 листами: Ombor (на складе в Китае) →
   Ortilgan furalar (погружен в фуру) → Yetib keldi (прибыл). SLA: склад→фура 3-6 дней, фура→Ташкент 18-25 дней,
   итого 30 дней от даты прихода на склад (DATE OF ARRIVE). Груз старше 30 дней = опаздывает.
   Группировка по рейсу (REYS RAQAMI), видно склад (SKLAD: ZHONGSHAN/YIWU) и продавца (SOTUVCHI).

9. SALES MONITOR (ТВ-экран отдела продаж, профиль sales). План месяца в м³. LTL = кубы из шитса Ombor,
   FTL = целые фуры (1 фура = N м³). Большой круг «BAJARILDI» = (LTL м³ + FTL м³) / план, вокруг него капли LTL/FTL.

10. ПЛАНЫ ПОГРУЗКИ (шитс партий) — ключевой процесс жизни партии.
    Партия ОТКРЫВАЕТСЯ по плану погрузки китайской фуры из шитса планов (месячные вкладки типа «AVGUST 2026»).
    Блок плана в шитсе: дата отправки + название + список SHIPPING MARK (BL) с CTN/CBM/KG.
    • Китайская фура (kind=china): «YIWU TO HORGOS - YARGXOL», «ZHONGSHAN TO HORGOS YARGXOL»
      (старые названия: «YIWU MUHAMMAD», «ZHONGSHAN YARGXOL») — погрузка со склада Китая до Хоргоса.
    • Казахская фура (kind=kazakh): «HORGOS TO TASHKENT YIWU + ZH YARGXOL» — погрузка в Хоргосе до Ташкента;
      обычно ОБЪЕДИНЯЕТ BL обеих китайских фур (YIWU + ZHONGSHAN) одной даты.
    ЖИЗНЕННЫЙ ЦИКЛ: партия открыта по китайскому плану → фура едет до Хоргоса → статус «Horgos (Qozoq)» →
    логисты перегружают груз в казахскую фуру → состав BL партии нужно ПЕРЕСИНХРОНИЗИРОВАТЬ по казахскому
    плану той же даты (раньше владелец делал это вручную — теперь это твоя работа, через подтверждение).
    Как делать: get_loading_plans → сравни план с партией (get_batch_detail) → propose_action
    kind='sync_batch_from_plan' (batch_id, tab, plan_title, plan_date). Синхронизация приводит состав партии
    ТОЧНО к плану: недостающие BL добавляются (Telegram-группы подтягиваются автоматически из истории привязок),
    а BL, которых в плане нет, УДАЛЯЮТСЯ из партии — груз не в этой фуре, и при рассылке их группы получили бы
    неверный трекинг. В summary заявки обязательно перечисли, какие BL будут удалены (сравни заранее), чтобы
    подтверждающий видел это до нажатия ✅.
    Когда предлагаешь смену статуса на «Horgos (Qozoq)» — сам предложи следом и синхронизацию по казахскому плану.

11. УТРЕННИЙ СБОР PACKING LIST. Каждое утро (по Ташкенту) бот сам пишет в управляющую группу,
    отмечает ответственного (Jigar, на узбекском) и перечисляет BL активных партий без packing list.
    Jigar кидает в группу ZIP-архив, ссылку на ZIP в Google Drive или ссылку на Drive-ПАПКУ с файлами
    (архивы >20 МБ Telegram не отдаёт — только ссылкой; RAR не поддерживается, просить ZIP) —
    бот сам скачивает, распаковывает и сопоставляет файлы с BL активных партий.
    Формат имени файла: «БРЕНД N MESTA товар.xlsx» (например «LUCEAT 10 MESTA LYUSTRA.xlsx»).
    Алгоритм: 1) главный ключ — БРЕНД в начале имени (ищется по BL коду, имени клиента и названию группы);
    2) если бренд найден в нескольких партиях — решает MESTA: N сверяется с количеством коробок CTN/件数
    (quantity_places, данные из шитса Sklad, включая разбивку мест). Совпало у одного BL — прикрепляет;
    не совпало ни у кого — файл в отчёт «неоднозначные», человек решает. Отчёт на узбекском: прикрепил /
    расхождения mesta / неоднозначные / без совпадений. get_missing_packing_lists — текущий список без файлов.

12. РАСПИСАНИЕ БОТА (всё по Ташкенту, знай это ТОЧНО):
    • {ask_hour}:00 — утренний запрос обновления трекинга (список партий + кнопка формы) в группы с /formon;
    • {packing_hour}:00 — запрос packing list у Jigar в управляющей группе (BL активных партий без файлов);
    • с {digest_hour}:00 — утренний дайджест трекинга наблюдателю (id {digest_id}): бот ждёт, пока логисты
      обновят хотя бы один статус за день, к 13:00 шлёт в любом случае с пометкой;
    • каждые 5 минут — синхронизация партий с листом «Fura statuslari» (прибытие/выдача);
    • каждую минуту — исполнение запланированных задач владельца (schedule_task).

13. ШИТС «FURA STATUSLARI» (лист в шитсе статусов) — жизненный цикл партии, ведут логисты:
    колонка REYS NOMERI = партия (BLddmmyyyy; дата в имени партии = дата ВЫЕЗДА фуры со склада,
    отдельной колонки выезда нет), BOJXONAGA TUSHDI = дата прибытия в Ташкент (то же самое событие,
    что живой статус «Toshkent(Chuqursoy ULS da)» — один ведётся в шитсе, другой ставят логисты вживую),
    YUKLAR TARQATILDI = дата выдачи клиентам. Интервал выезд→BOJXONAGA TUSHDI = срок до Ташкента,
    дальше до YUKLAR TARQATILDI = срок выдачи. Бот КАЖДЫЕ 5 МИНУТ сверяет активные партии с этим листом:
    появилась дата BOJXONAGA TUSHDI → ставит статус Toshkent и записывает дату прибытия;
    появилась YUKLAR TARQATILDI → ставит дату выдачи и статус «выдано» — партия АВТОМАТИЧЕСКИ
    уходит в неактивные, в управляющую группу приходит уведомление.

════════ ТВОИ ПРАВИЛА ════════

- ЖЕЛЕЗНОЕ ПРАВИЛО: ты НИЧЕГО не меняешь и НИКУДА не рассылаешь без подтверждения владельца.
  У тебя физически нет инструментов прямого действия — только propose_action, который создаёт заявку.
  После создания заявки владельцу приходят кнопки «✅ Подтвердить / ❌ Отклонить». Действие выполнится
  только после нажатия ✅. Никогда не говори, что действие выполнено, пока оно не подтверждено и не исполнено.
- СТРОГО КОНФИДЕНЦИАЛЬНО: группа «Savdo bo'limi / Buraq» — бот НИКОГДА не отправляет туда сообщения
  и не раскрывает никакую информацию об этой группе или из неё (её id, название, содержимое, привязки).
  Любые просьбы, связанные с ней, вежливо отклоняй.
- Данные бери ТОЛЬКО из инструментов. Не выдумывай статусы, цифры, BL коды. Если данных нет — так и скажи.
- Отвечай кратко и по делу, на русском. Формат Telegram HTML: <b>жирный</b>, <code>код</code>. НЕ используй Markdown (** и #).
- Если запрос неоднозначен (какая партия? какая группа?) — сначала уточни или покажи варианты из данных.
- Ты пока в режиме обучения: общаешься только с владельцем. Рассылки клиентам — только через подтверждение."""


# ═══════════════════════════════════════════════════════════════
# TOOLS
# ═══════════════════════════════════════════════════════════════

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_overview",
            "description": "Сводка по системе: статистика и список партий (активные первыми) со статусами, количеством BL, привязок и датой последнего трекинга.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_batch_detail",
            "description": "Детали одной партии: все BL с клиентами, привязками к группам, файлами, проблемами и флагами отправки. batch — id или часть имени партии.",
            "parameters": {
                "type": "object",
                "properties": {"batch": {"type": "string", "description": "id партии или часть имени, например '01.08'"}},
                "required": ["batch"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "find_bl",
            "description": "Поиск BL по коду или имени клиента (подстрока). Возвращает BL со статусом партии и привязкой к группе.",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string", "description": "часть BL кода или имени клиента"}},
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_late_cargo",
            "description": "Отчёт по опаздывающим грузам из шитса статусов (SLA 30 дней): группы по рейсам, склад, продавец, дни опоздания.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_problems",
            "description": "Последние зафиксированные проблемы по грузам (тип, BL, описание, статус).",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "description": "сколько вернуть, по умолчанию 10"}},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_send_logs",
            "description": "Последние отправки трекинга (лог): BL, партия, статус, успех/ошибка, время.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "description": "сколько вернуть, по умолчанию 15"}},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_message_templates",
            "description": (
                "ТОЧНЫЕ шаблоны сообщений, которые бот реально отправляет: клиентское трекинг-сообщение "
                "(рендер настоящим кодом, uz и ru) и утренние служебные тексты. Используй ВСЕГДА, когда "
                "спрашивают про шаблоны/тексты сообщений — ничего не выдумывай."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_missing_packing_lists",
            "description": "BL активных партий, к которым ещё не прикреплён packing list (их бот утром запрашивает у ответственного).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_loading_plans",
            "description": (
                "Планы погрузки из шитса партий (месячные вкладки, например 'AVGUST 2026'). "
                "Каждый блок: дата отправки, название плана, список SHIPPING MARK (BL) с CTN/CBM/KG и датой прихода. "
                "kind='china' — фура со склада Китая до Хоргоса (по этому плану открывается партия); "
                "kind='kazakh' — казахская фура Horgos→Tashkent (обычно объединяет BL обеих китайских фур; "
                "по нему пересинхронизируется состав партии после статуса Horgos (Qozoq))."
            ),
            "parameters": {
                "type": "object",
                "properties": {"tab": {"type": "string", "description": "вкладка; пусто = текущий месяц"}},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "propose_action",
            "description": (
                "Создать ЗАЯВКУ на действие (требует подтверждения владельца кнопками). "
                "kind: 'set_batch_status' (params: batch_id, status — точное название из цепочки статусов), "
                "'send_tracking_batch' (params: batch_id — разослать трекинг по группам партии), "
                "'send_group_message' (params: chat_id, text — отправить сообщение в конкретную группу), "
                "'sync_batch_from_plan' (params: batch_id, tab, plan_title, plan_date — привести состав партии "
                "ТОЧНО к плану погрузки: недостающие BL добавляются с авто-привязкой Telegram-групп, а BL, "
                "которых в плане нет, УДАЛЯЮТСЯ из партии — иначе их группы получат чужой трекинг). "
                "summary: короткое человекочитаемое описание действия по-русски."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "enum": ["set_batch_status", "send_tracking_batch", "send_group_message", "sync_batch_from_plan"]},
                    "params": {"type": "object", "description": "параметры действия"},
                    "summary": {"type": "string", "description": "краткое описание для карточки подтверждения"},
                },
                "required": ["kind", "params", "summary"],
            },
        },
    },
]


def _find_batch(batch_ref: str):
    batches = db.get_batches()
    ref = str(batch_ref or "").strip().lower()
    if not ref:
        return None
    for b in batches:
        if str(b.get("id")) == ref:
            return b
    exact = [b for b in batches if str(b.get("name") or "").strip().lower() == ref]
    if len(exact) == 1:
        return exact[0]
    partial = [b for b in batches if ref in str(b.get("name") or "").lower()]
    if len(partial) == 1:
        return partial[0]
    return {"__ambiguous__": [
        {"id": b["id"], "name": b["name"], "status": b["status"]} for b in partial
    ]} if partial else None


def _batch_brief(b: dict) -> dict:
    return {
        "id": b.get("id"),
        "name": b.get("name"),
        "status": b.get("status"),
        "eta_to_toshkent": b.get("eta_to_toshkent") or "",
        "eta_destination": b.get("eta_destination") or "",
        "active": not (b.get("client_delivery_date") or ""),
        "bl_count": b.get("bl_count"),
        "linked_groups": b.get("linked_count"),
        "last_tracking_at": b.get("last_tracking_at") or "никогда",
    }


def _tool_get_overview(_args: dict) -> dict:
    stats = db.get_stats()
    batches = [_batch_brief(b) for b in db.get_batches()]
    active = [b for b in batches if b["active"]]
    inactive = [b for b in batches if not b["active"]]
    return {
        "stats": stats,
        "active_batches": active,
        "archived_batches_count": len(inactive),
        "archived_last_3": inactive[:3],
    }


def _tool_get_batch_detail(args: dict) -> dict:
    found = _find_batch(str(args.get("batch") or ""))
    if not found:
        return {"error": "Партия не найдена. Уточни имя или id (см. get_overview)."}
    if "__ambiguous__" in found:
        return {"error": "Найдено несколько партий, уточни какая", "candidates": found["__ambiguous__"]}
    bls = db.get_bl_by_batch(found["id"])
    return {
        "batch": _batch_brief(found),
        "bl_codes": [
            {
                "id": bl.get("id"),
                "code": bl.get("code"),
                "client": bl.get("client_name") or "",
                "phone": bl.get("phone") or "",
                "group_linked": bool(bl.get("chat_id")),
                "chat_id": _mask_chat_id(bl.get("chat_id") or ""),
                "files": bl.get("file_count") or 0,
                "open_problems": bl.get("problem_count") or 0,
                "excluded_from_send": bool(bl.get("send_excluded")),
                "tracking_sent_current": bool(bl.get("tracking_sent_current")),
            }
            for bl in bls
        ],
    }


def _tool_find_bl(args: dict) -> dict:
    query = str(args.get("query") or "").strip()
    if len(query) < 2:
        return {"error": "Запрос слишком короткий"}
    conn = db.get_conn()
    try:
        rows = conn.execute(
            """
            SELECT bl.id, bl.code, bl.client_name, bl.chat_id, bl.batch_id,
                   b.name AS batch_name, b.status AS batch_status,
                   COALESCE(b.client_delivery_date,'') AS delivered_date
            FROM bl_codes bl JOIN batches b ON b.id = bl.batch_id
            WHERE UPPER(bl.code) LIKE UPPER(?) OR UPPER(COALESCE(bl.client_name,'')) LIKE UPPER(?)
            ORDER BY bl.id DESC LIMIT 25
            """,
            (f"%{query}%", f"%{query}%"),
        ).fetchall()
    finally:
        conn.close()
    return {
        "matches": [
            {
                "bl_id": r["id"],
                "code": r["code"],
                "client": r["client_name"] or "",
                "group_linked": bool(r["chat_id"]),
                "chat_id": _mask_chat_id(r["chat_id"] or ""),
                "batch": r["batch_name"],
                "batch_status": r["batch_status"],
                "batch_active": not r["delivered_date"],
            }
            for r in rows
        ]
    }


def _tool_get_late_cargo(_args: dict) -> dict:
    from services import late_cargo_service

    rep = late_cargo_service.get_late_cargo_report()
    if not rep.get("ok"):
        return {"error": rep.get("error") or "Шитс статусов недоступен"}
    return {
        "total_late": rep.get("total_late"),
        "late_in_ombor": rep.get("late_in_ombor"),
        "late_in_transit": rep.get("late_in_transit"),
        "active_total": (rep.get("active_ombor") or 0) + (rep.get("active_transit") or 0),
        "groups": [
            {
                "reys": g.get("label"),
                "max_late_days": g.get("max_late"),
                "items": [
                    {
                        "brand": it.get("brand"),
                        "sklad": it.get("sklad"),
                        "seller": it.get("seller"),
                        "arrived": it.get("arrived"),
                        "days_late": it.get("days_late"),
                    }
                    for it in g.get("items", [])
                ],
            }
            for g in rep.get("groups", [])
        ],
        "ombor_watchlist_count": len(rep.get("ombor_warning") or []),
    }


def _tool_get_problems(args: dict) -> dict:
    limit = int(args.get("limit") or 10)
    problems = db.get_problems()[:limit]
    return {
        "problems": [
            {
                "id": p.get("id"),
                "bl_code": p.get("bl_code") or p.get("code") or "",
                "type": p.get("problem_type"),
                "description": (p.get("description") or "")[:200],
                "status": p.get("status"),
                "created_at": p.get("created_at"),
            }
            for p in problems
        ]
    }


def _tool_get_send_logs(args: dict) -> dict:
    limit = int(args.get("limit") or 15)
    conn = db.get_conn()
    try:
        rows = conn.execute(
            """
            SELECT bl_code, batch_name, status, success, error_msg, sent_at
            FROM send_logs ORDER BY id DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return {"logs": [dict(r) for r in rows]}


def _tool_get_message_templates(_args: dict) -> dict:
    sample = {
        "id": 0, "code": "BL-171", "client_name": "SARDOR", "chat_id": "",
        "status": "Almata", "message_language": "uz_latn",
        "weight_kg": 730, "volume_cbm": 5.2, "quantity_places": 40,
        "quantity_places_breakdown": "", "cargo_type": "", "cargo_description": "",
        "merged_codes": "",
    }
    uz = db.render_message(dict(sample), "14.08.2026", include_related_batches=False)
    sample["message_language"] = "ru"
    ru = db.render_message(dict(sample), "14.08.2026", include_related_batches=False)
    ask_hour = (os.getenv("TRACKING_ASK_HOUR", "7") or "7").strip()
    packing_hour = (os.getenv("PACKING_REMINDER_HOUR", "9") or "9").strip()
    return {
        "tracking_client_message_uz_latn": uz,
        "tracking_client_message_ru": ru,
        "notes": (
            "Клиентский трекинг — этот текст (приветствие и строка «🖇 packing list» обычные, середина "
            "— цитатой-«окном» blockquote), а СРАЗУ ПОД ним отдельным сообщением файлы packing list "
            "настоящими документами Telegram (альбом, без ответов и ссылок). Без фото, PNG и "
            "геолокаций. Пустой срок прибытия = ETA партии не заполнен. Языки: uz_latn, uz_cyrl, ru, "
            "en — по языку группы."
        ),
        "morning_service_texts": {
            "tracking_ask": f"«🌅 Assalomu alaykum! Treking ma'lumotlarini yangilash vaqti bo'ldi» + список партий + кнопка формы (в {ask_hour}:00, группы с /formon)",
            "packing_ask": f"«🌅 Assalomu alaykum, Jigar!» + BL без packing list по партиям + просьба прислать ZIP/ссылку (в {packing_hour}:00, управляющая группа)",
            "tasdiqlash_card": "«📝 <имя> treking ma'lumotlarini to'ldirdi: партия/holat/nuqta/ETA … ❓ Hammasi to'g'rimi?» + кнопки ✅/❌",
        },
    }


def _tool_get_missing_packing_lists(_args: dict) -> dict:
    conn = db.get_conn()
    try:
        rows = conn.execute(
            """
            SELECT bl.code, bl.client_name, b.name AS batch_name
            FROM bl_codes bl JOIN batches b ON b.id = bl.batch_id
            WHERE COALESCE(b.client_delivery_date, '') = ''
              AND NOT EXISTS (SELECT 1 FROM files f WHERE f.bl_id = bl.id)
            ORDER BY b.id DESC, bl.code
            """
        ).fetchall()
    finally:
        conn.close()
    return {
        "count": len(rows),
        "missing": [
            {"bl_code": r["code"], "client": r["client_name"] or "", "batch": r["batch_name"]}
            for r in rows
        ],
    }


def _tool_get_loading_plans(args: dict) -> dict:
    from services import loading_plan_service

    result = loading_plan_service.get_loading_plans(str(args.get("tab") or ""))
    if not result.get("ok"):
        return {"error": result.get("error"), "tabs": result.get("tabs")}
    plans = []
    for block in result["plans"]:
        plans.append({
            "date": block["date"],
            "title": block["title"],
            "kind": block["kind"],
            "warehouses": block["warehouses"],
            "total_ctn": block["total_ctn"],
            "total_cbm": block["total_cbm"],
            "total_kg": block["total_kg"],
            "marks": [
                {"mark": it["mark"], "cbm": it["cbm"], "arrive": it["arrive"]}
                for it in block["items"]
            ],
        })
    return {"tab": result["tab"], "tabs": result["tabs"], "plans": plans}


# Runtime-хуки прямого исполнения (устанавливает app.py при импорте —
# ai_assistant не может импортировать app из-за цикла).
direct_send_message = None   # fn(chat_id, text) -> None
direct_send_poll = None      # fn(chat_id, question, options, is_anonymous) -> None

# ── ВЛАДЕЛЬЧЕСКИЙ РЕЖИМ: прямые инструменты без подтверждения ──────
# Доступны ТОЛЬКО в личке владельца (owner_direct=True).
OWNER_DIRECT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "send_chat_message",
            "description": (
                "СРАЗУ отправить сообщение в любой чат/группу (без подтверждения). "
                "Можно отметить человека: mention_user_id (его Telegram id) + mention_label (как назвать) — "
                "упоминание кликабельно и приходит с уведомлением. Telegram HTML разрешён."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "chat_id": {"type": "string", "description": "id чата (группы отрицательные, личка = tg id)"},
                    "text": {"type": "string"},
                    "mention_user_id": {"type": "string", "description": "tg id человека для @упоминания (опц.)"},
                    "mention_label": {"type": "string", "description": "имя для упоминания (опц.)"},
                },
                "required": ["chat_id", "text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "send_poll",
            "description": "СРАЗУ создать опрос в чате/группе (Telegram poll). 2-10 вариантов.",
            "parameters": {
                "type": "object",
                "properties": {
                    "chat_id": {"type": "string"},
                    "question": {"type": "string"},
                    "options": {"type": "array", "items": {"type": "string"}},
                    "is_anonymous": {"type": "boolean", "description": "по умолчанию false (видно кто голосовал)"},
                },
                "required": ["chat_id", "question", "options"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "schedule_task",
            "description": (
                "Запланировать отправку сообщения или опроса на время (Ташкент). "
                "when: 'HH:MM' (сегодня; если время прошло — завтра) или 'YYYY-MM-DD HH:MM' или 'DD.MM.YYYY HH:MM'. "
                "daily=true — повторять каждый день. kind: 'send_message' (нужен text, опц. mention_user_id/mention_label) "
                "или 'send_poll' (нужны question и options)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "when": {"type": "string"},
                    "daily": {"type": "boolean"},
                    "kind": {"type": "string", "enum": ["send_message", "send_poll"]},
                    "chat_id": {"type": "string"},
                    "text": {"type": "string"},
                    "mention_user_id": {"type": "string"},
                    "mention_label": {"type": "string"},
                    "question": {"type": "string"},
                    "options": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["when", "kind", "chat_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_scheduled_tasks",
            "description": "Список запланированных задач (id, время, повтор, что и куда).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "cancel_scheduled_task",
            "description": "Отменить запланированную задачу по id.",
            "parameters": {
                "type": "object",
                "properties": {"task_id": {"type": "integer"}},
                "required": ["task_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_groups",
            "description": "Известные боту Telegram-группы: название, chat_id, активна ли. Для выбора куда писать/опрашивать.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
]


def _mention_html(user_id, label) -> str:
    from html import escape
    return f'<a href="tg://user?id={str(user_id).strip()}">{escape(str(label or "👤"))}</a>'


def _guard_target_chat(chat_id) -> str | None:
    value = str(chat_id or "").strip()
    if not value:
        return "chat_id пуст"
    if value in confidential_chat_ids():
        return "Эта группа строго конфиденциальна — туда писать нельзя."
    return None


def _tool_send_chat_message(args: dict) -> dict:
    err = _guard_target_chat(args.get("chat_id"))
    if err:
        return {"error": err}
    if not callable(direct_send_message):
        return {"error": "direct_send_message hook is not wired"}
    text = str(args.get("text") or "").strip()
    if not text:
        return {"error": "Пустой текст"}
    mention_id = str(args.get("mention_user_id") or "").strip()
    if mention_id:
        text = f"{_mention_html(mention_id, args.get('mention_label'))}, {text}"
    direct_send_message(str(args.get("chat_id")).strip(), text)
    return {"ok": True, "sent_to": str(args.get("chat_id")).strip()}


def _tool_send_poll(args: dict) -> dict:
    err = _guard_target_chat(args.get("chat_id"))
    if err:
        return {"error": err}
    if not callable(direct_send_poll):
        return {"error": "direct_send_poll hook is not wired"}
    question = str(args.get("question") or "").strip()
    options = [str(o).strip() for o in (args.get("options") or []) if str(o).strip()]
    if not question or len(options) < 2:
        return {"error": "Нужен вопрос и минимум 2 варианта"}
    direct_send_poll(str(args.get("chat_id")).strip(), question, options[:10], bool(args.get("is_anonymous", False)))
    return {"ok": True}


def _parse_when(value: str):
    from datetime import datetime, timedelta

    tz = db.TASHKENT_TZ
    raw = str(value or "").strip()
    now = datetime.now(tz)
    try:
        if re.match(r"^\d{1,2}:\d{2}$", raw):
            hh, mm = raw.split(":")
            candidate = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
            if candidate <= now:
                candidate += timedelta(days=1)
            return candidate
        if re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}$", raw):
            return datetime.strptime(raw, "%Y-%m-%d %H:%M").replace(tzinfo=tz)
        if re.match(r"^\d{2}\.\d{2}\.\d{4}\s+\d{1,2}:\d{2}$", raw):
            return datetime.strptime(raw, "%d.%m.%Y %H:%M").replace(tzinfo=tz)
    except ValueError:
        return None
    return None


def _tool_schedule_task(args: dict, tg_user_id: str) -> dict:
    err = _guard_target_chat(args.get("chat_id"))
    if err:
        return {"error": err}
    kind = str(args.get("kind") or "").strip()
    if kind not in {"send_message", "send_poll"}:
        return {"error": "kind должен быть send_message или send_poll"}
    when = _parse_when(str(args.get("when") or ""))
    if not when:
        return {"error": "Не понял время. Форматы: 'HH:MM', 'YYYY-MM-DD HH:MM', 'DD.MM.YYYY HH:MM'"}
    params = {"chat_id": str(args.get("chat_id")).strip()}
    if kind == "send_message":
        text = str(args.get("text") or "").strip()
        if not text:
            return {"error": "Для send_message нужен text"}
        params["text"] = text
        if str(args.get("mention_user_id") or "").strip():
            params["mention_user_id"] = str(args.get("mention_user_id")).strip()
            params["mention_label"] = str(args.get("mention_label") or "").strip()
    else:
        question = str(args.get("question") or "").strip()
        options = [str(o).strip() for o in (args.get("options") or []) if str(o).strip()]
        if not question or len(options) < 2:
            return {"error": "Для send_poll нужны question и минимум 2 options"}
        params["question"] = question
        params["options"] = options[:10]
    recurrence = "daily" if args.get("daily") else "once"
    task_id = db.ai_create_scheduled_task(
        tg_user_id, when.strftime("%Y-%m-%d %H:%M"), recurrence, kind,
        json.dumps(params, ensure_ascii=False),
    )
    return {
        "ok": True,
        "task_id": task_id,
        "run_at": when.strftime("%d.%m.%Y %H:%M"),
        "recurrence": recurrence,
    }


def _tool_list_scheduled_tasks(_args: dict) -> dict:
    tasks = db.ai_list_scheduled_tasks("pending")
    return {
        "tasks": [
            {
                "id": t["id"],
                "run_at": t["run_at"],
                "recurrence": t["recurrence"],
                "kind": t["kind"],
                "params": json.loads(t.get("params_json") or "{}"),
            }
            for t in tasks
        ]
    }


def _tool_cancel_scheduled_task(args: dict) -> dict:
    ok = db.ai_cancel_scheduled_task(int(args.get("task_id") or 0))
    return {"ok": ok} if ok else {"error": "Задача не найдена или уже не pending"}


def _tool_list_groups(_args: dict) -> dict:
    conn = db.get_conn()
    try:
        rows = conn.execute(
            "SELECT chat_id, title, is_active FROM telegram_chats ORDER BY is_active DESC, last_seen_at DESC LIMIT 60"
        ).fetchall()
    finally:
        conn.close()
    hidden = confidential_chat_ids()
    return {
        "groups": [
            {
                "chat_id": ("🔒 конфиденциально" if str(r["chat_id"]) in hidden else r["chat_id"]),
                "title": ("🔒" if str(r["chat_id"]) in hidden else (r["title"] or "")),
                "active": bool(r["is_active"]),
            }
            for r in rows
        ]
    }


ALLOWED_ACTION_KINDS = {"set_batch_status", "send_tracking_batch", "send_group_message", "sync_batch_from_plan"}


def _tool_propose_action(args: dict, tg_user_id: str, created_actions: list) -> dict:
    kind = str(args.get("kind") or "").strip()
    params = args.get("params") or {}
    summary = str(args.get("summary") or "").strip()
    if kind not in ALLOWED_ACTION_KINDS:
        return {"error": f"Неизвестный вид действия: {kind}"}
    if not isinstance(params, dict):
        return {"error": "params должен быть объектом"}
    if not summary:
        return {"error": "Нужно короткое описание (summary)"}

    # Validate params early so the owner never confirms a broken action.
    if kind == "set_batch_status":
        batch = db.get_batch(int(params.get("batch_id") or 0))
        if not batch:
            return {"error": "batch_id не найден"}
        status = str(params.get("status") or "").strip()
        valid = set(db.STATUSES) | {db.DELIVERED_STATUS}
        if status not in valid:
            return {"error": f"Недопустимый статус. Разрешены: {sorted(valid)}"}
        params["batch_name"] = batch["name"]
    elif kind == "send_tracking_batch":
        batch = db.get_batch(int(params.get("batch_id") or 0))
        if not batch:
            return {"error": "batch_id не найден"}
        params["batch_name"] = batch["name"]
    elif kind == "send_group_message":
        chat_id = str(params.get("chat_id") or "").strip()
        text = str(params.get("text") or "").strip()
        if not chat_id or not text:
            return {"error": "Нужны chat_id и text"}
        if chat_id in confidential_chat_ids():
            return {"error": "Эта группа строго конфиденциальна — бот туда не пишет. Заявка не создана."}
    elif kind == "sync_batch_from_plan":
        from services import loading_plan_service

        batch = db.get_batch(int(params.get("batch_id") or 0))
        if not batch:
            return {"error": "batch_id не найден"}
        plan = loading_plan_service.find_plan(
            str(params.get("tab") or ""),
            str(params.get("plan_title") or ""),
            str(params.get("plan_date") or ""),
        )
        if not plan:
            return {
                "error": "План не найден или найдено несколько — уточни tab, plan_title и plan_date "
                         "(посмотри get_loading_plans)."
            }
        # Snapshot the plan composition INTO the pending action: the owner
        # approves exactly this list, later sheet edits can't change it.
        agg: dict = {}
        for it in plan["items"]:
            key = it["mark"].strip().upper()
            entry = agg.setdefault(key, {"mark": it["mark"].strip(), "ctn": 0.0, "cbm": 0.0, "kg": 0.0})
            entry["ctn"] += it["ctn"]
            entry["cbm"] += it["cbm"]
            entry["kg"] += it["kg"]
        params["marks"] = [
            {"mark": e["mark"], "ctn": round(e["ctn"], 2), "cbm": round(e["cbm"], 3), "kg": round(e["kg"], 2)}
            for e in agg.values()
        ]
        params["batch_name"] = batch["name"]
        params["tab"] = plan["tab"]
        params["plan_title"] = plan["title"]
        params["plan_date"] = plan["date"]

    action_id = db.ai_create_pending_action(tg_user_id, kind, json.dumps(params, ensure_ascii=False), summary)
    created_actions.append({"id": action_id, "kind": kind, "summary": summary})
    return {
        "ok": True,
        "pending_action_id": action_id,
        "note": "Заявка создана. Владельцу отправлены кнопки подтверждения — действие выполнится только после ✅.",
    }


_OWNER_TOOL_NAMES = {t["function"]["name"] for t in OWNER_DIRECT_TOOLS}


def _run_tool(name: str, args: dict, tg_user_id: str, created_actions: list,
              readonly: bool = False, owner_direct: bool = False) -> dict:
    try:
        if readonly and name == "propose_action":
            return {"error": "У этого пользователя доступ только на чтение — действия недоступны."}
        if name in _OWNER_TOOL_NAMES:
            if not owner_direct:
                return {"error": "Прямые инструменты доступны только владельцу в личке."}
            if name == "send_chat_message":
                return _tool_send_chat_message(args)
            if name == "send_poll":
                return _tool_send_poll(args)
            if name == "schedule_task":
                return _tool_schedule_task(args, tg_user_id)
            if name == "list_scheduled_tasks":
                return _tool_list_scheduled_tasks(args)
            if name == "cancel_scheduled_task":
                return _tool_cancel_scheduled_task(args)
            if name == "list_groups":
                return _tool_list_groups(args)
        if name == "get_overview":
            return _tool_get_overview(args)
        if name == "get_batch_detail":
            return _tool_get_batch_detail(args)
        if name == "find_bl":
            return _tool_find_bl(args)
        if name == "get_late_cargo":
            return _tool_get_late_cargo(args)
        if name == "get_problems":
            return _tool_get_problems(args)
        if name == "get_send_logs":
            return _tool_get_send_logs(args)
        if name == "get_loading_plans":
            return _tool_get_loading_plans(args)
        if name == "get_missing_packing_lists":
            return _tool_get_missing_packing_lists(args)
        if name == "get_message_templates":
            return _tool_get_message_templates(args)
        if name == "propose_action":
            return _tool_propose_action(args, tg_user_id, created_actions)
        return {"error": f"Неизвестный инструмент {name}"}
    except Exception as exc:  # tools must never crash the loop
        return {"error": f"Ошибка инструмента: {exc}"}


# ═══════════════════════════════════════════════════════════════
# CHAT LOOP
# ═══════════════════════════════════════════════════════════════

def _chat_completion(messages, use_model=None, tools=None):
    payload = {
        "model": use_model or _model(),
        "messages": messages,
        "tools": tools if tools is not None else TOOLS,
        "temperature": 0.3,
    }
    response = req.post(
        f"{DEEPSEEK_BASE_URL}/chat/completions",
        headers={
            "Authorization": f"Bearer {_api_key()}",
            "Content-Type": "application/json",
        },
        data=json.dumps(payload),
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code == 400 and use_model is None:
        # Unknown model name (e.g. the configured one isn't available on
        # this account) — retry once with the standard model.
        body = response.text or ""
        if "model" in body.lower():
            return _chat_completion(messages, use_model=FALLBACK_MODEL, tools=tools)
    response.raise_for_status()
    return response.json()


def handle_owner_message(tg_user_id, text: str, readonly: bool = False, owner_direct: bool = False) -> dict:
    """Process one owner/staff message. Returns {"reply", "pending": [...]}.

    readonly=True (просмотровый доступ): все READ-инструменты доступны,
    propose_action вырезан — пользователь физически не может ничего
    изменить или разослать.
    owner_direct=True (личка владельца): дополнительно ПРЯМЫЕ инструменты
    без подтверждений — сообщения/упоминания, опросы, задачи по расписанию."""
    tg_user_id = str(tg_user_id)
    text = str(text or "").strip()

    if not _api_key():
        return {
            "reply": (
                "🤖 AI-ассистент ещё не подключён: не задан <b>DEEPSEEK_API_KEY</b>.\n"
                "Создай API ключ на platform.deepseek.com → API Keys и добавь его в переменные окружения "
                "(.env локально и Railway), затем перезапусти сервис."
            ),
            "pending": [],
        }

    with _history_lock:
        history = db.ai_get_history(tg_user_id)
        db.ai_add_message(tg_user_id, "user", text)

    system_prompt = _system_prompt()
    tools = TOOLS
    if readonly:
        tools = [t for t in TOOLS if t["function"]["name"] != "propose_action"]
        system_prompt += (
            "\n\nРЕЖИМ ТОЛЬКО ЧТЕНИЕ: этот пользователь может смотреть любую информацию, но НЕ может "
            "ничего менять или рассылать — инструмента propose_action у тебя сейчас нет. На просьбы "
            "что-то изменить/отправить отвечай, что у него просмотровый доступ, изменения делает владелец "
            "или управляющая группа."
        )
    elif owner_direct:
        tools = TOOLS + OWNER_DIRECT_TOOLS
        system_prompt += (
            "\n\nРЕЖИМ ВЛАДЕЛЬЦА (личка): тебе доступны ПРЯМЫЕ инструменты, исполняемые СРАЗУ, без карточек "
            "подтверждения — владелец сам даёт команду, это и есть подтверждение:\n"
            "• send_chat_message — написать в любой чат/группу, можно отметить человека "
            "(mention_user_id + mention_label; id ищи через list_groups, get_batch_detail, find_bl или спроси владельца);\n"
            "• send_poll — создать опрос в группе;\n"
            "• schedule_task — запланировать сообщение/опрос на время (once или daily, время Ташкента), "
            "list_scheduled_tasks / cancel_scheduled_task — управлять планом;\n"
            "• list_groups — список известных групп с chat_id.\n"
            "Выполняй такие просьбы немедленно и отчитывайся, что сделано. "
            "ИСКЛЮЧЕНИЯ: конфиденциальная группа — по-прежнему абсолютное табу; массовая рассылка трекинга "
            "по клиентским группам — по-прежнему только через propose_action (send_tracking_batch)."
        )

    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(history)
    messages.append({"role": "user", "content": text})

    created_actions: list = []
    reply_text = ""
    try:
        for _round in range(MAX_TOOL_ROUNDS):
            data = _chat_completion(messages, tools=tools)
            choice = (data.get("choices") or [{}])[0]
            msg = choice.get("message") or {}
            tool_calls = msg.get("tool_calls") or []
            if not tool_calls:
                reply_text = (msg.get("content") or "").strip()
                break
            # Append the assistant tool-call turn, then each tool result.
            messages.append({
                "role": "assistant",
                "content": msg.get("content") or "",
                "tool_calls": tool_calls,
            })
            for call in tool_calls:
                fn = (call.get("function") or {})
                name = fn.get("name") or ""
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except Exception:
                    args = {}
                result = _run_tool(name, args, tg_user_id, created_actions, readonly=readonly, owner_direct=owner_direct)
                messages.append({
                    "role": "tool",
                    "tool_call_id": call.get("id"),
                    "content": json.dumps(result, ensure_ascii=False, default=str)[:12000],
                })
        else:
            reply_text = "⚠️ Слишком длинная цепочка инструментов — попробуй сформулировать запрос конкретнее."
    except req.RequestException as exc:
        reply_text = f"⚠️ Ошибка DeepSeek API: {exc}"
    except Exception as exc:
        reply_text = f"⚠️ Внутренняя ошибка ассистента: {exc}"

    if not reply_text:
        reply_text = "Готово." if created_actions else "Не смог сформировать ответ, попробуй ещё раз."

    with _history_lock:
        db.ai_add_message(tg_user_id, "assistant", reply_text)

    return {"reply": reply_text, "pending": created_actions}


def reset_history(tg_user_id) -> None:
    db.ai_clear_history(str(tg_user_id))


# ═══════════════════════════════════════════════════════════════
# GROUP AGENT — client-scoped smart replies on @mention
# ═══════════════════════════════════════════════════════════════
# Same DeepSeek brain as the owner assistant, but the ONLY data it can
# reach is the cargo linked to the group it's answering in (the tool is
# pre-bound to that chat_id — the model can't ask about другие группы).

def _group_model() -> str:
    return (os.getenv("DEEPSEEK_GROUP_MODEL") or "").strip() or _model()


GROUP_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_group_cargo",
            "description": (
                "Все грузы ЭТОЙ группы: BL код, клиент, партия (рейс), текущий статус, "
                "ETA до Ташкента, конечная точка, в пути / прибыл / выдан, количество packing-list файлов, "
                "дата последнего трекинга."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
]


def _tool_group_cargo(chat_id: str) -> dict:
    conn = db.get_conn()
    try:
        rows = conn.execute(
            """
            SELECT bl.code, bl.client_name, b.name AS batch_name, b.status,
                   COALESCE(b.eta_to_toshkent,'') AS eta_to_toshkent,
                   COALESCE(b.eta_destination,'') AS eta_destination,
                   COALESCE(b.client_delivery_date,'') AS delivered_date,
                   (SELECT COUNT(*) FROM files f WHERE f.bl_id = bl.id) AS files,
                   (SELECT MAX(sl.sent_at) FROM send_logs sl WHERE sl.bl_id = bl.id AND sl.success = 1) AS last_tracking_at
            FROM bl_codes bl JOIN batches b ON b.id = bl.batch_id
            WHERE TRIM(bl.chat_id) = ?
            ORDER BY b.id DESC
            LIMIT 30
            """,
            (str(chat_id).strip(),),
        ).fetchall()
    finally:
        conn.close()
    arrived = set(db.ARRIVED_STATUSES)
    cargo = []
    for r in rows:
        active = not r["delivered_date"]
        delivered = r["status"] in {db.DELIVERED_STATUS, db.LEGACY_DELIVERED_STATUS} or bool(r["delivered_date"])
        cargo.append({
            "bl_code": r["code"],
            "client": r["client_name"] or "",
            "batch": r["batch_name"],
            "status": r["status"],
            "eta_to_toshkent": r["eta_to_toshkent"],
            "destination": r["eta_destination"],
            "state": ("выдан клиенту" if delivered
                      else "прибыл (на складе Ташкента)" if r["status"] in arrived
                      else "в пути" if active
                      else "архив"),
            "packing_list_files": r["files"],
            "last_tracking_sent": r["last_tracking_at"] or "ещё не отправлялся",
        })
    return {"cargo": cargo, "count": len(cargo)}


def _group_system_prompt(chat_title: str) -> str:
    from datetime import datetime

    statuses = " → ".join(db.STATUSES)
    return f"""Ты — умный ассистент карго-компании BURAQ Logistics (доставка Китай → Узбекистан) в Telegram-группе клиента.
Группа: «{chat_title}». Сегодня: {datetime.now().strftime('%d.%m.%Y')}.
Тебя отмечают (@) в группе и задают вопросы — ты отвечаешь легко, точно и по-человечески.

КАК УСТРОЕНА ДОСТАВКА:
- Груз клиента идёт под BL кодом внутри партии (рейса). Статус партии — точка маршрута:
  {statuses}.
- Два маршрута: через Казахстан (Horgos→Nurjo'li→Jarkent→Almata→Taraz→Shimkent→Qonusbay→Saryagash→Yallama→Toshkent)
  и через Кыргызстан (Kashgar→Irkeshtam→Osh→Dostlik→Andijon).
- «Toshkent(Chuqursoy ULS da)» = груз прибыл на склад в Ташкенте. «{db.DELIVERED_STATUS}» = выдан клиенту.
- Трекинг приходит в эту группу автоматически при движении фуры. Packing list (упаковочный лист) приходит файлом
  кнопкой под сообщением трекинга.

ДАННЫЕ:
- Единственный источник данных — инструмент get_group_cargo: в нём ВСЕ грузы именно этой группы.
- ВСЕГДА вызывай его прежде чем отвечать на вопрос о грузах. Никогда ничего не выдумывай.
- НЕ проси у клиента BL код или «номер накладной»: данные группы у тебя уже есть. Если грузов несколько и вопрос
  неоднозначен — просто перечисли все с их статусами.
- Если в данных пусто — скажи, что в этой группе грузы пока не числятся, и предложи обратиться к менеджеру.

СТИЛЬ:
- Отвечай на языке вопроса (узбекский/русский). Коротко, тепло, без канцелярита и без роботных шаблонов.
- Telegram HTML: <b>жирный</b>, <code>код</code>. НЕ используй Markdown (** и #).
- Ты видишь только данные этой группы. О других клиентах, ценах, внутренних делах компании — не говори,
  предлагай связаться с менеджером."""


def handle_group_question(chat_id, chat_title: str, question: str, sender_name: str = "") -> str:
    """Answer one @mention question in a client group. Returns "" when
    the agent is unavailable (no key / API error) so the caller can fall
    back to the legacy intent flow."""
    if not _api_key():
        return ""
    chat_key = f"group:{chat_id}"
    question = str(question or "").strip()
    user_line = f"{sender_name}: {question}" if sender_name else question

    with _history_lock:
        history = db.ai_get_history(chat_key, limit=12)
        db.ai_add_message(chat_key, "user", user_line)

    messages = [{"role": "system", "content": _group_system_prompt(chat_title)}]
    messages.extend(history)
    messages.append({"role": "user", "content": user_line})

    reply_text = ""
    try:
        for _round in range(4):
            data = _chat_completion(messages, use_model=_group_model(), tools=GROUP_TOOLS)
            choice = (data.get("choices") or [{}])[0]
            msg = choice.get("message") or {}
            tool_calls = msg.get("tool_calls") or []
            if not tool_calls:
                reply_text = (msg.get("content") or "").strip()
                break
            messages.append({
                "role": "assistant",
                "content": msg.get("content") or "",
                "tool_calls": tool_calls,
            })
            for call in tool_calls:
                name = ((call.get("function") or {}).get("name")) or ""
                result = _tool_group_cargo(chat_id) if name == "get_group_cargo" else {"error": "unknown tool"}
                messages.append({
                    "role": "tool",
                    "tool_call_id": call.get("id"),
                    "content": json.dumps(result, ensure_ascii=False, default=str)[:8000],
                })
    except Exception:
        return ""

    if reply_text:
        with _history_lock:
            db.ai_add_message(chat_key, "assistant", reply_text)
    return reply_text
