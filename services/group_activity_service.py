"""Активность клиентских Telegram-групп: кто «заснул» и кого пора будить.

Сигналов два, и решают они ВМЕСТЕ: ГРУЗ (когда клиент последний раз давал нам
груз — дата партии) и ГОЛОС (когда сам клиент, а не наш сотрудник, писал в
группе). Молчание между отгрузками само по себе не сон: клиент, который возит
раз в квартал, молчит законно — поэтому считаем и его обычный ритм отгрузок.

Источники: bl_codes+batches (груз), moderator_response_requests (каждое
сообщение клиента и ответ на него), telegram_chat_members (кто и когда был
в группе), send_logs (рассылки), communication_ratings (оценки), problems.

СОН = клиент перестал давать груз (владелец, 14.10.2026). Решает ГРУЗ и
личный ритм клиента: возивший раз в неделю и пропавший на месяц уже
затихает, возивший раз в квартал и молчащий 100 дней — ещё нет. Переписка
— отдельный признак («пишет, но не грузит»), активным она группу больше
не делает. Клиент, чей код теперь едет через другую группу, — не спящий,
а «переехал».

КЛИЕНТЫ И СОТРУДНИКИ. Bot API не отдаёт список участников группы: бот знает
тех, кто писал, кому отвечали, и админов (getChatAdministrators), плюс общее
число участников (getChatMemberCount). Сотрудник — из списка сотрудников
владельца (staff_registry), из ролей бота, участник Tracking gruppa или
человек, которого бот видит в ≥3 группах (кроме подтверждённых клиентов).
"""
from __future__ import annotations

import os
import re
import time
from datetime import date, datetime, timedelta

import database as db

_DATE_RE = re.compile(r"(?<!\d)(\d{2})\.(\d{2})\.(\d{4})(?!\d)")
# наш сотрудник пишет во многих группах, клиент живёт в одной-двух своих
STAFF_MIN_GROUPS = 3

SEGMENT_LABELS = {
    "active": "🟢 активен",
    "fading": "🟡 затихает",
    "asleep": "🔴 заснул",
    "lost": "⚫ потерян (груза нет полгода и больше)",
    "moved": "🔀 возит через другую группу",
    "new": "⚪ бот в группе недавно — истории ещё нет",
    "no_cargo": "◻️ груз в системе не виден (BL не привязан или так и не отгрузил)",
}
STAFF_REGISTRY_KEY = "staff_registry"              # [{id, username, name}] — список владельца
CONFIRMED_CLIENTS_KEY = "staff_confirmed_clients"  # [user_id] — «это клиент, хоть и во многих группах»
# подписи наших людей в Telegram: «Buraq Logistics Sales Manager», «…_Logist»
_STAFF_NAME_HINT = re.compile(
    r"buraq|logist|sales.?manager|head.?of.?sales|moderator|retention|tracking.?manager|customs|collector",
    re.IGNORECASE)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "") or default)
    except ValueError:
        return default


def thresholds() -> dict:
    """Пороги «сна». Без ритма (меньше 3 партий) — фиксированные дни; с
    ритмом — кратные обычного перерыва клиента, но не меньше «полов»."""
    return {
        "active_cargo_days": _env_int("ACTIVITY_ACTIVE_CARGO_DAYS", 45),    # затихает после, если ритма нет
        "asleep_cargo_days": _env_int("ACTIVITY_ASLEEP_CARGO_DAYS", 90),    # заснул после, если ритма нет
        "lost_cargo_days": _env_int("ACTIVITY_LOST_CARGO_DAYS", 180),       # потерян после (и пол для ритма)
        "fade_floor_days": _env_int("ACTIVITY_FADE_FLOOR_DAYS", 30),
        "asleep_floor_days": _env_int("ACTIVITY_ASLEEP_FLOOR_DAYS", 60),
        "fade_x_gap": 2, "asleep_x_gap": 3, "lost_x_gap": 6,
        "rhythm_min_batches": 3,
        "active_msg_days": _env_int("ACTIVITY_ACTIVE_MSG_DAYS", 21),        # «пишет, но не грузит»
        "moved_window_days": _env_int("ACTIVITY_MOVED_WINDOW_DAYS", 60),
        "new_group_days": _env_int("ACTIVITY_NEW_GROUP_DAYS", 45),
    }


def rhythm_limits(gap: int | None, shipments: int, t: dict) -> dict:
    """{fade, asleep, lost, basis}: через сколько дней без груза клиент затихает / заснул / потерян."""
    if gap and shipments >= t["rhythm_min_batches"]:
        # кратные ритма, но в разумной вилке: редкий клиент (раз в полгода)
        # не должен годами числиться активным
        clamp = lambda v, lo, hi: max(lo, min(v, hi))  # noqa: E731
        return {
            "fade": clamp(t["fade_x_gap"] * gap, t["fade_floor_days"], 120),
            "asleep": clamp(t["asleep_x_gap"] * gap, t["asleep_floor_days"], 180),
            "lost": clamp(t["lost_x_gap"] * gap, t["lost_cargo_days"], 365),
            "basis": f"обычно возит раз в {gap} дн.",
        }
    return {"fade": t["active_cargo_days"], "asleep": t["asleep_cargo_days"], "lost": t["lost_cargo_days"],
            "basis": "ритма ещё нет (меньше 3 отгрузок)"}


def _today() -> date:
    return datetime.now(db.TASHKENT_TZ).date()


def _day(value) -> date | None:
    text = str(value or "").strip()[:10]
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _batch_day(name, created_at) -> date | None:
    """Дата груза = дата в названии партии («05.09.2026 ZH»), иначе дата создания."""
    m = _DATE_RE.search(str(name or ""))
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    return _day(created_at)


def _ago(day: date | None, today: date) -> int | None:
    return max(0, (today - day).days) if day else None


# ── ИСТОРИЯ ГРУЗОВ ИЗ ШИТСА ───────────────────────────────────────────
# База помнит партии с апреля 2026, а бота добавили во многие группы только
# в сентябре: для них «груза нет», хотя клиент возил годами. Китайские планы
# всех месячных вкладок шитса (с мая 2024) — вторая история: марка клиента
# по датам фур. Код берём из BL группы и из её названия («PIMAK (BL-139)»).
SHEET_CACHE_SECONDS = 3600
_SHEET_CACHE: dict = {"at": 0.0, "index": None, "error": ""}
_TITLE_BL_RE = re.compile(r"\bBL\s*[-–]?\s*(\d{1,5})\b", re.IGNORECASE)
_TITLE_NOISE = {"BURAQ", "BURAQLOGISTICS", "LOGISTICS", "GROUP", "GURUH"}
SHIPMENT_MERGE_DAYS = 5     # две фуры одного клиента за несколько дней — одна отгрузка


def _norm_mark(value) -> str:
    text = str(value or "").strip().upper()
    text = re.sub(r"\.0+$", "", text)
    return re.sub(r"[\s\-_.()\[\]]+", "", text)


def title_codes(title) -> set:
    """«PIMAK (BL-139) & BURAQ LOGISTICS» → {«BL139», «PIMAK»}."""
    head = re.split(r"&", str(title or ""), maxsplit=1)[0]
    codes = {"BL" + m.group(1) for m in _TITLE_BL_RE.finditer(head)}
    brand = _norm_mark(_TITLE_BL_RE.sub(" ", head))
    if len(brand) >= 3 and brand not in _TITLE_NOISE:
        codes.add(brand)
    return codes


def sheet_cargo_index(force: bool = False) -> tuple:
    """({марка: {даты фур}}, ошибка) из китайских планов шитса; кэш на час."""
    now = time.time()
    if not force and _SHEET_CACHE["index"] is not None and now - _SHEET_CACHE["at"] < SHEET_CACHE_SECONDS:
        return _SHEET_CACHE["index"], _SHEET_CACHE["error"]
    try:
        from services import plan_sync_service as pss

        index: dict = {}
        for block in pss.all_blocks():
            if block.get("kind") != "china":
                continue
            try:
                day = datetime.strptime(pss.date_key(block.get("date")), "%d%m%Y").date()
            except ValueError:
                continue
            for item in block.get("items") or []:
                mark = _norm_mark(item.get("mark"))
                if mark:
                    index.setdefault(mark, set()).add(day)
        error = ""
    except Exception as exc:  # шитс недоступен — анализ идёт по базе
        index, error = (_SHEET_CACHE["index"] or {}), f"шитс недоступен: {exc}"[:200]
    _SHEET_CACHE.update(at=now, index=index, error=error)
    return index, error


def _merge_days(days: list) -> list:
    """Отгрузки клиента: даты фур, где соседние ближе SHIPMENT_MERGE_DAYS, — одна."""
    out = []
    for d in sorted(days):
        if out and (d - out[-1]).days < SHIPMENT_MERGE_DAYS:
            out[-1] = d
        else:
            out.append(d)
    return out


def _members_clock_shift(conn) -> timedelta:
    """telegram_chat_members.last_seen_at пишется datetime('now','localtime')
    сервера — на Railway это UTC, а requested_at журнала — по Ташкенту.
    Сдвиг, переводящий первое во второе (на проде +5 ч). Без него ответ
    сотрудника через 10 минут выглядел «раньше» вопроса клиента, и 84 группы
    из 95 ложно числились «ждут ответа» (30.09.2026)."""
    try:
        row = conn.execute("SELECT datetime('now','localtime') AS t").fetchone()
        sqlite_now = datetime.strptime(str(row["t"]), "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, KeyError, IndexError):
        return timedelta(0)
    tash_now = datetime.now(db.TASHKENT_TZ).replace(tzinfo=None)
    return timedelta(minutes=round((tash_now - sqlite_now).total_seconds() / 900) * 15)


def _to_tashkent(value, shift: timedelta) -> str:
    """Отметка last_seen_at во времени Ташкента («YYYY-MM-DD HH:MM:SS»)."""
    text = str(value or "").strip()[:19]
    if not text or not shift:
        return text
    try:
        return (datetime.strptime(text, "%Y-%m-%d %H:%M:%S") + shift).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return text


def _excluded_chat_ids() -> set:
    from services import ai_assistant

    out = set(ai_assistant.confidential_chat_ids())
    control = str(ai_assistant.control_group_id() or "").strip()
    if control:
        out.add(control)
    return out


def _json_setting(key: str, default):
    import json
    raw = db.get_setting(key, None)
    if raw is None:
        return default
    try:
        value = json.loads(raw)
        return value if isinstance(value, type(default)) else default
    except (TypeError, ValueError):
        return default


def staff_registry() -> list:
    """Список сотрудников владельца: [{id, username, name}] (id может быть пустым,
    пока человек с таким ником не встретился боту)."""
    out = []
    for item in _json_setting(STAFF_REGISTRY_KEY, []):
        if isinstance(item, dict):
            out.append({"id": str(item.get("id") or "").strip(),
                        "username": str(item.get("username") or "").strip().lstrip("@").lower(),
                        "name": str(item.get("name") or "").strip()})
    return [x for x in out if x["id"] or x["username"]]


def confirmed_clients() -> set:
    return {str(x).strip() for x in _json_setting(CONFIRMED_CLIENTS_KEY, []) if str(x).strip()}


def _role_ids() -> dict:
    """{user_id: роль} — все, у кого есть роль в боте."""
    from services import ai_assistant

    out = {}
    for uid in ai_assistant.admin_ids():
        out[str(uid)] = "админ бота"
    for uid in ai_assistant.operator_ids():
        out.setdefault(str(uid), "оператор бота")
    for uid in ai_assistant.readonly_ids():
        out.setdefault(str(uid), "доступ «только чтение»")
    for uid, nick in ai_assistant.sales_users().items():
        out.setdefault(str(uid), f"продавец ({nick})")
    for env, label in (("HOJI_DODAM_TG_ID", "Hoji dodam"), ("PACKING_RESPONSIBLE_TG_ID", "Jigar")):
        default = {"HOJI_DODAM_TG_ID": "1514716826", "PACKING_RESPONSIBLE_TG_ID": "8526226966"}[env]
        uid = (os.getenv(env, default) or "").strip()
        if uid:
            out.setdefault(uid, label)
    return out


def staff_breakdown(conn) -> dict:
    """{user_id: {name, username, why, groups}} — кто считается сотрудником и почему."""
    from services import ai_assistant

    members = {}
    for r in conn.execute(
        """
        SELECT user_id, MAX(display_name) AS display_name, MAX(username) AS username,
               COUNT(DISTINCT chat_id) AS groups
        FROM telegram_chat_members GROUP BY user_id
        """).fetchall():
        members[str(r["user_id"])] = {"name": r["display_name"] or r["username"] or str(r["user_id"]),
                                      "username": (r["username"] or "").lower(), "groups": int(r["groups"] or 0)}
    control = str(ai_assistant.control_group_id() or "").strip()
    in_control = {str(r["user_id"]) for r in conn.execute(
        "SELECT user_id FROM telegram_chat_members WHERE chat_id = ?", (control,)).fetchall()} if control else set()
    clients_ok = confirmed_clients()
    staff = {}

    def put(uid, why):
        uid = str(uid).strip()
        if not uid or uid in staff:
            return
        info = members.get(uid, {"name": uid, "username": "", "groups": 0})
        staff[uid] = {"name": info["name"], "username": info["username"], "groups": info["groups"], "why": why}

    for item in staff_registry():
        if item["id"]:
            put(item["id"], "в списке сотрудников")
        if item["username"]:
            for uid, info in members.items():
                if info["username"] == item["username"]:
                    put(uid, "в списке сотрудников")
    for uid, role in _role_ids().items():
        put(uid, role)
    for uid in in_control:
        put(uid, "участник Tracking gruppa")
    for uid, info in members.items():
        if info["username"].endswith("bot"):
            put(uid, "бот")
        elif info["groups"] >= STAFF_MIN_GROUPS and uid not in clients_ok:
            put(uid, f"есть в {info['groups']} группах")
    return staff


def staff_directory(conn) -> dict:
    """{user_id: имя} наших людей (см. staff_breakdown)."""
    return {uid: info["name"] for uid, info in staff_breakdown(conn).items()}


def staff_review(conn) -> dict:
    """Для проверки владельцем: кого бот считает сотрудником только по догадке
    (много групп), и похожих на сотрудников по подписи, но пока не учтённых."""
    staff = staff_breakdown(conn)
    by_guess = [{"user_id": uid, **info} for uid, info in staff.items() if info["why"].startswith("есть в ")]
    hinted = []
    for r in conn.execute(
        """
        SELECT user_id, MAX(display_name) AS display_name, MAX(username) AS username,
               COUNT(DISTINCT chat_id) AS groups
        FROM telegram_chat_members GROUP BY user_id
        """).fetchall():
        uid = str(r["user_id"])
        label = f"{r['display_name'] or ''} {r['username'] or ''}"
        if uid not in staff and uid not in confirmed_clients() and _STAFF_NAME_HINT.search(label):
            hinted.append({"user_id": uid, "name": r["display_name"] or r["username"] or uid,
                           "username": r["username"] or "", "groups": int(r["groups"] or 0)})
    by_guess.sort(key=lambda x: -x["groups"])
    hinted.sort(key=lambda x: -x["groups"])
    return {"counted_by_guess": by_guess, "looks_like_staff_not_counted": hinted}


def resolve_people(entries, conn=None) -> dict:
    """Строки владельца («7588682119», «@JAHONGIR_moderator», «Mirziyod») → люди,
    которых бот видел в группах. → {resolved, unresolved, ambiguous}."""
    own = conn is None
    conn = conn or db.get_conn()
    try:
        people = [dict(r) for r in conn.execute(
            """
            SELECT user_id, MAX(display_name) AS display_name, MAX(username) AS username,
                   COUNT(DISTINCT chat_id) AS groups
            FROM telegram_chat_members GROUP BY user_id
            """).fetchall()]
    finally:
        if own:
            conn.close()
    out = {"resolved": [], "unresolved": [], "ambiguous": []}
    for raw in entries or []:
        entry = str(raw or "").strip()
        if not entry:
            continue
        low = entry.lstrip("@").lower()
        if entry.isdigit():
            hit = [p for p in people if str(p["user_id"]) == entry]
            p = hit[0] if hit else {"user_id": entry, "display_name": "", "username": "", "groups": 0}
            out["resolved"].append({"id": entry, "name": p["display_name"] or p["username"] or entry,
                                    "username": (p["username"] or "").lower(), "groups": p["groups"], "seen": bool(hit)})
            continue
        hit = [p for p in people if (p["username"] or "").lower() == low]
        if not hit and not entry.startswith("@"):
            hit = [p for p in people if low in (p["display_name"] or "").lower()
                   or (len(low) >= 4 and low in (p["username"] or "").lower())]
        if len(hit) == 1:
            p = hit[0]
            out["resolved"].append({"id": str(p["user_id"]), "name": p["display_name"] or p["username"],
                                    "username": (p["username"] or "").lower(), "groups": p["groups"], "seen": True})
        elif hit:
            hit.sort(key=lambda p: -int(p["groups"] or 0))
            out["ambiguous"].append({"entry": entry, "candidates": [
                {"id": str(p["user_id"]), "name": p["display_name"], "username": p["username"],
                 "groups": p["groups"]} for p in hit[:8]]})
        elif re.fullmatch(r"@?[A-Za-z][A-Za-z0-9_]{4,31}", entry):
            # ник, которого бот ещё не встречал: запомним ником, id подставится сам
            out["resolved"].append({"id": "", "name": entry.lstrip("@"), "username": low, "groups": 0, "seen": False})
        else:
            out["unresolved"].append(entry)
    return out


def update_staff(add=(), remove=(), mark_client=(), unmark_client=()) -> dict:
    """Правка списка сотрудников и подтверждённых клиентов. → что сделано."""
    import json

    registry = staff_registry()
    clients_ok = confirmed_clients()
    report = {"added": [], "removed": [], "marked_client": [], "unmarked_client": [],
              "unresolved": [], "ambiguous": []}

    def key(item):
        return item["id"] or "@" + item["username"]

    if add:
        res = resolve_people(add)
        report["unresolved"] += res["unresolved"]
        report["ambiguous"] += res["ambiguous"]
        have = {key(x) for x in registry}
        for p in res["resolved"]:
            item = {"id": p["id"], "username": p["username"], "name": p["name"]}
            if key(item) not in have:
                registry.append(item)
                have.add(key(item))
                report["added"].append(item)
            clients_ok.discard(p["id"])
    if remove:
        wanted = {str(x).strip().lstrip("@").lower() for x in remove if str(x).strip()}
        keep = []
        for item in registry:
            if {item["id"], item["username"], item["name"].lower()} & wanted:
                report["removed"].append(item)
            else:
                keep.append(item)
        registry = keep
    if mark_client:
        res = resolve_people(mark_client)
        report["unresolved"] += res["unresolved"]
        report["ambiguous"] += res["ambiguous"]
        for p in res["resolved"]:
            if p["id"]:
                clients_ok.add(p["id"])
                registry = [x for x in registry if x["id"] != p["id"]]
                report["marked_client"].append(p)
    for raw in unmark_client or ():
        uid = str(raw).strip()
        if uid in clients_ok:
            clients_ok.discard(uid)
            report["unmarked_client"].append(uid)
    db.set_setting(STAFF_REGISTRY_KEY, json.dumps(registry, ensure_ascii=False))
    db.set_setting(CONFIRMED_CLIENTS_KEY, json.dumps(sorted(clients_ok)))
    report["registry_size"] = len(registry)
    return report


def _in_clause(ids) -> tuple[str, list]:
    ids = [str(i) for i in ids] or ["-"]
    return "(" + ",".join("?" for _ in ids) + ")", ids


def _median(values: list) -> int | None:
    values = sorted(values)
    if not values:
        return None
    mid = len(values) // 2
    return values[mid] if len(values) % 2 else round((values[mid - 1] + values[mid]) / 2)


def _classify(rec: dict, t: dict) -> tuple[str, str]:
    """Сегмент по ГРУЗУ и личному ритму клиента (владелец, 14.10.2026:
    «уснул — перестал давать нам груз»). Переписка в сегмент не входит."""
    d_cargo, d_msg = rec["days_since_cargo"], rec["days_since_client_message"]
    voice = "клиент никогда не писал" if d_msg is None else f"клиент писал {d_msg} дн. назад"
    if rec["shipments"] == 0:
        if (rec["group_age_days"] or 0) < t["new_group_days"]:
            return "new", (f"бот видит группу {rec['group_age_days']} дн., груза ни в базе, ни в шитсе по "
                           "кодам группы нет — похоже на нового клиента")
        return "no_cargo", f"ни одного груза ни в базе, ни в шитсе по кодам группы; {voice}"
    if rec["cargo_in_transit"]:
        return "active", f"груз сейчас в пути ({rec['cargo_in_transit']} BL)"
    lim = rhythm_limits(rec["usual_gap_days"], rec["shipments"], t)
    rec["limits"] = {k: lim[k] for k in ("fade", "asleep", "lost")}
    src = "" if rec["cargo_source"] == "база" else f" (по {rec['cargo_source']})"
    base = f"последний груз {d_cargo} дн. назад{src}, {lim['basis']}"
    if rec.get("moved_to") and d_cargo is not None and d_cargo > lim["fade"]:
        return "moved", f"{base}; его коды теперь едут через «{rec['moved_to']}» — это не сон"
    if d_cargo is None or d_cargo <= lim["fade"]:
        return "active", base
    if d_cargo <= lim["asleep"]:
        return "fading", f"{base} — затихает после {lim['fade']} дн.; {voice}"
    if d_cargo <= lim["lost"]:
        return "asleep", f"{base} — спит после {lim['asleep']} дн.; {voice}"
    return "lost", f"{base} — потерян после {lim['lost']} дн.; {voice}"


def history_chat_ids(chat_id) -> list:
    """Группа и все её старые id (до перехода в супергруппу)."""
    cid = str(chat_id)
    return [cid] + sorted(old for old, new in db.chat_migrations().items() if new == cid)


def _merge_migrated(migrations: dict, cargo, voice, seen, ours, sent, rating, problems, logged_last) -> None:
    def _sum(dst, src, keys):
        for k in keys:
            dst[k] = int(dst.get(k) or 0) + int(src.get(k) or 0)

    for old, new in migrations.items():
        if old in cargo:
            cargo.setdefault(new, []).extend(cargo.pop(old))
        if old in ours:
            ours.setdefault(new, []).extend(ours.pop(old))
        for agg, counters in ((voice, ("total", "n30", "prev30", "n90")), (seen, ("people",)),
                              (sent, ("n",)), (problems, ("total", "open_now"))):
            src = agg.pop(old, None)
            if not src:
                continue
            dst = agg.get(new)
            if not dst:
                agg[new] = src
                continue
            _sum(dst, src, counters)
            if "last_at" in src or "last_at" in dst:
                dst["last_at"] = max(str(dst.get("last_at") or ""), str(src.get("last_at") or "")) or None
        if old in rating:
            rating.setdefault(new, rating[old])
            rating.pop(old, None)
        if old in logged_last:
            logged_last[new] = max(logged_last.get(new, ""), logged_last.pop(old))


def analyze_all(today: date | None = None) -> dict:
    """Разбор ВСЕХ групп, где состоит бот. → {summary, groups:[…]}"""
    today = today or _today()
    t = thresholds()
    skip = _excluded_chat_ids()
    sheet_index, sheet_error = sheet_cargo_index()
    d30, d60, d90 = (today.toordinal() - n for n in (30, 60, 90))
    iso = lambda o: date.fromordinal(o).isoformat()
    conn = db.get_conn()
    try:
        shift = _members_clock_shift(conn)
        staff = staff_directory(conn)
        staff_sql, staff_args = _in_clause(staff)
        chats = [dict(r) for r in conn.execute(
            "SELECT chat_id, title, is_active, created_at, last_seen_at, sales_manager_tg_id, moderator_tg_id "
            "FROM telegram_chats WHERE chat_type IN ('group','supergroup')").fetchall()]
        # последнее сообщение в журнале групп (все авторы, с 15.10.2026; время Ташкента)
        logged_last = {str(r["chat_id"]): str(r["last_at"] or "") for r in conn.execute(
            "SELECT chat_id, MAX(sent_at) AS last_at FROM group_messages GROUP BY chat_id").fetchall()}
        cargo: dict = {}
        for r in conn.execute(
            """
            SELECT bl.chat_id, bl.code, bl.quantity_places, bl.weight_kg, bl.volume_cbm,
                   b.id AS batch_id, b.name, b.created_at, COALESCE(b.client_delivery_date,'') AS delivered
            FROM bl_codes bl JOIN batches b ON b.id = bl.batch_id
            WHERE TRIM(COALESCE(bl.chat_id,'')) != ''
            """).fetchall():
            cargo.setdefault(str(r["chat_id"]).strip(), []).append(dict(r))
        voice = {str(r["chat_id"]): dict(r) for r in conn.execute(
            f"""
            SELECT chat_id, COUNT(*) AS total, MAX(requested_at) AS last_at,
                   SUM(CASE WHEN requested_at >= ? THEN 1 ELSE 0 END) AS n30,
                   SUM(CASE WHEN requested_at >= ? AND requested_at < ? THEN 1 ELSE 0 END) AS prev30,
                   SUM(CASE WHEN requested_at >= ? THEN 1 ELSE 0 END) AS n90,
                   AVG(CASE WHEN response_seconds > 0 THEN response_seconds END) AS avg_resp
            FROM moderator_response_requests
            WHERE request_user_id NOT IN {staff_sql}
            GROUP BY chat_id
            """, (iso(d30), iso(d60), iso(d30), iso(d90), *staff_args)).fetchall()}
        # голос клиента — только его собственные сообщения: last_spoke_at (Ташкент).
        # last_seen_at клиентам не годится: его освежают список админов и чужие reply
        seen = {}
        for r in conn.execute(
                f"SELECT chat_id, MAX(NULLIF(last_spoke_at, '')) AS last_at, COUNT(*) AS people "
                f"FROM telegram_chat_members WHERE user_id NOT IN {staff_sql} GROUP BY chat_id",
                staff_args).fetchall():
            seen[str(r["chat_id"])] = dict(r)
        ours: dict = {}
        for r in conn.execute(
            f"SELECT chat_id, user_id, display_name, username, last_seen_at FROM telegram_chat_members "
            f"WHERE user_id IN {staff_sql}", staff_args).fetchall():
            item = dict(r)
            item["last_seen_at"] = _to_tashkent(item.get("last_seen_at"), shift)
            ours.setdefault(str(r["chat_id"]), []).append(item)
        sent = {str(r["chat_id"]): dict(r) for r in conn.execute(
            "SELECT chat_id, MAX(sent_at) AS last_at, COUNT(*) AS n FROM send_logs "
            "WHERE success = 1 GROUP BY chat_id").fetchall()}
        rating = {}
        for r in conn.execute(
            "SELECT chat_id, score, month_key FROM communication_ratings ORDER BY month_key").fetchall():
            rating[str(r["chat_id"])] = {"score": r["score"], "month": r["month_key"]}
        problems = {str(r["chat_id"]): dict(r) for r in conn.execute(
            """
            SELECT bl.chat_id, COUNT(*) AS total,
                   SUM(CASE WHEN p.status = 'open' THEN 1 ELSE 0 END) AS open_now
            FROM problems p JOIN bl_codes bl ON bl.id = p.bl_id
            WHERE TRIM(COALESCE(bl.chat_id,'')) != '' GROUP BY bl.chat_id
            """).fetchall()}
    finally:
        conn.close()

    # группа стала супергруппой: история старой записи — это история новой
    migrations = db.chat_migrations()
    _merge_migrated(migrations, cargo, voice, seen, ours, sent, rating, problems, logged_last)
    # где каждый код ехал недавно: клиент мог перейти в другую группу
    titles = {str(c["chat_id"]).strip(): c.get("title") or str(c["chat_id"]) for c in chats}
    recent_by_code: dict = {}
    window_start = today - timedelta(days=t["moved_window_days"])
    for cid_, rows_ in cargo.items():
        for r in rows_:
            day = _batch_day(r["name"], r["created_at"])
            if day and day >= window_start:
                recent_by_code.setdefault(str(r["code"]).strip().upper(), set()).add(cid_)

    groups, other = [], {"inactive": 0, "internal": 0, "merged": 0}
    for c in chats:
        cid = str(c["chat_id"]).strip()
        if cid in skip:
            continue
        if cid in migrations:          # старая запись переехавшей группы — учтена в новой
            other["merged"] += 1
            continue
        rows = cargo.get(cid, [])
        if not c.get("is_active"):
            other["inactive"] += 1
            continue
        if not rows and not db.is_client_group_title(c.get("title")):
            # группа без BL и не похожая на клиентскую — служебная
            other["internal"] += 1
            continue
        db_days = {d for d in (_batch_day(r["name"], r["created_at"]) for r in rows) if d}
        codes_used = {_norm_mark(r["code"]) for r in rows if str(r["code"]).strip()} | title_codes(c.get("title"))
        sheet_days = set()
        for code in codes_used:
            sheet_days |= sheet_index.get(code, set())
        days = _merge_days([d for d in (db_days | sheet_days) if d <= today + timedelta(days=30)])
        gaps = [(b - a).days for a, b in zip(days, days[1:]) if (b - a).days > 0]
        source = ("база+шитс" if db_days and sheet_days - db_days else "база" if db_days
                  else "шитс" if sheet_days else "")
        v, s = voice.get(cid, {}), seen.get(cid, {})
        last_voice = max([d for d in (_day(v.get("last_at")), _day(s.get("last_at"))) if d], default=None)
        team = sorted(ours.get(cid, []), key=lambda m: str(m.get("last_seen_at") or ""), reverse=True)
        # «последнее слово за клиентом»: он написал, и после этого никто из наших
        # в группе не появлялся. Поле status журнала для этого не годится — оно
        # почти никогда не закрывается (на проде «answered» у 111 строк из 29 559)
        voice_ts = max(str(v.get("last_at") or ""), str(s.get("last_at") or ""))
        team_ts = str(team[0].get("last_seen_at") or "") if team else ""
        waiting = bool(voice_ts) and voice_ts > team_ts
        sales = staff.get(str(c.get("sales_manager_tg_id") or "").strip(), "")
        if not sales:
            sales = next((m["display_name"] or m["username"] for m in team
                          if "sales" in f"{m.get('display_name')} {m.get('username')}".lower()), "")
        rec = {
            "chat_id": cid,
            "title": c.get("title") or cid,
            "bl_codes": sorted({str(r["code"]) for r in rows})[:8],
            "bl_codes_all": sorted({str(r["code"]).strip() for r in rows if str(r["code"]).strip()}),
            "batches": len({r["batch_id"] for r in rows}),
            "shipments": len(days),
            "cargo_source": source,
            "codes_used": sorted(codes_used),
            "first_cargo": days[0].isoformat() if days else "",
            "last_cargo": days[-1].isoformat() if days else "",
            "days_since_cargo": _ago(days[-1] if days else None, today),
            "cargo_in_transit": sum(1 for r in rows if not r["delivered"]),
            "total_places": int(sum(float(r["quantity_places"] or 0) for r in rows)),
            "total_kg": round(sum(float(r["weight_kg"] or 0) for r in rows), 1),
            "total_cbm": round(sum(float(r["volume_cbm"] or 0) for r in rows), 2),
            "usual_gap_days": _median(gaps),
            "last_client_message": last_voice.isoformat() if last_voice else "",
            "days_since_client_message": _ago(last_voice, today),
            "client_msgs_30d": int(v.get("n30") or 0),
            "client_msgs_prev_30d": int(v.get("prev30") or 0),
            "client_msgs_90d": int(v.get("n90") or 0),
            "client_msgs_total": int(v.get("total") or 0),
            "client_people": int(s.get("people") or 0),
            "client_spoke_last": waiting,
            "days_waiting_for_us": _ago(last_voice, today) if waiting else None,
            "avg_reply_minutes": round(float(v["avg_resp"]) / 60) if v.get("avg_resp") else None,
            "last_staff_seen": (team[0].get("last_seen_at") or "")[:10] if team else "",
            "sales_manager": sales,
            "last_tracking_sent": str((sent.get(cid) or {}).get("last_at") or "")[:10],
            "last_rating": rating.get(cid),
            "problems_total": int((problems.get(cid) or {}).get("total") or 0),
            "problems_open": int((problems.get(cid) or {}).get("open_now") or 0),
            "group_age_days": _ago(_day(c.get("created_at")), today),
        }
        moved = set()
        for code in rec["bl_codes_all"]:
            moved |= {x for x in recent_by_code.get(code.upper(), set()) if x != cid and x not in skip}
        rec["moved_to"] = ", ".join(sorted(titles.get(x, x) for x in moved))[:200]
        rec["segment"], rec["why"] = _classify(rec, t)
        # переписка — отдельный признак: пишет, но груза давно нет (тёплый — спросить, что мешает)
        rec["talking"] = (rec["segment"] in ("fading", "asleep", "lost")
                          and rec["days_since_client_message"] is not None
                          and rec["days_since_client_message"] <= t["active_msg_days"])
        rec["segment_label"] = SEGMENT_LABELS[rec["segment"]]
        # «резко замолчал»: месяц назад писал много, сейчас почти ноль
        rec["dropped"] = rec["client_msgs_prev_30d"] >= 6 and rec["client_msgs_30d"] <= rec["client_msgs_prev_30d"] // 3
        gap = rec["usual_gap_days"]
        rec["overdue_vs_usual"] = (
            round(rec["days_since_cargo"] / gap, 1)
            if gap and rec["days_since_cargo"] is not None and not rec["cargo_in_transit"] else None
        )
        # кого будить первым: ценность (сколько возил) × свежесть потери × наши долги
        rec["wake_priority"] = (
            min(rec["shipments"], 30) * 10 + min(rec["client_msgs_total"], 40)
            + (20 if rec["dropped"] else 0) + (15 if rec["client_spoke_last"] else 0)
            + (15 if rec["talking"] else 0)
            + (10 if rec["problems_open"] else 0)
            + (10 if (rec["last_rating"] or {}).get("score", 5) <= 3 else 0)
        )
        # тишина в группе: последнее событие в чате от ЛЮБОГО автора (клиент, сотрудник,
        # админ — telegram_chats.last_seen_at обновляется на каждое сообщение с момента,
        # как бот в группе), журнал сообщений и рассылки трекинга
        last_any = max(
            _to_tashkent(c.get("last_seen_at"), shift)[:19],
            str(logged_last.get(cid) or "")[:19],
            str((v or {}).get("last_at") or "")[:19],
            str((sent.get(cid) or {}).get("last_at") or "")[:19],
        )
        rec["last_activity"] = last_any[:16]
        rec["days_silent"] = _ago(_day(last_any), today)
        groups.append(rec)

    counts = {k: 0 for k in SEGMENT_LABELS}
    for g in groups:
        counts[g["segment"]] += 1
    return {
        "today": today.isoformat(),
        "thresholds": t,
        "summary": {
            "groups_bot_is_in": len(groups) + other["inactive"] + other["internal"],
            "client_groups_analyzed": len(groups),
            "internal_or_service_groups": other["internal"],
            "inactive_groups": other["inactive"],
            "by_segment": counts,
            "dropped_sharply": sum(1 for g in groups if g["dropped"]),
            "talking_no_cargo": sum(1 for g in groups if g["talking"]),
            "client_spoke_last": sum(1 for g in groups if g["client_spoke_last"]),
            "silent_30d": sum(1 for g in groups if (g["days_silent"] or 0) >= 30),
            "merged_old_group_records": other["merged"],
        },
        "groups": groups,
        "sheet_history": {"marks": len(sheet_index), "error": sheet_error},
    }


def resolve_chat(query: str) -> list:
    """Группа по chat_id, части названия или коду BL → [{chat_id, title}]."""
    q = str(query or "").strip()
    if not q:
        return []
    skip = _excluded_chat_ids()
    conn = db.get_conn()
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT c.chat_id, c.title FROM telegram_chats c
            LEFT JOIN bl_codes bl ON bl.chat_id = c.chat_id
            WHERE c.chat_type IN ('group','supergroup')
              AND (c.chat_id = ? OR LOWER(c.title) LIKE ? OR LOWER(bl.code) = ?)
            ORDER BY c.title LIMIT 12
            """, (q, f"%{q.lower()}%", q.lower())).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows if str(r["chat_id"]) not in skip]


def group_detail(chat_id: str, messages_limit: int = 12, today: date | None = None) -> dict:
    """Карточка одной группы: история грузов, ритм переписки, последние слова
    клиента и наши ответы — чтобы разбудить его предметно, а не шаблоном."""
    cid = str(chat_id or "").strip()
    if cid in _excluded_chat_ids():
        return {"error": "Эта группа закрыта для анализа"}
    today = today or _today()
    overview = next((g for g in analyze_all(today)["groups"] if g["chat_id"] == cid), None)
    if overview is None:
        return {"error": "Это не клиентская группа (или бот в ней больше не состоит)"}
    limit = max(1, min(int(messages_limit or 12), 40))
    conn = db.get_conn()
    try:
        shift = _members_clock_shift(conn)
        staff = staff_directory(conn)
        staff_sql, staff_args = _in_clause(staff)
        shipments = [dict(r) for r in conn.execute(
            """
            SELECT b.name AS batch, b.status, COALESCE(b.client_delivery_date,'') AS delivered,
                   GROUP_CONCAT(bl.code, ', ') AS codes, SUM(bl.quantity_places) AS places,
                   ROUND(SUM(bl.weight_kg), 1) AS kg, ROUND(SUM(bl.volume_cbm), 2) AS cbm,
                   b.created_at
            FROM bl_codes bl JOIN batches b ON b.id = bl.batch_id
            WHERE TRIM(bl.chat_id) = ? GROUP BY b.id ORDER BY b.id DESC LIMIT 30
            """, (cid,)).fetchall()]
        for s in shipments:
            day = _batch_day(s["batch"], s.pop("created_at"))
            s["cargo_date"] = day.isoformat() if day else ""
        monthly = [dict(r) for r in conn.execute(
            f"""
            SELECT SUBSTR(requested_at, 1, 7) AS month, COUNT(*) AS client_msgs
            FROM moderator_response_requests
            WHERE chat_id = ? AND request_user_id NOT IN {staff_sql}
            GROUP BY month ORDER BY month DESC LIMIT 8
            """, (cid, *staff_args)).fetchall()]
        messages = [dict(r) for r in conn.execute(
            f"""
            SELECT requested_at AS at, request_user_name AS who, SUBSTR(request_text, 1, 240) AS text,
                   responder_name AS answered_by, SUBSTR(response_text, 1, 160) AS answer,
                   response_seconds
            FROM moderator_response_requests
            WHERE chat_id = ? AND request_user_id NOT IN {staff_sql}
            ORDER BY requested_at DESC LIMIT ?
            """, (cid, *staff_args, limit)).fetchall()]
        for m in messages:
            secs = int(m.pop("response_seconds") or 0)
            m["answered_after_min"] = round(secs / 60) if secs else None
        people = [dict(r) for r in conn.execute(
            "SELECT user_id, display_name, username, first_seen_at, last_seen_at FROM telegram_chat_members "
            "WHERE chat_id = ? ORDER BY last_seen_at DESC LIMIT 40", (cid,)).fetchall()]
        ratings = [dict(r) for r in conn.execute(
            "SELECT month_key AS month, score, voter_name FROM communication_ratings "
            "WHERE chat_id = ? ORDER BY month_key DESC LIMIT 6", (cid,)).fetchall()]
        problems = [dict(r) for r in conn.execute(
            """
            SELECT p.problem_type AS type, p.status, SUBSTR(p.description, 1, 160) AS description,
                   p.created_at, bl.code, b.name AS batch
            FROM problems p JOIN bl_codes bl ON bl.id = p.bl_id JOIN batches b ON b.id = p.batch_id
            WHERE TRIM(bl.chat_id) = ? ORDER BY p.id DESC LIMIT 10
            """, (cid,)).fetchall()]
        lang = conn.execute(
            "SELECT message_language FROM bl_codes WHERE TRIM(chat_id) = ? ORDER BY id DESC LIMIT 1",
            (cid,)).fetchone()
    finally:
        conn.close()
    clients, team = [], []
    for p in people:
        item = {"name": p["display_name"] or p["username"] or p["user_id"], "username": p["username"],
                "last_seen": _to_tashkent(p["last_seen_at"], shift)[:10]}
        (team if str(p["user_id"]) in staff else clients).append(item)
    return {
        "overview": overview,
        "message_language": (lang["message_language"] if lang else "") or db.DEFAULT_MESSAGE_LANGUAGE,
        "shipments": shipments,
        "client_messages_by_month": monthly,
        "last_client_messages": messages,
        "client_people": clients[:10],
        "our_team_in_group": team[:10],
        "ratings": ratings,
        "problems": problems,
    }


# ── КЛИЕНТЫ В ГРУППАХ ─────────────────────────────────────────────────
def group_clients(query: str = "", today: date | None = None) -> dict:
    """Кто клиент в каждой клиентской группе: люди, которых бот видел (писали,
    им отвечали, админы), за вычетом сотрудников; имена клиентов из BL;
    сколько всего участников по Telegram и скольких бот не видел."""
    today = today or _today()
    analysis = analyze_all(today)
    by_chat = {g["chat_id"]: g for g in analysis["groups"]}
    if query:
        wanted = {r["chat_id"] for r in resolve_chat(query)}
        by_chat = {k: v for k, v in by_chat.items() if k in wanted}
    conn = db.get_conn()
    try:
        shift = _members_clock_shift(conn)
        staff = staff_breakdown(conn)
        ids_sql, ids_args = _in_clause(by_chat)
        members = [dict(r) for r in conn.execute(
            f"SELECT chat_id, user_id, display_name, username, is_admin, first_seen_at, last_seen_at, "
            f"COALESCE(last_spoke_at, '') AS last_spoke_at "
            f"FROM telegram_chat_members WHERE chat_id IN {ids_sql}", ids_args).fetchall()]
        msgs = {(str(r["chat_id"]), str(r["request_user_id"])): (int(r["n"]), str(r["last_at"] or ""))
                for r in conn.execute(
                    f"SELECT chat_id, request_user_id, COUNT(*) AS n, MAX(requested_at) AS last_at "
                    f"FROM moderator_response_requests WHERE chat_id IN {ids_sql} "
                    f"GROUP BY chat_id, request_user_id", ids_args).fetchall()}
        counts = {str(r["chat_id"]): (int(r["member_count"] or 0), str(r["members_checked_at"] or ""))
                  for r in conn.execute(
                      f"SELECT chat_id, member_count, members_checked_at FROM telegram_chats "
                      f"WHERE chat_id IN {ids_sql}", ids_args).fetchall()}
        names = {}
        for r in conn.execute(
            f"SELECT chat_id, client_name FROM bl_codes WHERE TRIM(COALESCE(client_name,'')) != '' "
            f"AND chat_id IN {ids_sql}", ids_args).fetchall():
            names.setdefault(str(r["chat_id"]).strip(), set()).add(str(r["client_name"]).strip())
    finally:
        conn.close()
    people_by_chat: dict = {}
    for m in members:
        people_by_chat.setdefault(str(m["chat_id"]), []).append(m)
    groups = []
    totals = {"groups": 0, "client_people": 0, "groups_without_known_client": 0, "unseen_people": 0}
    for cid, g in sorted(by_chat.items(), key=lambda kv: str(kv[1]["title"]).lower()):
        clients, team = [], []
        for m in people_by_chat.get(cid, []):
            uid = str(m["user_id"])
            n, last_msg = msgs.get((cid, uid), (0, ""))
            person = {"user_id": uid, "name": m["display_name"] or m["username"] or uid,
                      "username": m["username"] or "", "admin": bool(m["is_admin"]),
                      "last_seen": _to_tashkent(m["last_seen_at"], shift)[:10], "messages": n,
                      # когда сам писал: журнал вопросов + его собственные сообщения (в т.ч. админа)
                      "last_wrote": max(str(last_msg or ""), str(m["last_spoke_at"] or ""))[:10]}
            if uid in staff:
                team.append(person["name"])
            else:
                clients.append(person)
        clients.sort(key=lambda p: (-p["messages"], p["name"]))
        member_count, checked = counts.get(cid, (0, ""))
        known = len(clients) + len(team)
        rec = {
            "chat_id": cid, "title": g["title"], "bl_codes": g["bl_codes_all"],
            "client_names_in_bl": sorted(names.get(cid, set())),
            "segment": g["segment"], "segment_label": g["segment_label"], "last_cargo": g["last_cargo"],
            "batches": g["batches"], "shipments": g["shipments"], "cargo_source": g["cargo_source"],
            "sales_manager": g["sales_manager"],
            "clients": clients, "our_people": sorted(set(team)),
            "member_count": member_count or None, "members_checked_at": checked[:16],
            "unseen_people": max(0, member_count - known) if member_count else None,
        }
        groups.append(rec)
        totals["groups"] += 1
        totals["client_people"] += len(clients)
        totals["groups_without_known_client"] += 0 if clients else 1
        totals["unseen_people"] += rec["unseen_people"] or 0
    return {"today": today.isoformat(), "totals": totals, "groups": groups}


def clients_xlsx(data: dict) -> bytes:
    """Excel: лист «Клиенты» (человек на строку) и лист «Группы»."""
    import io
    from openpyxl import Workbook
    from openpyxl.styles import Font
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Klientlar"
    head = ["Guruh", "BL kodlar", "Mijoz (Telegram)", "@username", "User ID", "Admin", "Xabarlar",
            "Oxirgi ko'rilgan", "Holat", "Oxirgi yuk", "Sotuvchi"]
    ws.append(head)
    for g in data["groups"]:
        rows = g["clients"] or [None]
        for p in rows:
            ws.append([
                g["title"], ", ".join(g["bl_codes"]),
                p["name"] if p else "— (bot ko'rmagan)", ("@" + p["username"]) if p and p["username"] else "",
                p["user_id"] if p else "", "ha" if p and p["admin"] else "", p["messages"] if p else "",
                p["last_seen"] if p else "", g["segment_label"], g["last_cargo"], g["sales_manager"],
            ])
    ws2 = wb.create_sheet("Guruhlar")
    ws2.append(["Guruh", "BL kodlar", "BL dagi mijoz nomi", "Holat", "Jo'natmalar", "Oxirgi yuk",
                "Mijozlar (bot ko'rgan)", "A'zolar (Telegram)", "Bot ko'rmagan", "Bizning xodimlar", "Sotuvchi"])
    for g in data["groups"]:
        ws2.append([g["title"], ", ".join(g["bl_codes"]), ", ".join(g["client_names_in_bl"]), g["segment_label"],
                    g["shipments"], g["last_cargo"], len(g["clients"]), g["member_count"] or "",
                    "" if g["unseen_people"] is None else g["unseen_people"], ", ".join(g["our_people"]),
                    g["sales_manager"]])
    for sheet in (ws, ws2):
        for cell in sheet[1]:
            cell.font = Font(bold=True)
        sheet.freeze_panes = "A2"
        for i, col in enumerate(sheet.columns, 1):
            width = max(len(str(c.value or "")) for c in col)
            sheet.column_dimensions[get_column_letter(i)].width = min(max(10, width + 2), 48)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── МОНИТОРИНГ: снимки и сводка изменений ─────────────────────────────
_ASLEEP_SET = {"asleep", "lost"}


def take_snapshot(today: date | None = None) -> int:
    today = today or _today()
    groups = analyze_all(today)["groups"]
    return db.save_activity_snapshot(today.isoformat(), [
        {"chat_id": g["chat_id"], "segment": g["segment"], "days_since_cargo": g["days_since_cargo"],
         "batches": g["shipments"]} for g in groups])


def changes(days: int = 7, today: date | None = None) -> dict:
    """Что изменилось за N дней: сравнение текущего разбора со снимком N дней назад."""
    today = today or _today()
    days = max(1, min(int(days or 7), 90))
    now = analyze_all(today)
    prev_day, prev = db.get_activity_snapshot((today - timedelta(days=days)).isoformat())
    out = {"today": today.isoformat(), "compared_with": prev_day, "days": days,
           "fell_asleep": [], "started_fading": [], "woke_up": [], "lost": [], "moved": [], "new_cargo_groups": [],
           "talking": [{"title": g["title"], "wake_priority": g["wake_priority"]}
                       for g in sorted(now["groups"], key=lambda g: -g["wake_priority"]) if g.get("talking")]}
    if not prev_day:
        out["note"] = ("Снимков за этот срок ещё нет: бот сохраняет сегменты каждый день, сравнение "
                       "появится, когда накопится история.")
        out["summary"] = now["summary"]
        return out
    for g in now["groups"]:
        before = (prev.get(g["chat_id"]) or {}).get("segment")
        item = {"title": g["title"], "bl_codes": g["bl_codes"], "was": before, "now": g["segment"],
                "why": g["why"], "shipments": g["shipments"], "wake_priority": g["wake_priority"],
                "days_since_cargo": g["days_since_cargo"], "usual_gap_days": g["usual_gap_days"],
                "moved_to": g.get("moved_to") or ""}
        if before is None:
            if g["shipments"]:
                out["new_cargo_groups"].append(item)
            continue
        if before == g["segment"]:
            continue
        if g["segment"] == "lost":
            out["lost"].append(item)
        elif g["segment"] == "asleep" and before not in _ASLEEP_SET:
            out["fell_asleep"].append(item)
        elif g["segment"] == "fading" and before == "active":
            out["started_fading"].append(item)
        elif g["segment"] == "active" and before in ("fading", "asleep", "lost", "moved"):
            out["woke_up"].append(item)
        elif g["segment"] == "moved":
            out["moved"].append(item)
    for key in ("fell_asleep", "started_fading", "woke_up", "lost", "moved", "new_cargo_groups"):
        out[key].sort(key=lambda x: -x["wake_priority"])
    out["summary"] = now["summary"]
    return out


def digest_text(days: int = 7, today: date | None = None) -> str:
    """Короткая сводка для Telegram (HTML)."""
    from html import escape

    today = today or _today()
    ch = changes(days, today)
    seg = ch["summary"]["by_segment"]
    lines = [f"📊 <b>Faollik — mijoz guruhlari</b> · {today.strftime('%d.%m')}",
             f"Hozir: 🟢 {seg.get('active', 0)} · 🟡 {seg.get('fading', 0)} · 🔴 {seg.get('asleep', 0)} · "
             f"⚫ {seg.get('lost', 0)} · 🔀 {seg.get('moved', 0)}"]
    if not ch["compared_with"]:
        lines.append("Taqqoslash uchun tarix hali yig'ilmoqda — keyingi hisobotda o'zgarishlar ko'rinadi.")
    else:
        lines.append(f"{ch['days']} kunda: uxlab qoldi {len(ch['fell_asleep'])} · susaydi {len(ch['started_fading'])} · "
                     f"uyg'ondi {len(ch['woke_up'])} · yo'qotildi {len(ch['lost'])}")

        def reason(it):
            # обрезаем ДО экранирования: срез «&amp;» → «&am» Telegram отвергает целиком
            if it.get("moved_to"):
                return "endi " + escape(str(it["moved_to"])[:60]) + " orqali"
            d, gap = it.get("days_since_cargo"), it.get("usual_gap_days")
            if d is None:
                return "yuk ko'rinmaydi"
            return f"oxirgi yuk {d} kun oldin" + (f", odatda har {gap} kunda" if gap else "")

        def block(icon, title, items):
            if not items:
                return
            lines.append(f"\n{icon} <b>{title}</b>")
            for it in items[:8]:
                lines.append(f"• {escape(str(it['title'])[:60])} — {reason(it)}")
            if len(items) > 8:
                lines.append(f"…yana {len(items) - 8}")
        block("🔴", "Uxlab qoldi", ch["fell_asleep"])
        block("🟡", "Susaymoqda", ch["started_fading"])
        block("🟢", "Uyg'ondi", ch["woke_up"])
        block("⚫", "Yo'qotildi", ch["lost"])
        block("🔀", "Boshqa guruh orqali yuk beryapti", ch["moved"])
    try:  # разборы переписки (instructor): недовольные за период; сбой не ломает сводку
        from services import client_insights_service as cis

        lines += cis.digest_lines(ch.get("days") or days, today)
    except Exception:
        pass
    talking = ch.get("talking") or []
    if talking:
        lines.append(f"\n💬 <b>Yozadi, lekin yuk bermayapti</b> ({len(talking)}): "
                     + ", ".join(escape(str(g["title"])[:40]) for g in talking[:6]))
    return "\n".join(lines)
