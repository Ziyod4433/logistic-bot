"""Активность клиентских Telegram-групп: кто «заснул» и кого пора будить.

Сигналов два, и решают они ВМЕСТЕ: ГРУЗ (когда клиент последний раз давал нам
груз — дата партии) и ГОЛОС (когда сам клиент, а не наш сотрудник, писал в
группе). Молчание между отгрузками само по себе не сон: клиент, который возит
раз в квартал, молчит законно — поэтому считаем и его обычный ритм отгрузок.

Источники: bl_codes+batches (груз), moderator_response_requests (каждое
сообщение клиента и ответ на него), telegram_chat_members (кто и когда был
в группе), send_logs (рассылки), communication_ratings (оценки), problems.
"""
from __future__ import annotations

import os
import re
from datetime import date, datetime

import database as db

_DATE_RE = re.compile(r"(?<!\d)(\d{2})\.(\d{2})\.(\d{4})(?!\d)")
# наш сотрудник пишет во многих группах, клиент живёт в одной-двух своих
STAFF_MIN_GROUPS = 3

SEGMENT_LABELS = {
    "active": "🟢 активен",
    "fading": "🟡 затихает",
    "asleep": "🔴 заснул",
    "new": "⚪ новая группа, груза ещё не было",
    "no_cargo": "⚫ груз в системе не виден (BL не привязан или так и не отгрузил)",
}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "") or default)
    except ValueError:
        return default


def thresholds() -> dict:
    return {
        "active_cargo_days": _env_int("ACTIVITY_ACTIVE_CARGO_DAYS", 45),
        "active_msg_days": _env_int("ACTIVITY_ACTIVE_MSG_DAYS", 21),
        "asleep_cargo_days": _env_int("ACTIVITY_ASLEEP_CARGO_DAYS", 90),
        "asleep_msg_days": _env_int("ACTIVITY_ASLEEP_MSG_DAYS", 60),
        "new_group_days": _env_int("ACTIVITY_NEW_GROUP_DAYS", 45),
    }


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
    return (today - day).days if day else None


def _excluded_chat_ids() -> set:
    from services import ai_assistant

    out = set(ai_assistant.confidential_chat_ids())
    control = str(ai_assistant.control_group_id() or "").strip()
    if control:
        out.add(control)
    return out


def staff_directory(conn) -> dict:
    """{user_id: имя} наших людей: админы/операторы + все, кто виден в ≥3 группах."""
    from services import ai_assistant

    rows = conn.execute(
        """
        SELECT user_id, MAX(display_name) AS display_name, MAX(username) AS username
        FROM telegram_chat_members
        GROUP BY user_id HAVING COUNT(DISTINCT chat_id) >= ?
        """,
        (STAFF_MIN_GROUPS,),
    ).fetchall()
    staff = {str(r["user_id"]): (r["display_name"] or r["username"] or str(r["user_id"])) for r in rows}
    for uid in set(ai_assistant.admin_ids()) | set(ai_assistant.operator_ids()):
        staff.setdefault(str(uid), str(uid))
    return staff


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
    d_cargo, d_msg = rec["days_since_cargo"], rec["days_since_client_message"]
    if rec["batches"] == 0:
        if (rec["group_age_days"] or 0) < t["new_group_days"]:
            return "new", "группа моложе полутора месяцев, груза ещё не было"
        voice = "клиент никогда не писал" if d_msg is None else f"клиент писал {d_msg} дн. назад"
        return "no_cargo", f"ни одного груза в системе; {voice}"
    if rec["cargo_in_transit"]:
        return "active", f"груз сейчас в пути ({rec['cargo_in_transit']} BL)"
    if d_cargo is not None and d_cargo <= t["active_cargo_days"]:
        return "active", f"последний груз {d_cargo} дн. назад"
    if d_msg is not None and d_msg <= t["active_msg_days"]:
        return "active", f"клиент писал {d_msg} дн. назад"
    silent = "клиент ни разу не писал" if d_msg is None else f"клиент молчит {d_msg} дн."
    if (d_cargo is None or d_cargo > t["asleep_cargo_days"]) and (d_msg is None or d_msg > t["asleep_msg_days"]):
        return "asleep", f"груза нет {d_cargo} дн., {silent}"
    return "fading", f"груз {d_cargo} дн. назад, {silent}"


def analyze_all(today: date | None = None) -> dict:
    """Разбор ВСЕХ групп, где состоит бот. → {summary, groups:[…]}"""
    today = today or _today()
    t = thresholds()
    skip = _excluded_chat_ids()
    d30, d60, d90 = (today.toordinal() - n for n in (30, 60, 90))
    iso = lambda o: date.fromordinal(o).isoformat()
    conn = db.get_conn()
    try:
        staff = staff_directory(conn)
        staff_sql, staff_args = _in_clause(staff)
        chats = [dict(r) for r in conn.execute(
            "SELECT chat_id, title, is_active, created_at, sales_manager_tg_id, moderator_tg_id "
            "FROM telegram_chats WHERE chat_type IN ('group','supergroup')").fetchall()]
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
                   SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS unanswered,
                   AVG(CASE WHEN response_seconds > 0 THEN response_seconds END) AS avg_resp
            FROM moderator_response_requests
            WHERE request_user_id NOT IN {staff_sql}
            GROUP BY chat_id
            """, (iso(d30), iso(d60), iso(d30), iso(d90), *staff_args)).fetchall()}
        seen = {str(r["chat_id"]): dict(r) for r in conn.execute(
            f"SELECT chat_id, MAX(last_seen_at) AS last_at, COUNT(*) AS people FROM telegram_chat_members "
            f"WHERE user_id NOT IN {staff_sql} GROUP BY chat_id", staff_args).fetchall()}
        ours: dict = {}
        for r in conn.execute(
            f"SELECT chat_id, user_id, display_name, username, last_seen_at FROM telegram_chat_members "
            f"WHERE user_id IN {staff_sql}", staff_args).fetchall():
            ours.setdefault(str(r["chat_id"]), []).append(dict(r))
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

    groups, other = [], {"inactive": 0, "internal": 0}
    for c in chats:
        cid = str(c["chat_id"]).strip()
        if cid in skip:
            continue
        rows = cargo.get(cid, [])
        if not c.get("is_active"):
            other["inactive"] += 1
            continue
        if not rows and not db.is_client_group_title(c.get("title")):
            other["internal"] += 1
            continue
        days = sorted({d for d in (_batch_day(r["name"], r["created_at"]) for r in rows) if d})
        gaps = [(b - a).days for a, b in zip(days, days[1:]) if (b - a).days > 0]
        v, s = voice.get(cid, {}), seen.get(cid, {})
        last_voice = max([d for d in (_day(v.get("last_at")), _day(s.get("last_at"))) if d], default=None)
        team = sorted(ours.get(cid, []), key=lambda m: str(m.get("last_seen_at") or ""), reverse=True)
        sales = staff.get(str(c.get("sales_manager_tg_id") or "").strip(), "")
        if not sales:
            sales = next((m["display_name"] or m["username"] for m in team
                          if "sales" in f"{m.get('display_name')} {m.get('username')}".lower()), "")
        rec = {
            "chat_id": cid,
            "title": c.get("title") or cid,
            "bl_codes": sorted({str(r["code"]) for r in rows})[:8],
            "batches": len({r["batch_id"] for r in rows}),
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
            "unanswered_client_msgs": int(v.get("unanswered") or 0),
            "avg_reply_minutes": round(float(v["avg_resp"]) / 60) if v.get("avg_resp") else None,
            "last_staff_seen": (team[0].get("last_seen_at") or "")[:10] if team else "",
            "sales_manager": sales,
            "last_tracking_sent": str((sent.get(cid) or {}).get("last_at") or "")[:10],
            "last_rating": rating.get(cid),
            "problems_total": int((problems.get(cid) or {}).get("total") or 0),
            "problems_open": int((problems.get(cid) or {}).get("open_now") or 0),
            "group_age_days": _ago(_day(c.get("created_at")), today),
        }
        rec["segment"], rec["why"] = _classify(rec, t)
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
            rec["batches"] * 10 + min(rec["client_msgs_total"], 40)
            + (20 if rec["dropped"] else 0) + (15 if rec["unanswered_client_msgs"] else 0)
            + (10 if rec["problems_open"] else 0)
            + (10 if (rec["last_rating"] or {}).get("score", 5) <= 3 else 0)
        )
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
            "with_unanswered_client_msgs": sum(1 for g in groups if g["unanswered_client_msgs"]),
        },
        "groups": groups,
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
            SELECT SUBSTR(requested_at, 1, 7) AS month, COUNT(*) AS client_msgs,
                   SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS unanswered
            FROM moderator_response_requests
            WHERE chat_id = ? AND request_user_id NOT IN {staff_sql}
            GROUP BY month ORDER BY month DESC LIMIT 8
            """, (cid, *staff_args)).fetchall()]
        messages = [dict(r) for r in conn.execute(
            f"""
            SELECT requested_at AS at, request_user_name AS who, SUBSTR(request_text, 1, 240) AS text,
                   status, responder_name AS answered_by, SUBSTR(response_text, 1, 160) AS answer,
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
                "last_seen": str(p["last_seen_at"] or "")[:10]}
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
