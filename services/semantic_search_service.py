"""ПОИСК ПО СМЫСЛУ В ИСТОРИИ ПЕРЕПИСКИ КЛИЕНТСКИХ ГРУПП (владелец, 15.10.2026).

Задача: находить старые обсуждения задержек, цены, повреждений и т.п., даже
если клиент писал другими словами или на другом языке (узбекский латиницей и
кириллицей, русский).

Каждое сообщение превращается в «смысловой отпечаток» (эмбеддинг) и хранится в
ОТДЕЛЬНОМ файле SQLite рядом с основной базой (основная база и её ночная копия
не растут; файл пересобирается из сообщений в любой момент). Поиск — sqlite-vec
(vec_distance_cosine прямо в SQL); если расширение не загрузилось — тот же
расчёт на Python.

Поставщик эмбеддингов — SEMANTIC_PROVIDER: gemini (gemini-embedding-001: на
узбекско-русской выборке нашёл 14 из 14) или openai (text-embedding-3-large:
7 из 14). Пусто — модуль выключен. Только чтение основной базы; трекинг,
партии и отправки не затрагиваются; клиентам ничего не пишет.
"""
from __future__ import annotations

import math
import os
import re
import sqlite3
import struct
import threading
import time

import requests

import database as db

GEMINI_MODEL = "gemini-embedding-001"
OPENAI_MODEL = "text-embedding-3-large"
BATCH = 100
_PLACEHOLDER_RE = re.compile(r"^\[(sticker|photo|voice|video|audio|video_note|animation|location|contact|poll)\]$",
                             re.IGNORECASE)
_LETTERS_RE = re.compile(r"[^\W\d_]", re.UNICODE)
TOPICS = {
    "DELAY": "задержка доставки, груз долго идёт | yuk kechikdi, hali kelmadi, qachon keladi",
    "PRICE": "дорого, цена высокая, другие дешевле | narx qimmat, boshqa kompaniya arzon",
    "DAMAGE": "груз повреждён, сломан, разбит, мятая коробка | yuk shikastlangan, singan, ezilgan",
    "LOSS": "груз потерялся, не хватает мест | yuk yo'qoldi, joy kam keldi, yetishmayapti",
    "SERVICE": "не отвечают, плохо обслуживают, недоволен | javob yo'q, javob bermayapsizlar, norozi",
}


def provider() -> str:
    value = (os.getenv("SEMANTIC_PROVIDER") or "").strip().lower()
    return value if value in ("gemini", "openai") else ""


def dims() -> int:
    try:
        return max(64, min(int(os.getenv("SEMANTIC_DIMS") or 256), 3072))
    except ValueError:
        return 256


def enabled() -> bool:
    if not provider():
        return False
    return bool(_key())


def _key() -> str:
    # отдельный платный ключ Gemini для поиска (владелец, 15.10.2026); распознавание речи — на своём
    if provider() == "gemini":
        value = os.getenv("SEMANTIC_GEMINI_API_KEY") or os.getenv("GEMINI_API_KEY")
    else:
        value = os.getenv("OPENAI_API_KEY")
    return (value or "").strip()


def model_name() -> str:
    return GEMINI_MODEL if provider() == "gemini" else OPENAI_MODEL


# ── ВЕКТОРНЫЙ ФАЙЛ ──────────────────────────────────────────────────────
def vector_db_path() -> str:
    explicit = (os.getenv("VECTOR_DB_PATH") or "").strip()
    if explicit:
        return explicit
    base = os.path.dirname(os.path.abspath(db.DB_PATH)) or "."
    return os.path.join(base, "message_vectors.db")


_VEC_STATE = {"loaded": None}


def _vconn() -> sqlite3.Connection:
    conn = sqlite3.connect(vector_db_path(), timeout=30)
    conn.row_factory = sqlite3.Row
    if _VEC_STATE["loaded"] is not False:
        try:
            import sqlite_vec

            conn.enable_load_extension(True)
            sqlite_vec.load(conn)
            conn.enable_load_extension(False)
            _VEC_STATE["loaded"] = True
        except Exception:
            _VEC_STATE["loaded"] = False     # нет расширения — поиск на Python
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS message_vectors (
            msg_key TEXT PRIMARY KEY,
            chat_id TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            user_id TEXT NOT NULL DEFAULT '',
            user_name TEXT NOT NULL DEFAULT '',
            text TEXT NOT NULL,
            model TEXT NOT NULL,
            dims INTEGER NOT NULL,
            embedding BLOB NOT NULL
        )
        """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mv_chat_time ON message_vectors(chat_id, sent_at)")
    return conn


def engine() -> str:
    if _VEC_STATE["loaded"] is None:
        _vconn().close()
    return "sqlite-vec" if _VEC_STATE["loaded"] else "python"


def _pack(vec: list) -> bytes:
    return struct.pack(f"{len(vec)}f", *vec)


def _unpack(blob: bytes) -> list:
    return list(struct.unpack(f"{len(blob) // 4}f", blob))


def _normalize(vec: list) -> list:
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [x / norm for x in vec]


# ── ЭМБЕДДИНГИ ──────────────────────────────────────────────────────────
class EmbeddingError(RuntimeError):
    pass


def _embed_remote(texts: list, for_query: bool) -> list:
    """Отдельная функция — точка подмены в тестах. → [вектор] в том же порядке."""
    if provider() == "gemini":
        # ключ — в заголовке, не в адресе: адреса попадают в логи
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:batchEmbedContents"
        task = "RETRIEVAL_QUERY" if for_query else "RETRIEVAL_DOCUMENT"
        body = {"requests": [{"model": f"models/{GEMINI_MODEL}", "content": {"parts": [{"text": t}]},
                              "taskType": task, "outputDimensionality": dims()} for t in texts]}
        resp = requests.post(url, json=body, headers={"x-goog-api-key": _key()}, timeout=60)
        if resp.status_code == 429:
            raise EmbeddingError("лимит запросов Gemini (429)")
        resp.raise_for_status()
        return [e["values"] for e in resp.json()["embeddings"]]
    resp = requests.post("https://api.openai.com/v1/embeddings",
                         headers={"Authorization": f"Bearer {_key()}"},
                         json={"model": OPENAI_MODEL, "input": texts, "dimensions": dims()}, timeout=60)
    if resp.status_code == 429:
        raise EmbeddingError("лимит запросов OpenAI (429)")
    resp.raise_for_status()
    return [x["embedding"] for x in resp.json()["data"]]


def embed(texts: list, for_query: bool = False) -> list:
    out = []
    for i in range(0, len(texts), BATCH):
        part = texts[i:i + BATCH]
        for attempt in range(3):
            try:
                out.extend(_normalize(v) for v in _embed_remote(part, for_query))
                break
            except EmbeddingError:
                if attempt == 2:
                    raise
                time.sleep(20 * (attempt + 1))
    return out


# ── ЧТО ИНДЕКСИРУЕМ ─────────────────────────────────────────────────────
def _worth(text: str) -> bool:
    t = str(text or "").strip()
    if not t or _PLACEHOLDER_RE.match(t):
        return False
    return len(_LETTERS_RE.findall(t)) >= 3


def _excluded() -> set:
    from services import group_activity_service as gas

    return gas._excluded_chat_ids()


def pending_messages(known: set, limit: int | None = None) -> list:
    """Сообщения групп без отпечатка: журнал group_messages + журнал вопросов и ответы
    reply'ем. Один и тот же Telegram-message из двух журналов — один ключ."""
    skip = _excluded()
    conn = db.get_conn()
    try:
        rows = []
        for r in conn.execute(
                "SELECT chat_id, message_id, user_id, user_name, text, sent_at FROM group_messages "
                "ORDER BY sent_at DESC").fetchall():
            rows.append((f"{r['chat_id']}:{r['message_id']}", r["chat_id"], r["sent_at"], r["user_id"],
                         r["user_name"], r["text"]))
        for r in conn.execute(
                "SELECT id, chat_id, request_message_id, request_user_id, request_user_name, request_text, "
                "requested_at, responder_user_id, responder_name, response_text, responded_at "
                "FROM moderator_response_requests ORDER BY requested_at DESC").fetchall():
            rows.append((f"{r['chat_id']}:{r['request_message_id']}", r["chat_id"], r["requested_at"],
                         r["request_user_id"], r["request_user_name"], r["request_text"]))
            if str(r["response_text"] or "").strip():
                rows.append((f"resp:{r['id']}", r["chat_id"], r["responded_at"], r["responder_user_id"],
                             r["responder_name"], r["response_text"]))
    finally:
        conn.close()
    out, seen = [], set(known)
    for key, chat_id, sent_at, uid, name, text in rows:
        if key in seen or str(chat_id) in skip or not _worth(text) or not str(sent_at or "").strip():
            continue
        seen.add(key)
        out.append({"key": key, "chat_id": str(chat_id), "sent_at": str(sent_at)[:19], "user_id": str(uid or ""),
                    "user_name": str(name or "")[:80], "text": str(text).strip()[:1500]})
        if limit and len(out) >= limit:
            break
    return out


_INDEX_LOCK = threading.Lock()
_INDEX_STATE: dict = {"running": False}


def index_pending(limit: int | None = None) -> dict:
    """Проиндексировать новые сообщения (по умолчанию — все). → {indexed, pending_left, error}."""
    if not enabled():
        return {"indexed": 0, "error": "поиск по смыслу выключен (SEMANTIC_PROVIDER)"}
    conn = _vconn()
    try:
        # отпечатки другой модели или размера пересчитываются (INSERT OR REPLACE ниже)
        known = {r[0] for r in conn.execute("SELECT msg_key FROM message_vectors WHERE model = ? AND dims = ?",
                                            (model_name(), dims())).fetchall()}
        todo = pending_messages(known, limit)
        done, error = 0, ""
        for i in range(0, len(todo), BATCH):
            part = todo[i:i + BATCH]
            try:
                vecs = embed([m["text"] for m in part])
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"[:200]
                break
            conn.executemany(
                "INSERT OR REPLACE INTO message_vectors(msg_key, chat_id, sent_at, user_id, user_name, text, model, "
                "dims, embedding) VALUES (?,?,?,?,?,?,?,?,?)",
                [(m["key"], m["chat_id"], m["sent_at"], m["user_id"], m["user_name"], m["text"], model_name(),
                  len(v), _pack(v)) for m, v in zip(part, vecs)])
            conn.commit()
            done += len(part)
        return {"indexed": done, "pending_left": len(todo) - done, "error": error}
    finally:
        conn.close()


def start_indexing() -> dict:
    """Полная индексация в фоне (первый запуск — вся история)."""
    if not enabled():
        return {"ok": False, "error": "поиск по смыслу выключен (SEMANTIC_PROVIDER)"}
    if not _INDEX_LOCK.acquire(blocking=False):
        return {"ok": True, "running": True, **_INDEX_STATE}

    def _run():
        try:
            _INDEX_STATE.update(running=True, started_at=time.strftime("%Y-%m-%d %H:%M"))
            _INDEX_STATE.update(index_pending())
        except Exception as exc:
            _INDEX_STATE["error"] = f"{type(exc).__name__}: {exc}"[:200]
        finally:
            _INDEX_STATE["running"] = False
            _INDEX_LOCK.release()

    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "started": True}


def stats() -> dict:
    if not enabled():
        return {"enabled": False, "provider": provider() or "—"}
    conn = _vconn()
    try:
        row = conn.execute("SELECT COUNT(*) AS n, MIN(sent_at) AS first, MAX(sent_at) AS last FROM message_vectors").fetchone()
        return {"enabled": True, "provider": provider(), "model": model_name(), "engine": engine(),
                "indexed": row["n"], "first": row["first"], "last": row["last"], "indexing": dict(_INDEX_STATE)}
    finally:
        conn.close()


# ── ПОИСК ───────────────────────────────────────────────────────────────
_QUERY_CACHE: dict = {}


def _query_vectors(queries: list) -> list:
    # ключ — вместе с поставщиком, моделью и размером: смена настроек не подсунет старый вектор
    tag = (provider(), model_name(), dims())
    missing = [q for q in queries if (tag, q) not in _QUERY_CACHE]
    if missing:
        for q, v in zip(missing, embed(missing, for_query=True)):
            if len(_QUERY_CACHE) > 500:
                _QUERY_CACHE.pop(next(iter(_QUERY_CACHE)))
            _QUERY_CACHE[(tag, q)] = v
    return [_QUERY_CACHE[(tag, q)] for q in queries]


def search(query, chat_ids: list | None = None, since: str = "", until: str = "", limit: int = 15,
           min_score: float = 0.0, refresh: bool = True) -> list:
    """Похожие по смыслу сообщения. query — строка (фразы через « | ») или список фраз;
    счёт сообщения — лучшее совпадение с любой фразой. → [{key, chat_id, sent_at, user_id,
    user_name, text, score}] по убыванию."""
    if not enabled():
        return []
    if refresh:
        try:
            index_pending(limit=300)          # свежие сообщения — до поиска
        except Exception:
            pass
    phrases = query if isinstance(query, list) else [p.strip() for p in str(query or "").split("|")]
    phrases = [p for p in phrases if p][:6]
    if not phrases:
        return []
    qvecs = _query_vectors(phrases)
    where, args = ["dims = ?", "model = ?"], [dims(), model_name()]
    if chat_ids:
        where.append(f"chat_id IN ({','.join('?' for _ in chat_ids)})")
        args += [str(c) for c in chat_ids]
    if since:
        where.append("sent_at >= ?")
        args.append(since)
    if until:
        where.append("sent_at < ?")
        args.append(until)
    sql_where = " AND ".join(where)
    best: dict = {}
    conn = _vconn()
    try:
        for qv in qvecs:
            if _VEC_STATE["loaded"]:
                rows = conn.execute(
                    f"SELECT msg_key, chat_id, sent_at, user_id, user_name, text, "
                    f"1 - vec_distance_cosine(embedding, ?) AS score FROM message_vectors WHERE {sql_where} "
                    f"ORDER BY score DESC LIMIT ?", [_pack(qv), *args, max(limit * 3, 30)]).fetchall()
                found = [dict(r) for r in rows]
            else:
                found = []
                for r in conn.execute(
                        f"SELECT msg_key, chat_id, sent_at, user_id, user_name, text, embedding FROM message_vectors "
                        f"WHERE {sql_where}", args):
                    vec = _unpack(r["embedding"])
                    item = {k: r[k] for k in ("msg_key", "chat_id", "sent_at", "user_id", "user_name", "text")}
                    item["score"] = sum(a * b for a, b in zip(qv, vec))
                    found.append(item)
            for item in found:
                if item["score"] < min_score:
                    continue
                prev = best.get(item["msg_key"])
                if prev is None or item["score"] > prev["score"]:
                    best[item["msg_key"]] = item
    finally:
        conn.close()
    hits = sorted(best.values(), key=lambda x: -x["score"])[:limit]
    for h in hits:
        h["key"] = h.pop("msg_key")
        h["score"] = round(float(h["score"]), 3)
    return hits


def topic_hits(chat_ids: list, until: str, per_topic: int = 3, min_score: float = 0.55) -> list:
    """Для разбора переписки: старые сообщения группы (до окна разбора) по главным темам
    жалоб. → [{topic, ...hit}]; без поставщика — пусто."""
    if not enabled() or not chat_ids:
        return []
    out, seen = [], set()
    for topic, query in TOPICS.items():
        try:
            hits = search(query, chat_ids=chat_ids, until=until, limit=per_topic, min_score=min_score,
                          refresh=False)
        except Exception:
            return out
        for h in hits:
            if h["key"] not in seen:
                seen.add(h["key"])
                out.append(dict(h, topic=topic))
    return out
