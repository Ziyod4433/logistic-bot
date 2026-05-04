import json
import os
import re

import requests as req


VALID_INTENTS = {
    "check_cargo_status",
    "ask_for_bl",
    "complaint",
    "general_question",
    "unknown",
}
VALID_LANGUAGES = {"uz_latin", "uz_cyrillic", "ru", "mixed"}
GREETING_MARKERS = (
    "salom",
    "assalomu",
    "assalom",
    "privet",
    "hello",
    "hi",
    "здравствуйте",
    "привет",
    "салом",
)
STATUS_MARKERS = (
    "yukim",
    "yuk",
    "holat",
    "status",
    "qayerda",
    "qachon",
    "keladi",
    "kelarkan",
    "груз",
    "статус",
    "где",
    "когда",
    "доставка",
)
COMPLAINT_MARKERS = (
    "muammo",
    "shikast",
    "yo'qol",
    "yaroqsiz",
    "jalob",
    "жалоб",
    "проблем",
    "повреж",
    "потер",
)


def _get_openai_api_key() -> str:
    return (os.getenv("OPENAI_API_KEY") or "").strip()


def _get_openai_model() -> str:
    return (os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()


def get_runtime_status() -> dict:
    api_key = _get_openai_api_key()
    return {
        "openai_api_key_present": bool(api_key),
        "openai_model": _get_openai_model(),
    }


def _detect_language(text: str) -> str:
    raw = str(text or "")
    if not raw.strip():
        return "mixed"

    lowered = raw.lower()
    has_cyrillic = bool(re.search(r"[А-Яа-яЁёҚқҒғҲҳЎўЪъЬь]", raw))
    has_latin = bool(re.search(r"[A-Za-z]", raw))
    russian_markers = (
        "груз",
        "статус",
        "где",
        "когда",
        "доставка",
        "пожалуйста",
    )
    if any(marker in lowered for marker in russian_markers):
        return "ru"
    if has_cyrillic and has_latin:
        return "mixed"
    if has_cyrillic:
        return "uz_cyrillic"
    return "uz_latin"


def _extract_bl_code(text: str) -> str | None:
    raw = str(text or "").upper()
    patterns = [
        r"\bBL[-\s]?\d{1,6}\b",
        r"\b[A-Z]{1,6}-\d{1,6}\b",
        r"\b[A-Z0-9-]{3,20}\b",
    ]
    for pattern in patterns:
        for match in re.findall(pattern, raw):
            candidate = re.sub(r"\s+", "", match).strip("- ")
            if len(candidate) >= 3 and re.search(r"\d", candidate):
                return candidate
    return None


def _fallback_reply(intent: str, language: str, bl_code: str | None = None) -> str:
    if language == "ru":
        if intent in {"check_cargo_status", "ask_for_bl", "unknown"}:
            return "Пожалуйста, отправьте BL-код, чтобы я мог проверить статус груза."
        if intent == "complaint":
            return "Спасибо, мы зафиксировали ваше обращение. Пожалуйста, по возможности отправьте BL-код."
        return "Спасибо за сообщение. Пожалуйста, уточните вопрос или отправьте BL-код."
    if language == "uz_cyrillic":
        if intent in {"check_cargo_status", "ask_for_bl", "unknown"}:
            return "Илтимос, юк ҳолатини текшириш учун BL кодингизни юборинг."
        if intent == "complaint":
            return "Мурожаатингизни қабул қилдик. Имкон бўлса, BL кодингизни ҳам юборинг."
        return "Раҳмат. Илтимос, саволингизни аниқроқ ёзинг ёки BL кодингизни юборинг."
    if intent in {"check_cargo_status", "ask_for_bl", "unknown"}:
        return "Iltimos, yuk holatini tekshirish uchun BL kodingizni yuboring."
    if intent == "complaint":
        return "Murojaatingiz qabul qilindi. Imkon bo'lsa, BL kodingizni ham yuboring."
    return "Rahmat. Iltimos, savolingizni aniqroq yozing yoki BL kodingizni yuboring."


def _general_greeting_reply(language: str) -> str:
    if language == "ru":
        return "Здравствуйте! Чтобы я помог со статусом груза, отправьте, пожалуйста, ваш BL-код."
    if language == "uz_cyrillic":
        return "Ассалому алайкум! Юк ҳолатини текширишим учун, илтимос, BL кодингизни юборинг."
    return "Assalomu alaykum! Yuk holatini tekshirishim uchun, iltimos, BL kodingizni yuboring."


def _heuristic_result(text: str) -> dict:
    message_text = str(text or "").strip()
    language = _detect_language(message_text)
    bl_code = _extract_bl_code(message_text)
    lowered = message_text.lower()

    if bl_code:
        return {
            "intent": "check_cargo_status",
            "bl_code": bl_code,
            "language": language,
            "confidence": 0.96,
            "reply": _fallback_reply("check_cargo_status", language, bl_code),
        }

    if any(marker in lowered for marker in COMPLAINT_MARKERS):
        return {
            "intent": "complaint",
            "bl_code": None,
            "language": language,
            "confidence": 0.8,
            "reply": _fallback_reply("complaint", language, None),
        }

    if any(marker in lowered for marker in STATUS_MARKERS):
        return {
            "intent": "ask_for_bl",
            "bl_code": None,
            "language": language,
            "confidence": 0.82,
            "reply": _fallback_reply("ask_for_bl", language, None),
        }

    if any(marker in lowered for marker in GREETING_MARKERS):
        return {
            "intent": "general_question",
            "bl_code": None,
            "language": language,
            "confidence": 0.78,
            "reply": _general_greeting_reply(language),
        }

    return {
        "intent": "unknown",
        "bl_code": None,
        "language": language,
        "confidence": 0.35,
        "reply": _fallback_reply("unknown", language, None),
    }


def _normalize_result(result: dict, source_text: str) -> dict:
    language = str(result.get("language") or _detect_language(source_text)).strip().lower()
    if language not in VALID_LANGUAGES:
        language = _detect_language(source_text)

    intent = str(result.get("intent") or "unknown").strip()
    if intent not in VALID_INTENTS:
        intent = "unknown"

    raw_bl = result.get("bl_code")
    bl_code = str(raw_bl).strip().upper() if raw_bl not in (None, "", "null") else None
    if bl_code == "NONE":
        bl_code = None
    if not bl_code:
        bl_code = _extract_bl_code(source_text)

    try:
        confidence = float(result.get("confidence", 0))
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    reply = str(result.get("reply") or "").strip()
    if not reply:
        reply = _fallback_reply(intent, language, bl_code)

    return {
        "intent": intent,
        "bl_code": bl_code,
        "language": language,
        "confidence": confidence,
        "reply": reply,
    }


def analyze_message(text: str) -> dict:
    message_text = str(text or "").strip()
    fallback_language = _detect_language(message_text)
    fallback_bl = _extract_bl_code(message_text)
    heuristic = _heuristic_result(message_text)

    if heuristic.get("intent") in {"check_cargo_status", "ask_for_bl", "complaint", "general_question"}:
        return heuristic

    api_key = _get_openai_api_key()
    if not api_key:
        return heuristic

    system_prompt = (
        "You are a Telegram logistics intent classifier for Buraq Logistics. "
        "Return only valid JSON with exactly these keys: "
        "intent, bl_code, language, confidence, reply. "
        "intent must be one of: check_cargo_status, ask_for_bl, complaint, general_question, unknown. "
        "bl_code must be a string BL code if clearly present, otherwise null. "
        "language must be one of: uz_latin, uz_cyrillic, ru, mixed. "
        "confidence must be a number between 0 and 1. "
        "reply must be a short polite customer-facing reply in the same language as the user. "
        "If the user is asking about cargo status and no BL code is clearly present, use intent ask_for_bl. "
        "Do not invent cargo status. Do not add markdown fences."
    )

    payload = {
        "model": _get_openai_model(),
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": message_text},
        ],
    }
    try:
        response = req.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            data=json.dumps(payload),
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
    except req.HTTPError:
        return heuristic
    except req.RequestException:
        return heuristic
    content = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "{}")
    try:
        parsed = json.loads(content)
    except Exception:
        parsed = {}

    result = _normalize_result(parsed, message_text)
    if result["intent"] == "unknown" and fallback_bl:
        result["intent"] = "check_cargo_status"
        result["bl_code"] = fallback_bl
        result["reply"] = _fallback_reply("check_cargo_status", fallback_language, fallback_bl)
        result["confidence"] = max(result["confidence"], 0.45)
    return result


def handle_group_message(message: dict) -> dict:
    return analyze_message((message or {}).get("text") or "")
