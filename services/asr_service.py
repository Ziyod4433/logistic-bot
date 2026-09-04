"""Распознавание голосовых сообщений (узбекский + русский).

Бэкенд переключается переменной ASR_PROVIDER:

  openai (по умолчанию) — OpenAI transcription API; ключ OPENAI_API_KEY
      уже стоит на проде; узбекский и русский распознаются, язык
      определяется сам. Модель: ASR_MODEL (default gpt-4o-mini-transcribe,
      можно whisper-1).
  gemini — Google Gemini Audio: аудио уходит inline в generateContent
      с промптом «расшифруй дословно». Нужен GEMINI_API_KEY
      (aistudio.google.com, бесплатный тариф есть). Модель: ASR_MODEL
      (default gemini-2.5-flash). Лимит запроса 20 МБ — голосовые
      Телеграма всегда меньше.
  custom — любой OpenAI-совместимый endpoint. Ровно так vLLM отдаёт
      Qwen3-ASR-1.7B (+ узбекский файн-тюн, напр.
      Gearnode/qwen3-asr-uzbek-v2): `vllm serve <model>` на GPU-сервере →
      ASR_API_BASE_URL=http://host:8000/v1, ASR_API_KEY=...,
      ASR_MODEL=<имя модели>.
"""
import base64
import os

import requests as req

REQUEST_TIMEOUT = 120

_GEMINI_PROMPT = (
    "Transcribe this voice message verbatim. The speech is in Uzbek or Russian "
    "(possibly mixed). Return ONLY the transcribed text in the original language, "
    "no translation, no comments, no quotes."
)


def provider() -> str:
    p = (os.getenv("ASR_PROVIDER") or "").strip().lower()
    return p if p in ("openai", "gemini", "custom") else "openai"


def _cfg() -> tuple[str, str, str]:
    """(base_url, api_key, model) выбранного бэкенда."""
    if provider() == "custom":
        return (
            (os.getenv("ASR_API_BASE_URL") or "").rstrip("/"),
            (os.getenv("ASR_API_KEY") or "").strip(),
            (os.getenv("ASR_MODEL") or "Qwen/Qwen3-ASR-1.7B").strip(),
        )
    return (
        "https://api.openai.com/v1",
        (os.getenv("OPENAI_API_KEY") or "").strip(),
        (os.getenv("ASR_MODEL") or "gpt-4o-mini-transcribe").strip(),
    )


def _gemini_key() -> str:
    return (os.getenv("GEMINI_API_KEY") or "").strip()


def _gemini_model() -> str:
    return (os.getenv("ASR_MODEL") or "gemini-2.5-flash").strip()


def available() -> bool:
    if provider() == "gemini":
        return bool(_gemini_key())
    base, key, _model = _cfg()
    return bool(base and key)


def _mime_for(filename: str) -> str:
    low = (filename or "").lower()
    if low.endswith(".mp3"):
        return "audio/mp3"
    if low.endswith(".wav"):
        return "audio/wav"
    if low.endswith((".m4a", ".aac")):
        return "audio/aac"
    return "audio/ogg"        # Telegram voice = ogg/opus


def _transcribe_gemini(audio_bytes: bytes, filename: str) -> tuple[bool, str]:
    key = _gemini_key()
    if not key:
        return False, "ASR не настроен: нужен GEMINI_API_KEY"
    model = _gemini_model()
    payload = {
        "contents": [{
            "parts": [
                {"inline_data": {
                    "mime_type": _mime_for(filename),
                    "data": base64.b64encode(audio_bytes).decode("ascii"),
                }},
                {"text": _GEMINI_PROMPT},
            ],
        }],
    }
    try:
        resp = req.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            headers={"x-goog-api-key": key, "Content-Type": "application/json"},
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
    except req.RequestException as exc:
        return False, f"сеть/таймаут: {exc}"
    if resp.status_code >= 400:
        return False, f"HTTP {resp.status_code}: {str(resp.text or '')[:200]}"
    try:
        cands = (resp.json() or {}).get("candidates") or []
        parts = ((cands[0].get("content") or {}).get("parts") or []) if cands else []
        text = " ".join(str(p.get("text") or "") for p in parts).strip()
    except (ValueError, AttributeError, IndexError):
        return False, "не-JSON/пустой ответ Gemini"
    if not text:
        return False, "распознан пустой текст"
    return True, text


def transcribe(audio_bytes: bytes, filename: str = "voice.ogg") -> tuple[bool, str]:
    """(ok, текст | причина). Язык не подсказываем жёстко — в компании
    звучат и узбекский, и русский, автоопределение справляется лучше."""
    if provider() == "gemini":
        return _transcribe_gemini(audio_bytes, filename)
    base, key, model = _cfg()
    if not (base and key):
        return False, "ASR не настроен: нужен OPENAI_API_KEY или ASR_API_BASE_URL + ASR_API_KEY"
    try:
        resp = req.post(
            f"{base}/audio/transcriptions",
            headers={"Authorization": f"Bearer {key}"},
            data={"model": model},
            files={"file": (filename, audio_bytes, "audio/ogg")},
            timeout=REQUEST_TIMEOUT,
        )
    except req.RequestException as exc:
        return False, f"сеть/таймаут: {exc}"
    if resp.status_code >= 400:
        return False, f"HTTP {resp.status_code}: {str(resp.text or '')[:200]}"
    try:
        text = str((resp.json() or {}).get("text") or "").strip()
    except ValueError:
        return False, "не-JSON ответ от ASR"
    if not text:
        return False, "распознан пустой текст"
    return True, text
