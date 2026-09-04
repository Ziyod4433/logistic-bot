"""Распознавание голосовых сообщений (узбекский + русский).

Бэкенд переключается переменной ASR_PROVIDER — протокол один и тот же
(OpenAI-совместимый POST /audio/transcriptions), поэтому смена модели —
это только адрес/ключ/имя:

  openai (по умолчанию) — OpenAI transcription API; ключ OPENAI_API_KEY
      уже стоит на проде; узбекский и русский распознаются, язык
      определяется сам. Модель: ASR_MODEL (default gpt-4o-mini-transcribe,
      можно whisper-1).
  custom — любой OpenAI-совместимый endpoint. Ровно так vLLM отдаёт
      Qwen3-ASR-1.7B (+ узбекский файн-тюн, напр.
      Gearnode/qwen3-asr-uzbek-v2): `vllm serve <model>` на GPU-сервере →
      ASR_API_BASE_URL=http://host:8000/v1, ASR_API_KEY=...,
      ASR_MODEL=<имя модели>. Включение = три переменные Railway.
"""
import os

import requests as req

REQUEST_TIMEOUT = 120


def provider() -> str:
    p = (os.getenv("ASR_PROVIDER") or "").strip().lower()
    return p if p in ("openai", "custom") else "openai"


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


def available() -> bool:
    base, key, _model = _cfg()
    return bool(base and key)


def transcribe(audio_bytes: bytes, filename: str = "voice.ogg") -> tuple[bool, str]:
    """(ok, текст | причина). Язык не подсказываем — в компании звучат и
    узбекский, и русский, автоопределение справляется лучше жёсткой метки."""
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
