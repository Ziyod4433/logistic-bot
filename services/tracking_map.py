"""Живая карта трекинга: координаты точек маршрута по статусу BL.

Позиция фуры показывается по ГОРОДУ текущего статуса (не GPS с машины):
бот отправляет в клиентскую группу нативную live-геолокацию Telegram и
передвигает маркер, когда логисты подтверждают новый статус.
"""

# Статус → (широта, долгота). Позиции — города/погранпереходы маршрута.
_STATUS_COORDS = {
    # Китай
    "Xitoy": (29.3068, 120.0757),        # по умолчанию — Yiwu
    "Yiwu": (29.3068, 120.0757),
    "Zhongshan": (22.5176, 113.3928),
    # Казахская ветка
    "Horgos (Qozoq)": (44.2016, 80.4113),
    "Nurjo'li": (44.1900, 80.3600),      # МЦПС «Нур жолы»
    "Jarkent": (44.1626, 80.0106),
    "Almata": (43.2380, 76.9457),
    "Taraz": (42.8994, 71.3667),
    "Shimkent": (42.3417, 69.5901),
    "Qonusbay": (41.9000, 69.2500),
    "Saryagash": (41.4614, 69.1656),
    "Yallama": (40.9600, 68.6800),
    # Кыргызская ветка
    "Kashgar (Qirg'iz)": (39.4704, 75.9898),
    "Irkeshtam": (39.6853, 73.9126),
    "Osh": (40.5140, 72.8161),
    "Dostlik": (40.7300, 72.1750),
    "Andijon": (40.7821, 72.3442),
    # Финал
    "Toshkent(Chuqursoy ULS da)": (41.3260, 69.2286),
}

_TASHKENT = _STATUS_COORDS["Toshkent(Chuqursoy ULS da)"]

# Статусы, на которых маршрут завершён: маркер встаёт в Ташкент и live
# останавливается (дальше двигать нечего).
_FINAL_STATUSES = {
    "Toshkent(Chuqursoy ULS da)",
    "Mijozga yetkazib berildi",
    "Доставлен",
    "Р”РѕСЃС‚Р°РІР»РµРЅ",  # легаси-строка со сломанной кодировкой в старых записях
}


def position_for_status(status: str):
    """(lat, lon, final) для статуса; None — если точка неизвестна."""
    normalized = (status or "").strip() or "Xitoy"
    if normalized in _FINAL_STATUSES:
        lat, lon = _TASHKENT
        return lat, lon, True
    coords = _STATUS_COORDS.get(normalized)
    if not coords:
        return None
    return coords[0], coords[1], False
