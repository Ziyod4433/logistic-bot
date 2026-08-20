# -*- coding: utf-8 -*-
"""PNG-карточка трекинга (стиль «окна» из утверждённого дизайна).

Отправляется как фото с подписью-реквизитами; packing list файлы идут
следом отдельными сообщениями (внизу). Рисуется Pillow + DejaVu (кир/лат),
эмодзи не используются — маршрут, галочки и грузовик рисуются фигурами.
"""

import io
import os

from PIL import Image, ImageDraw, ImageFont

_FONT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static", "fonts")

# палитра сайта
BG = (10, 12, 20)
PANEL = (17, 22, 38)
BORDER = (34, 211, 238)          # cyan рамка
BORDER_SOFT = (35, 43, 66)
TEXT = (232, 234, 242)
MUTED = (125, 134, 168)
CYAN = (34, 211, 238)
GREEN = (16, 185, 129)
AMBER = (245, 158, 11)
GRAY_NODE = (58, 66, 88)

W, H = 1000, 560


def _font(size: int, bold: bool = False):
    name = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
    return ImageFont.truetype(os.path.join(_FONT_DIR, name), size)


def _center_text(draw, y, text, font, fill):
    w = draw.textlength(text, font=font)
    draw.text(((W - w) / 2, y), text, font=font, fill=fill)


def _fit_font(draw, text, max_width, size, bold=True, min_size=18):
    while size > min_size:
        f = _font(size, bold)
        if draw.textlength(text, font=f) <= max_width:
            return f
    # noqa: недостижимо при нормальных данных
        size -= 2
    return _font(min_size, bold)


def render_card(view: dict) -> bytes:
    strings = view["strings"]
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)

    # рамка как у «окна»
    d.rounded_rectangle([8, 8, W - 8, H - 8], radius=26, outline=BORDER, width=3)
    d.rounded_rectangle([14, 14, W - 14, H - 14], radius=22, outline=BORDER_SOFT, width=1)

    # шапка
    d.text((44, 34), "BURAQ LOGISTICS", font=_font(21, True), fill=MUTED)
    right_label = "TREKING"
    rw = d.textlength(right_label, font=_font(21, True))
    d.text((W - 44 - rw, 34), right_label, font=_font(21, True), fill=MUTED)

    # заголовок (без эмодзи из строк)
    title = strings["holat"].replace("📍", "").strip()
    _center_text(d, 74, title, _font(40, True), TEXT)
    tw = d.textlength(title, font=_font(40, True))
    d.line([(W - tw) / 2, 128, (W + tw) / 2, 128], fill=(212, 175, 55), width=3)

    # главный блок: ETA или «прибыл»
    if view["arrived"]:
        badge = f"{view['place']} — {strings['arrived_note'].upper()}"
        f = _fit_font(d, badge, W - 200, 42)
        bw = d.textlength(badge, font=f)
        pad = 26
        d.rounded_rectangle(
            [(W - bw) / 2 - pad, 156, (W + bw) / 2 + pad, 232],
            radius=18, fill=(11, 44, 33), outline=GREEN, width=2,
        )
        d.text(((W - bw) / 2, 176), badge, font=f, fill=GREEN)
    else:
        _center_text(d, 156, view["eta_label"], _font(24), MUTED)
        eta = view["eta_value"]
        f = _fit_font(d, eta, W - 160, 52)
        _center_text(d, 190, eta, f, CYAN)

    # ── линия маршрута ──
    route_y = 322
    x0, x1 = 120, W - 120
    nodes = 6
    step = (x1 - x0) / (nodes - 1)
    progress = 1.0 if view["arrived"] else max(0.0, min(1.0, float(view["progress"])))
    pos = nodes - 1 if view["arrived"] else min(nodes - 1, max(0, round(progress * (nodes - 1))))

    for i in range(nodes - 1):
        seg_color = GREEN if i < pos else GRAY_NODE
        d.line([x0 + step * i + 20, route_y, x0 + step * (i + 1) - 20, route_y], fill=seg_color, width=5)

    check_font = _font(22, True)
    for i in range(nodes):
        cx = x0 + step * i
        if view["arrived"]:
            passed, current = True, (i == nodes - 1)
        else:
            passed, current = i < pos, i == pos
        if current and not view["arrived"]:
            # текущая точка — «грузовик»: янтарный круг со стрелкой
            d.ellipse([cx - 22, route_y - 22, cx + 22, route_y + 22], fill=AMBER)
            d.polygon([(cx - 7, route_y - 10), (cx + 11, route_y), (cx - 7, route_y + 10)], fill=BG)
        elif passed:
            d.ellipse([cx - 18, route_y - 18, cx + 18, route_y + 18], fill=GREEN)
            w = d.textlength("✓", font=check_font)
            d.text((cx - w / 2, route_y - 15), "✓", font=check_font, fill=(255, 255, 255))
        else:
            d.ellipse([cx - 16, route_y - 16, cx + 16, route_y + 16], outline=GRAY_NODE, width=4, fill=BG)

    # подписи концов маршрута
    d.text((x0 - d.textlength(view["endpoint_left"], font=_font(20, True)) / 2, route_y + 34),
           view["endpoint_left"], font=_font(20, True), fill=MUTED)
    endr_w = d.textlength(view["endpoint_right"], font=_font(20, True))
    d.text((x1 - endr_w / 2, route_y + 34), view["endpoint_right"], font=_font(20, True), fill=MUTED)

    # текущее место
    now_line = f"{strings['now']}: {view['place']}"
    f = _fit_font(d, now_line, W - 160, 30)
    _center_text(d, route_y + 74, now_line, f, AMBER if not view["arrived"] else GREEN)

    # нижняя панель реквизитов
    d.rounded_rectangle([44, 468, W - 44, 528], radius=16, fill=PANEL, outline=BORDER_SOFT, width=1)
    req = view["requisites_plain"]
    f = _fit_font(d, req, W - 140, 26)
    rw = d.textlength(req, font=f)
    d.text(((W - rw) / 2, 486), req, font=f, fill=TEXT)

    out = io.BytesIO()
    img.save(out, format="PNG")
    return out.getvalue()
