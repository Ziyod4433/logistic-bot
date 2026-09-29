# -*- coding: utf-8 -*-
"""Loading-plan spreadsheet (партии / фуры).

The batches in the panel are OPENED and ADJUSTED from this Google
Sheets file. Monthly tabs (e.g. "AVGUST 2026", "Iyul2026") hold plan
blocks laid out in column groups:

    <date of departure>          ← e.g. 14.08.2026
    <plan title>                 ← e.g. "YIWU TO HORGOS - YARGXOL"
    SHIPPING MARK | CTN | T/CBM | KG | KG IN 1 CBM | DATE OF ARRIVE
    BL-309        | 2   | 1.53  | ...
    ...
    TOTAL         | ... |

Plan kinds (by title):
  - CHINA truck  ("YIWU TO HORGOS…", "ZHONGSHAN TO HORGOS…", legacy
    "YIWU MUHAMMAD" / "ZHONGSHAN YARGXOL"): loading from a China
    warehouse toward Horgos. A batch is opened from this plan.
  - KAZAKH truck ("HORGOS TO TASHKENT YIWU + ZH …"): loading at Horgos
    toward Tashkent; usually merges the BLs of the YIWU and ZHONGSHAN
    China trucks. When a batch reaches "Horgos" its BL list is
    re-synced from this plan. The kazakh block sometimes lacks the
    SHIPPING MARK header row, so parsing anchors on date+title instead.
"""

import io
import logging
import os
import re
import threading
import time
import warnings
from datetime import datetime, date

import requests

LOADING_PLAN_SHEET_ID = (
    os.environ.get("LOADING_PLAN_SHEET_ID", "").strip()
    or "10xia-LGEIwi6zufRvfxP3vzwxEix3sf7sRP71M6hjPw"
)

CACHE_TTL_SECONDS = 120
_lock = threading.Lock()
_cache: dict = {}
log = logging.getLogger(__name__)
# с какой ширины листа писать в лог — чтобы рост вкладки был заметен
_WIDTH_WARN_COLUMNS = 80

_TITLE_KEYWORDS = ("YARGXOL", "YARGOL", "MUHAMMAD", "YIWU", "ZHONGSHAN", "HORGOS", "FURA")

# месячные вкладки: разные написания месяца по-узбекски/русски
_MONTH_TOKENS = {
    1: ("yanvar", "январ"), 2: ("fevral", "феврал"), 3: ("mart", "март"),
    4: ("aprel", "апрел"), 5: ("may", "май"), 6: ("iyun", "июн"),
    7: ("iyul", "июл"), 8: ("avgust", "август"), 9: ("sentabr", "сентяб"),
    10: ("oktabr", "октяб"), 11: ("noyabr", "нояб"), 12: ("dekabr", "декаб"),
}


def _download_workbook():
    from openpyxl import load_workbook

    url = f"https://docs.google.com/spreadsheets/d/{LOADING_PLAN_SHEET_ID}/export?format=xlsx"
    response = requests.get(url, timeout=90)
    response.raise_for_status()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return load_workbook(io.BytesIO(response.content), read_only=True, data_only=True)


def list_tabs(force: bool = False) -> list:
    data = _get_parsed(force=force)
    return sorted(data.keys())


def default_tab(tabs: list, today: date | None = None) -> str:
    today = today or datetime.now().date()
    tokens = _MONTH_TOKENS[today.month]
    year = str(today.year)
    best = ""
    for tab in tabs:
        low = tab.lower().replace(" ", "")
        if any(t in low for t in tokens) and year in low:
            best = tab
    return best or (tabs[-1] if tabs else "")


_DATE_STR_RE = re.compile(r"^\s*\d{1,2}[.\-/]\d{1,2}[.\-/]\d{4}")


def _is_block_date(value) -> bool:
    """Ячейка-дата блока: настоящая дата ИЛИ строка вида «14.08.2026»,
    «14.08.2026-2» (так логисты подписывают ВТОРУЮ фуру того же дня)."""
    if isinstance(value, (datetime, date)):
        return True
    return isinstance(value, str) and bool(_DATE_STR_RE.match(value))


def _is_title(value) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip().upper()
    if not text or len(text) < 4:
        return False
    # отбрасываем текстовые даты вида "10-Aug" из колонок DATE OF ARRIVE
    if re.match(r"^\d{1,2}[-./]", text):
        return False
    return any(k in text for k in _TITLE_KEYWORDS)


def _fmt_date(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y")
    if isinstance(value, date):
        return value.strftime("%d.%m.%Y")
    return str(value or "").strip()


def _num(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace("\xa0", "").replace(",", ".").strip() or 0)
    except ValueError:
        return 0.0


# Под планом логисты ведут отдельную таблицу «horgos skladda qoladigan yuklar»
# («HORGOSDA QOLADIGAN YUKLAR» в старых вкладках): груз, который приехал в
# Хоргос, но на эту фуру НЕ погружен. Это не состав партии.
_STAYS_RE = re.compile(r"horgos\w*\s+(sklad\w*\s+)?qol", re.IGNORECASE)
# на сколько строк ниже конца блока искать эту таблицу
_STAYS_SCAN_ROWS = 15


def _is_stays_heading(value) -> bool:
    return isinstance(value, str) and bool(_STAYS_RE.search(value))


def _is_table_header(value) -> bool:
    """Шапка таблицы груза: «SHIPPING MARK | CTN | T/CBM | KG …»."""
    return isinstance(value, str) and value.strip().upper().startswith("SHIPPING MARK")


def _is_kazakh_title(title) -> bool:
    """Казахская фура: «HORGOS TO TASHKENT…», «HORGOS - TASHKENT…», «HORGOS
    YARGXOL» — всё, что начинается с HORGOS или содержит HORGOS не в виде
    «TO HORGOS» (китайская: «YIWU TO HORGOS…»)."""
    compact = re.sub(r"\s+", " ", str(title or "").upper()).strip()
    return compact.startswith("HORGOS") or ("HORGOS" in compact and "TO HORGOS" not in compact)


def _parse_stays(grid: list, j: int, after: int) -> list:
    """Строки таблицы «остаётся на складе Хоргоса» под блоком в колонке j.
    Ищем заголовок ниже строки `after`; новый блок в этой колонке — стоп.
    Промежуточную таблицу (неподписанный казахский план, копию с ценами)
    проходим насквозь: остатки всех блоков колонки всё равно общие."""
    n_rows = len(grid)

    def cell(r, c=j):
        return grid[r][c] if r < n_rows and c < len(grid[r]) else None

    head = None
    for r in range(after, min(n_rows, after + _STAYS_SCAN_ROWS)):
        v = cell(r)
        if _is_stays_heading(v):
            head = r
            break
        if _is_block_date(v) and _is_title(cell(r + 1)):
            return []
    if head is None:
        return []
    k = head + 1
    first = cell(k)
    if isinstance(first, str) and first.strip().upper().startswith("SHIPPING MARK"):
        k += 1
    out = []
    while k < n_rows:
        v = cell(k)
        mark = str(v).strip() if v is not None else ""
        if not mark or mark.upper() == "TOTAL" or _is_block_date(v) or _is_stays_heading(v):
            break
        out.append({
            "mark": mark,
            "ctn": _num(cell(k, j + 1)),
            "cbm": _num(cell(k, j + 2)),
            "kg": _num(cell(k, j + 3)),
            # строка заголовка таблицы: одну и ту же таблицу могут увидеть
            # два блока колонки (китайский и казахский) — считать её раз
            "table_row": head,
        })
        k += 1
    return out


def _read_rows(grid: list, j: int, start: int) -> tuple:
    """Строки груза таблицы в колонке j, начиная со строки start.
    → (items, строка, с которой продолжать поиск ниже таблицы).

    Стоп: TOTAL, три пустые строки подряд, заголовок таблицы остатков,
    дата следующего блока и ШАПКА НОВОЙ ТАБЛИЦЫ (SHIPPING MARK) — иначе
    китайский план, под которым логисты не подписали казахский, проглатывал
    его строки как свои (17.09.2026)."""
    n_rows = len(grid)
    items = []
    empty_streak = 0
    k = start
    while k < n_rows and empty_streak <= 2:
        mark_cell = grid[k][j] if j < len(grid[k]) else None
        mark = str(mark_cell).strip() if mark_cell is not None else ""
        if not mark:
            empty_streak += 1
            k += 1
            continue
        empty_streak = 0
        if mark.upper() == "TOTAL":
            return items, k + 1
        if _is_stays_heading(mark_cell):
            # у блока нет строки TOTAL, и сразу под ним таблица
            # остатков — её строки НЕ состав фуры
            break
        if _is_table_header(mark_cell):
            break
        if _is_title(mark_cell) and re.search(r"HORGOS|TASHKENT|\bTO\b", mark.upper()):
            # заголовок следующей таблицы без даты («HORGOS TO TASHKENT …»):
            # марки грузов таких слов не содержат
            break
        if isinstance(mark_cell, (datetime, date)) or (
            isinstance(mark_cell, str) and _DATE_STR_RE.match(mark_cell)
            and _is_title(grid[k + 1][j] if k + 1 < n_rows and j < len(grid[k + 1]) else None)
        ):
            # наткнулись на дату следующего блока в этой же колонке
            break

        def cval(off, _k=k):
            idx = j + off
            return grid[_k][idx] if idx < len(grid[_k]) else None

        arrive = ""
        for off in (5, 4, 6):
            v = cval(off)
            if isinstance(v, (datetime, date)):
                arrive = _fmt_date(v)
                break
            if isinstance(v, str) and re.match(r"^\d{1,2}[-./]", v.strip()):
                arrive = v.strip()
                break
        items.append({
            "mark": mark,
            "ctn": _num(cval(1)),
            "cbm": _num(cval(2)),
            "kg": _num(cval(3)),
            "arrive": arrive,
        })
        k += 1
    return items, k


# Под китайским планом логисты пишут казахский (Horgos → Tashkent) и иногда
# забывают его дату и заголовок «HORGOS TO TASHKENT»: остаётся голая таблица
# с шапкой SHIPPING MARK. По правилу владельца (30.09.2026) таблица под
# китайским планом — это его казахский план; бот предупреждает логистов, но
# без ответа принимает её как казахский.
# Признак казахской таблицы — колонка PARTIYA (из какой партии груз): она
# есть у всех казахских планов с мая 2026. Таблица без неё под китайским
# планом бывает и другой — в 03.09 это копия китайского плана с ценами,
# а настоящий казахский план подписан ниже.
_UNTITLED_SCAN_ROWS = 25


def _header_has_partiya(grid: list, r: int, j: int) -> bool:
    row = grid[r] if 0 <= r < len(grid) else []
    for c in range(j, min(len(row), j + 11)):
        v = row[c]
        if isinstance(v, str) and re.search(r"PART|ПАРТ", v.strip().upper()):
            return True
    return False


def _find_untitled_table(grid: list, j: int, after: int):
    """Таблица без даты/заголовка под китайским планом в колонке j.
    → (строка шапки SHIPPING MARK, заголовок, если он всё-таки написан
    без даты, иначе "") или None. Без заголовка таблица должна иметь
    колонку PARTIYA — иначе это не казахский план."""
    n_rows = len(grid)

    def cell(r):
        return grid[r][j] if r < n_rows and j < len(grid[r]) else None

    title = ""
    preamble = 0
    for r in range(after, min(n_rows, after + _UNTITLED_SCAN_ROWS)):
        v = cell(r)
        text = str(v).strip() if v is not None else ""
        if not text or text.upper() == "TOTAL":
            continue
        if _is_block_date(v) and _is_title(cell(r + 1)):
            return None          # ниже обычный блок с датой и заголовком
        if _is_stays_heading(v):
            return None          # сразу таблица остатков — казахского плана нет
        if _is_table_header(v):
            if not title and not _header_has_partiya(grid, r, j):
                return None      # не казахская таблица (копия, расчёт цен…)
            return r, title
        preamble += 1
        if preamble > 2:
            return None          # что-то своё — не угадываем
        if _is_block_date(v):
            continue             # дата без заголовка
        if _is_title(v) and _is_kazakh_title(text):
            title = text         # заголовок без даты
            continue
        return None
    return None


def _implicit_kazakh_title(warehouses: list) -> str:
    """Название для неподписанного казахского плана — так, как логисты
    обычно пишут сами: когда они допишут заголовок, привязка партии не
    потеряется."""
    wh = set(warehouses or [])
    if {"YIWU", "ZHONGSHAN"} <= wh:
        return "HORGOS TO TASHKENT YIWU + ZH YARGXOL"
    if "YIWU" in wh:
        return "HORGOS TO TASHKENT - YIWU YARGXOL"
    if "ZHONGSHAN" in wh:
        return "HORGOS TO TASHKENT - ZHONGSHAN YARGXOL"
    return "HORGOS TO TASHKENT YARGXOL"


def _block(date_cell, title: str, kind: str, warehouses: list, items: list, stays: list, col: int) -> dict:
    return {
        "date": _fmt_date(date_cell),
        "title": title,
        "kind": kind,          # china (склад→Horgos) | kazakh (Horgos→Tashkent)
        "warehouses": warehouses,
        "items": items,
        # «horgos skladda qoladigan yuklar» под этим блоком
        "stays": stays,
        "col": col,
        "total_ctn": round(sum(x["ctn"] for x in items), 2),
        "total_cbm": round(sum(x["cbm"] for x in items), 3),
        "total_kg": round(sum(x["kg"] for x in items), 2),
    }


def _parse_tab(grid: list) -> list:
    """Extract every plan block from one tab's cell grid."""
    blocks = []
    n_rows = len(grid)
    for i in range(n_rows - 2):
        row = grid[i]
        for j, cell in enumerate(row):
            if not _is_block_date(cell):
                continue
            title_cell = grid[i + 1][j] if j < len(grid[i + 1]) else None
            if not _is_title(title_cell):
                continue
            title = str(title_cell).strip()
            # данные начинаются под заголовком; строку 'SHIPPING MARK'
            # пропускаем (в казахских блоках её может не быть)
            start = i + 2
            first = grid[start][j] if start < n_rows and j < len(grid[start]) else None
            if _is_table_header(first):
                start += 1
            items, k = _read_rows(grid, j, start)
            if not items:
                continue
            upper_title = title.upper()
            kind = "kazakh" if _is_kazakh_title(title) else "china"
            warehouses = [w for w in ("YIWU", "ZHONGSHAN") if w in upper_title]
            if kind == "kazakh" and ("ZH" in upper_title and "ZHONGSHAN" not in warehouses):
                warehouses.append("ZHONGSHAN")
            blocks.append(_block(cell, title, kind, warehouses, items, _parse_stays(grid, j, k), j))
            if kind != "china":
                continue
            found = _find_untitled_table(grid, j, k)
            if found is None:
                continue
            header_row, written_title = found
            kz_items, kz_end = _read_rows(grid, j, header_row + 1)
            if not kz_items:
                continue
            kz = _block(cell, written_title or _implicit_kazakh_title(warehouses), "kazakh",
                        list(warehouses), kz_items, _parse_stays(grid, j, kz_end), j)
            # логисты не написали «HORGOS TO TASHKENT» — бот предупредит их
            kz["untitled"] = not written_title
            kz["header_row"] = header_row + 1          # номер строки в листе (с 1)
            blocks.append(kz)
    # подписанный казахский план той же даты в той же колонке — главный:
    # таблица без заголовка над ним тогда не план (кейс 03.09.2026)
    titled = {(b["col"], b["date"][:10]) for b in blocks
              if b["kind"] == "kazakh" and not b.get("header_row")}
    return [b for b in blocks
            if not (b.get("header_row") and (b["col"], b["date"][:10]) in titled)]


def _get_parsed(force: bool = False) -> dict:
    """{tab_name: [blocks]} for the whole workbook, cached."""
    now = time.monotonic()
    with _lock:
        cached = _cache.get("parsed")
        if not force and cached and cached["expires_at"] > now:
            return cached["data"]
    wb = _download_workbook()
    data = {}
    for ws in wb.worksheets:
        # Читаем ВСЮ ширину листа. Раньше стоял потолок max_col=80 (до
        # колонки CB), и планы, дописанные правее, были невидимы: план
        # 25.08.2026 лежал в CH:CM (86-91) — бот честно отвечал «такого
        # плана нет», хотя в шитсе он был. Любой фиксированный предел
        # рано или поздно упирается в то же самое: логисты дописывают
        # блоки вправо.
        grid = [list(r) for r in ws.iter_rows(values_only=True)]
        try:
            blocks = _parse_tab(grid)
        except Exception:
            blocks = []
        if blocks:
            data[ws.title] = blocks
            width = max((len(r) for r in grid), default=0)
            if width >= _WIDTH_WARN_COLUMNS:
                log.info("Лист «%s»: ширина %s колонок, блоков %s", ws.title, width, len(blocks))
    with _lock:
        _cache["parsed"] = {"data": data, "expires_at": time.monotonic() + CACHE_TTL_SECONDS}
    return data


def get_loading_plans(tab: str = "", force: bool = False) -> dict:
    """Plan blocks of one tab (default: the current month's tab)."""
    data = _get_parsed(force=force)
    tabs = sorted(data.keys())
    chosen = ""
    if tab:
        wanted = tab.strip().lower().replace(" ", "")
        for name in tabs:
            if name.lower().replace(" ", "") == wanted:
                chosen = name
                break
        if not chosen:
            for name in tabs:
                if wanted in name.lower().replace(" ", ""):
                    chosen = name
                    break
        if not chosen:
            return {"ok": False, "error": f"Вкладка «{tab}» не найдена", "tabs": tabs}
    else:
        chosen = default_tab(tabs)
    if not chosen:
        return {"ok": False, "error": "В шитсе не найдено ни одного блока планов", "tabs": tabs}
    return {"ok": True, "tab": chosen, "tabs": tabs, "plans": data[chosen]}


def find_plan(tab: str, title: str, plan_date: str = "", force: bool = False) -> dict | None:
    """Locate ONE plan block by tab + (partial) title + optional date."""
    result = get_loading_plans(tab, force=force)
    if not result.get("ok"):
        return None
    wanted_title = (title or "").strip().upper()
    wanted_date = (plan_date or "").strip()
    candidates = []
    for block in result["plans"]:
        if wanted_title and wanted_title not in block["title"].upper():
            continue
        if wanted_date and block["date"] != wanted_date:
            continue
        candidates.append(block)
    if len(candidates) == 1:
        found = dict(candidates[0])
        found["tab"] = result["tab"]
        return found
    return None
