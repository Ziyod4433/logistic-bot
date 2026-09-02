# -*- coding: utf-8 -*-
"""Авто-жизнь партий по планам погрузки (шитс SKLAD, ведут логисты).

Жизненный цикл (утверждён владельцем 21.08.2026):

1. Логисты составляют ПЕРВЫЙ план — китайская фура со склада
   (YIWU/ZHONGSHAN → Horgos). Как только блок появился в шитсе, бот
   утром (06:30) САМ открывает партию в панели: имя = дата + суффикс
   склада («14.08.2026 YIWU» / «14.08.2026 ZH»), статус Xitoy, состав
   из плана, Telegram-группы подтягиваются автоматически. BL без
   группы — бот спрашивает в управляющей группе, отмечая ответственных.

2. Пока партия едет до Хоргоса, состав сверяется с китайским планом
   ежедневно: новые BL добавляются, цифры обновляются. Ничего не
   удаляется автоматически.

3. Фура прибыла в Хоргос (логист ставит статус «Horgos») —
   груз ПЕРЕГРУЖАЕТСЯ в казахские фуры по ВТОРОМУ плану (HORGOS TO
   TASHKENT…). Вместимость фур разная, поэтому груз одной китайской
   фуры может разъехаться по казахским планам РАЗНЫХ партий. Бот
   спрашивает разрешения (отмечая Hoji dodam), а если ответа нет —
   применяет сам: состав партии приводится к казахскому плану,
   «чужие» грузы ПЕРЕЕЗЖАЮТ между партиями (дедубликатор: один и тот
   же груз не может числиться в двух партиях; но раздельные грузы
   одного клиента в двух фурах — законны и не трогаются).

4. BL, застрявший в Хоргосе (есть в китайском плане, нет ни в одном
   казахском) — никуда не перекидывается, остаётся и попадает в отчёт.

Этот модуль — чистые вычисления (парсинг/сопоставление/дифф);
записью в БД и Telegram занимается app.py.
"""

import re
from datetime import datetime, timedelta

import database as db
from services import loading_plan_service as lps
from services.fura_status_service import warehouse_suffix

# статус, с которого источником истины становится казахский план
HORGOS_STATUS = "Horgos"

# насколько должны разойтись цифры, чтобы считать это изменением
CTN_EPS = 0.5
CBM_EPS = 0.01
KG_EPS = 1.0

_DATE_RE = re.compile(r"(\d{2})[.\-/](\d{2})[.\-/](\d{4})")
_DIGITS8_RE = re.compile(r"(?<!\d)(\d{8})(?!\d)")


def normalize_mark(value) -> str:
    """«BL-358», «bl 358», «5077.0» → «BL358», «5077»."""
    text = str(value or "").strip().upper()
    text = re.sub(r"\.0+$", "", text)          # числовые марки: 555.0 → 555
    return re.sub(r"[\s\-_.]+", "", text)


def date_key(value) -> str:
    """«14.08.2026 YIWU 2» / datetime / «BL14082026 ZH» → «14082026».

    Берём ПЕРВУЮ дату dd.mm.yyyy (или первую 8-значную группу), а не все
    цифры подряд — иначе порядковый номер в имени («… YIWU 2») ломал бы
    ключ."""
    if isinstance(value, datetime):
        return value.strftime("%d%m%Y")
    text = str(value or "")
    m = _DATE_RE.search(text)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"
    m = _DIGITS8_RE.search(text)
    return m.group(1) if m else ""


def stage_for_status(status: str) -> str:
    """china (до Хоргоса) | kazakh (Хоргос и дальше)."""
    # «Horgos (Qozoq)» переименован в «Horgos»; строки из внешних
    # источников и не мигрированных БД могут прийти со старым написанием
    normalized = db.normalize_status_value(status)
    if normalized in (db.DELIVERED_STATUS, getattr(db, "LEGACY_DELIVERED_STATUS", "")):
        return "kazakh"
    try:
        idx = db.STATUSES.index(normalized)
        horgos_idx = db.STATUSES.index(HORGOS_STATUS)
    except ValueError:
        return "china"
    kyrgyz_start = len(db.STATUSES)
    try:
        kyrgyz_start = db.STATUSES.index("Kashgar (Qirg'iz)")
    except ValueError:
        pass
    if idx >= kyrgyz_start:
        # кыргызская ветка — казахских планов у неё нет
        return "china"
    return "kazakh" if idx >= horgos_idx else "china"


# Локации ПОСЛЕ погрузки в казахскую фуру (владелец, 24.08.2026): с этого
# момента состав партии обязан жить по КАЗАХСКОМУ плану — груз уже едет
# другой фурой, и трекинг по китайскому составу уйдёт не тем клиентам.
# Сам Хоргос сюда не входит: там перегрузка ещё идёт (вопрос Hoji dodam).
AFTER_HORGOS_STATUSES = (
    "Nurjo'li", "Jarkent", "Almata", "Taraz", "Shimkent",
    "Qonusbay", "Saryagash", "Yallama", "Toshkent(Chuqursoy ULS da)",
)


def is_after_horgos_loading(batch: dict) -> bool:
    """Фура уже выехала из Хоргоса — казахский план обязан быть применён."""
    return db.normalize_status_value(batch.get("status")) in AFTER_HORGOS_STATUSES


def is_arrived(batch: dict) -> bool:
    """Партия уже в Ташкенте/выдана — перегрузка в Хоргосе позади."""
    status = str(batch.get("status") or "")
    return (
        status in set(getattr(db, "ARRIVED_STATUSES", ()))
        or status in (db.DELIVERED_STATUS, getattr(db, "LEGACY_DELIVERED_STATUS", ""))
        or bool((batch.get("client_delivery_date") or "").strip())
    )


def aggregate_block(block: dict) -> dict:
    """Марки блока, схлопнутые по коду: один BL часто занимает несколько
    строк плана (отсюда разбивка мест «2 + 1 + 2 + 1»)."""
    agg: dict = {}
    for item in block.get("items") or []:
        raw = str(item.get("mark") or "").strip()
        if not raw:
            continue
        code = re.sub(r"\.0+$", "", raw)
        key = normalize_mark(code)
        if not key or key == "TOTAL":
            continue
        entry = agg.setdefault(key, {"code": code, "ctn": 0.0, "cbm": 0.0, "kg": 0.0, "parts": []})
        ctn = float(item.get("ctn") or 0)
        entry["ctn"] += ctn
        entry["cbm"] += float(item.get("cbm") or 0)
        entry["kg"] += float(item.get("kg") or 0)
        if ctn:
            entry["parts"].append(ctn)
    for entry in agg.values():
        entry["ctn"] = round(entry["ctn"], 2)
        entry["cbm"] = round(entry["cbm"], 3)
        entry["kg"] = round(entry["kg"], 2)
        entry["breakdown"] = (
            " + ".join(f"{p:g}" for p in entry["parts"]) if len(entry["parts"]) > 1 else ""
        )
    return agg


def _tab_rank(tab: str) -> tuple:
    """(год, месяц) вкладки — чтобы из двух копий блока предпочесть более
    позднюю (логисты копируют блок в следующий месяц и правят там)."""
    low = str(tab or "").lower().replace(" ", "")
    year_m = re.search(r"(20\d{2})", low)
    year = int(year_m.group(1)) if year_m else 0
    month = 0
    for num, tokens in lps._MONTH_TOKENS.items():
        if any(t in low for t in tokens):
            month = num
            break
    return (year, month)


def is_plan_tab(tab: str) -> bool:
    """Планы погрузки живут ТОЛЬКО в месячных вкладках («AVGUST 2026»,
    «Iyul2026»). Служебные листы (SKLAD — остатки склада с датой =TODAY(),
    CRM, REPORT, STATISTICS…) планами не являются, хотя парсер и может
    выцепить там «дата + ZHONGSHAN»."""
    year, month = _tab_rank(tab)
    return bool(year and month)


def is_plan_block(block: dict) -> bool:
    """Блок — настоящий план фуры: в названии есть склад или маршрут и
    у строк есть цифры (список остатков склада идёт с нулями)."""
    title = re.sub(r"\s+", " ", str(block.get("title") or "").upper()).strip()
    if not any(k in title for k in ("YIWU", "ZHONGSHAN", "HORGOS", "FURA")):
        return False
    if not title.replace("YIWU", "").replace("ZHONGSHAN", "").strip(" -"):
        return False  # одно лишь слово «ZHONGSHAN» — заголовок склада, не план
    totals = float(block.get("total_ctn") or 0) + float(block.get("total_cbm") or 0) + float(block.get("total_kg") or 0)
    if totals <= 0:
        totals = sum(
            float(i.get("ctn") or 0) + float(i.get("cbm") or 0) + float(i.get("kg") or 0)
            for i in (block.get("items") or [])
        )
    return totals > 0


def _block_marks(block: dict) -> frozenset:
    return frozenset(
        normalize_mark(i.get("mark")) for i in (block.get("items") or []) if i.get("mark")
    )


def all_blocks(force: bool = False) -> list:
    """Все блоки планов всей книги: [{tab, date, title, kind, ordinal, items…}].

    • Один и тот же блок, скопированный в другую вкладку (та же дата,
      название, вид и ЗАМЕТНО пересекающийся состав — ≥ половины марок
      или один набор вложен в другой), считается ОДНИМ блоком; берётся
      копия из более поздней вкладки (там правят), при равенстве — с
      бо́льшим числом марок.
    • ordinal — порядковый номер среди РАЗНЫХ блоков с одинаковыми
      названием и датой (две фуры одного склада в один день); порядок
      задаётся составом, а не позицией на листе."""
    data = lps._get_parsed(force=force)
    groups: dict = {}
    order: list = []
    for tab, tab_blocks in data.items():
        if not is_plan_tab(tab):
            continue  # SKLAD, CRM, REPORT, … — не планы погрузки
        for block in tab_blocks:
            title = str(block.get("title") or "").strip()
            if not is_plan_block(block):
                continue
            dkey = date_key(block.get("date"))
            marks = _block_marks(block)
            key = (title.upper(), dkey, block.get("kind"))
            if key not in groups:
                groups[key] = []
                order.append(key)
            entry = dict(block)
            entry["tab"] = tab
            entry["_marks"] = marks
            merged = False
            for i, existing in enumerate(groups[key]):
                em = existing["_marks"]
                inter = len(em & marks)
                union = len(em | marks) or 1
                same_tab = existing["tab"] == tab
                # вложенность считаем копией ТОЛЬКО между вкладками: в одной
                # вкладке маленькая «добивочная» фура с частью тех же клиентов —
                # это отдельная фура, а не копия
                is_copy = inter and (inter / union >= 0.5 or (not same_tab and (em <= marks or marks <= em)))
                if is_copy:
                    if (_tab_rank(tab), len(marks)) > (_tab_rank(existing["tab"]), len(em)):
                        groups[key][i] = entry
                    merged = True
                    break
            if not merged:
                groups[key].append(entry)
    blocks = []
    for key in order:
        group = groups[key]
        group.sort(key=lambda e: (min(e["_marks"]) if e["_marks"] else "", e["tab"]))
        for i, entry in enumerate(group, 1):
            entry["ordinal"] = i
            entry.pop("_marks", None)
            blocks.append(entry)
    return blocks


def ref_title(block: dict) -> str:
    """Название блока для привязки: «TITLE» или «TITLE #2» для дублей."""
    title = str(block.get("title") or "").strip()
    ordinal = int(block.get("ordinal") or 1)
    return title if ordinal <= 1 else f"{title} #{ordinal}"


def ref_base(title: str) -> str:
    """«TITLE #2» → «TITLE»."""
    return re.sub(r"\s+#\d+$", "", str(title or "").strip()).upper()


def block_matches_ref(block: dict, tab: str, title: str, plan_date: str) -> bool:
    """Привязка = название(+порядковый номер) + дата. Вкладка НЕ входит:
    блок могут скопировать в следующий месяц."""
    return (
        ref_title(block).upper() == str(title or "").strip().upper()
        and date_key(block.get("date")) == date_key(plan_date)
    )


def block_same_group_as_ref(block: dict, title: str, plan_date: str) -> bool:
    """Тот же план по названию и дате, возможно с другим порядковым номером."""
    return (
        ref_base(str(block.get("title") or "")) == ref_base(title)
        and date_key(block.get("date")) == date_key(plan_date)
    )


def resolve_ref_block(batch: dict, blocks: list, kind: str, batch_codes: set):
    """Блок по сохранённой привязке партии. Если одноимённых блоков на
    дату несколько (ordinal), выбираем тот, что больше всего пересекается
    с составом партии — порядковые номера могут поехать при правках листа."""
    if not batch_has_ref(batch):
        return None
    exact = None
    group = []
    for block in blocks:
        if block.get("kind") != kind:
            continue
        if block_matches_ref(block, batch.get("plan_tab"), batch.get("plan_title"), batch.get("plan_date")):
            exact = block
        if block_same_group_as_ref(block, batch.get("plan_title"), batch.get("plan_date")):
            group.append(block)
    if len(group) <= 1:
        chosen, hits = (exact or (group[0] if group else None)), None
    else:
        best, best_hits = exact, -1
        for block in group:
            hits = len(set(aggregate_block(block).keys()) & batch_codes)
            if hits > best_hits or (hits == best_hits and block is exact):
                best, best_hits = block, hits
        chosen, hits = best, best_hits
    # Привязка могла ПРОТУХНУТЬ: логисты меняют дату плана прямо в шитсе
    # («14.08.2026-2» стал «15.08.2026-2»), и тогда по старой дате находится
    # ОДНОИМЁННЫЙ СОСЕДНИЙ блок — чужой партии. Если у него нет ни одного
    # общего груза с нашим составом, это не наш план: пусть вызывающий код
    # подберёт блок по составу (find_block_for_batch).
    if chosen is not None and batch_codes and exact is None:
        if hits is None:
            hits = len(set(aggregate_block(chosen).keys()) & batch_codes)
        if hits == 0:
            return None
    return chosen


def batch_has_ref(batch: dict) -> bool:
    return bool((batch.get("plan_title") or "").strip())


def block_attached_to(block: dict, batches: list, kind: str | None = None,
                      blocks: list | None = None, codes_of=None) -> list:
    """Партии, у которых сохранённая привязка указывает на этот блок.
    Если переданы blocks+codes_of, одноимённые блоки (ordinal) разрешаются
    по пересечению состава, а не по точному «#N» — номера могут поехать."""
    out = []
    for batch in batches:
        if kind and (batch.get("plan_kind") or "") != kind:
            continue
        if not batch_has_ref(batch):
            continue
        if blocks is not None and codes_of is not None:
            if block_same_group_as_ref(block, batch.get("plan_title"), batch.get("plan_date")):
                resolved = resolve_ref_block(batch, blocks, kind or block.get("kind"), codes_of(batch))
                if resolved is block:
                    out.append(batch)
            continue
        if block_matches_ref(block, batch.get("plan_tab"), batch.get("plan_title"), batch.get("plan_date")):
            out.append(batch)
    return out


def suffix_for_block(block: dict) -> str:
    """Суффикс склада для имени авто-партии: YIWU | ZH | ''."""
    warehouses = block.get("warehouses") or []
    title = str(block.get("title") or "").upper()
    if "YIWU" in warehouses or "YIWU" in title:
        return "YIWU"
    if "ZHONGSHAN" in warehouses or "ZHONGSHAN" in title:
        return "ZH"
    return ""


def batch_name_for_block(block: dict) -> str:
    # дата в блоке может быть подписана «14.08.2026-2» (вторая фура дня) —
    # имя партии всегда каноничное dd.mm.yyyy, дубль различается суффиксом
    key = date_key(block.get("date"))
    date = f"{key[:2]}.{key[2:4]}.{key[4:]}" if len(key) == 8 else str(block.get("date") or "").strip()
    suffix = suffix_for_block(block)
    ordinal = int(block.get("ordinal") or 1)
    name = f"{date} {suffix}".strip()
    return name if ordinal <= 1 else f"{name} {ordinal}"


def fresh_china_blocks(blocks: list, today=None, days_back: int = 10, days_forward: int = 45) -> list:
    """Китайские планы со «свежей» датой — кандидаты на авто-открытие.
    Открываем только планы, где в названии назван склад (YIWU/ZHONGSHAN)
    или маршрут «TO HORGOS» — иначе это не фура со склада."""
    today = today or datetime.now(db.TASHKENT_TZ).date()
    fresh = []
    for block in blocks:
        if block.get("kind") != "china":
            continue
        title = str(block.get("title") or "").upper()
        if not any(k in title for k in ("YIWU", "ZHONGSHAN", "TO HORGOS")):
            continue
        key = date_key(block.get("date"))
        if len(key) != 8:
            continue
        try:
            block_date = datetime.strptime(key, "%d%m%Y").date()
        except ValueError:
            continue
        if today - timedelta(days=days_back) <= block_date <= today + timedelta(days=days_forward):
            fresh.append(block)
    return fresh


def find_block_for_batch(batch: dict, blocks: list, batch_codes: set, batches: list | None = None) -> tuple:
    """(block, why) — блок плана, который описывает партию на её стадии.

    Блоки, уже привязанные к ДРУГОЙ партии того же вида, не кандидаты.
    На казахской стадии требуется пересечение состава (> 0): казахский
    план той же даты, в котором нет ни одного нашего груза, — чужой."""
    stage = stage_for_status(batch.get("status"))
    key = date_key(batch.get("name"))
    if not key:
        return None, "в имени партии нет даты"
    others = [b for b in (batches or []) if b["id"] != batch["id"]]

    saved_kind = (batch.get("plan_kind") or "").strip()
    if saved_kind == stage and batch_has_ref(batch):
        block = resolve_ref_block(batch, blocks, stage, batch_codes)
        if block is not None:
            return block, "по сохранённой привязке"

    candidates = []
    for b in blocks:
        if b.get("kind") != stage or date_key(b.get("date")) != key:
            continue
        if others and block_attached_to(b, others, kind=stage):
            continue  # занят другой партией
        candidates.append(b)
    if not candidates:
        return None, ("казахского плана на эту дату ещё нет" if stage == "kazakh"
                      else "китайского плана на эту дату нет")
    if stage == "china" and len(candidates) == 1:
        return candidates[0], "по дате"
    best, best_hits = None, 0
    for block in candidates:
        hits = len(set(aggregate_block(block).keys()) & batch_codes)
        if hits > best_hits:
            best, best_hits = block, hits
    if best is None:
        return None, "состав партии не совпал ни с одним планом на эту дату"
    if stage == "kazakh":
        # один общий клиент с чужим планом (раздельный груз) — не повод
        # считать план своим: нужна заметная доля состава партии
        need = 1 if len(batch_codes) <= 1 else max(2, -(-len(batch_codes) // 4))
        if best_hits < need:
            return None, f"казахский план на эту дату совпал лишь по {best_hits} BL — мал для своего"
    return best, f"по составу ({best_hits} совпадений)"


def find_batch_for_china_block(block: dict, batches: list, codes_of, blocks: list | None = None) -> dict | None:
    """Какая партия (активная ИЛИ уже закрытая) живёт под этим китайским
    планом — чтобы не открыть вторую под тот же план.

    codes_of(batch) -> set нормализованных кодов партии (ленивая загрузка).
    blocks — все блоки книги: если на дату несколько ОДНОИМЁННЫХ планов
    (две фуры одного склада), партия владеет тем из них, с которым больше
    пересекается состав — порядковые номера «#N» могут поехать, когда
    логисты дописывают второй блок.
    Совпадение (по убыванию надёжности): сохранённая привязка; имя партии
    ровно как у авто-партии этого блока; та же дата + БОЛЬШИНСТВО состава
    плана в партии; партия той же даты, уже ушедшая за Хоргос (её
    китайский план — история); единственная партия на дату без привязки.
    """
    key = date_key(block.get("date"))
    auto_name = batch_name_for_block(block).upper()
    block_suffix = suffix_for_block(block)

    def owns_in_group(batch) -> bool:
        """Привязка партии — в группе этого блока; но её ли ЭТОТ блок
        (а не одноимённый сосед)? Решает пересечение состава."""
        if blocks is None or not batch_has_ref(batch):
            return True
        best = resolve_ref_block(batch, blocks, "china", codes_of(batch))
        return best is None or best is block

    plan_keys_early = set(aggregate_block(block).keys())
    n_plan_early = len(plan_keys_early) or 1

    def _date_change_candidate():
        """Смена даты фуры в шитсе (кейс 02.09.2026: 30.08 → 31.08 открыл
        дубль на 19 одинаковых BL). Та же фура, а не новый рейс, если:
        партия привязана к плану ТОЙ ЖЕ группы названий (kind=china), ещё
        в Китае и не закрыта, её блок со старой датой ИСЧЕЗ из листа, а
        состав нового блока минимум наполовину уже лежит в партии."""
        if blocks is None:
            return None
        cand, cand_hits = None, 0
        for b in batches:
            if not batch_has_ref(b) or (b.get("plan_kind") or "") != "china":
                continue
            if (b.get("client_delivery_date") or "").strip() or is_arrived(b):
                continue
            if stage_for_status(b.get("status")) != "china":
                continue                      # за Хоргосом дату уже не двигают
            if date_key(b.get("plan_date")) == key:
                continue                      # та же дата — не «переезд»
            if ref_base(str(block.get("title") or "")) != ref_base(str(b.get("plan_title") or "")):
                continue                      # другой план
            old_block = resolve_ref_block(b, blocks, "china", codes_of(b))
            if old_block is not None and old_block is not block:
                continue                      # старый блок жив — это ДВЕ фуры
            hits = len(codes_of(b) & plan_keys_early)
            if hits >= 2 and hits * 2 >= n_plan_early and hits > cand_hits:
                cand, cand_hits = b, hits
        return cand

    same_date = []
    for batch in batches:
        if batch_has_ref(batch) and block_matches_ref(
            block, batch.get("plan_tab"), batch.get("plan_title"), batch.get("plan_date")
        ) and owns_in_group(batch):
            return batch
        if date_key(batch.get("name")) == key:
            same_date.append(batch)
    if not same_date:
        return _date_change_candidate()

    def bound_elsewhere(batch) -> bool:
        """Партия уже ведётся ДРУГИМ китайским планом (не копией этого)."""
        if not batch_has_ref(batch) or (batch.get("plan_kind") or "") != "china":
            return False
        if not block_same_group_as_ref(block, batch.get("plan_title"), batch.get("plan_date")):
            return True
        return not owns_in_group(batch)   # одноимённый сосед — тоже чужой

    plan_keys = set(aggregate_block(block).keys())
    n_plan = len(plan_keys) or 1

    # имя ровно как у авто-партии этого блока — но не чужая по плану
    for batch in same_date:
        if str(batch.get("name") or "").strip().upper() == auto_name and not bound_elsewhere(batch):
            return batch

    # пересечение состава
    best, best_hits = None, 0
    for batch in same_date:
        hits = len(codes_of(batch) & plan_keys)
        if batch_has_ref(batch) and (batch.get("plan_kind") or "") == "china" and block_same_group_as_ref(
            block, batch.get("plan_title"), batch.get("plan_date")
        ) and not owns_in_group(batch):
            continue  # одноимённый сосед уже её план — этот блок (отпочкованная фура) откроет свою партию
        if bound_elsewhere(batch) and hits * 2 < n_plan:
            continue  # чужой план и нет большинства нашего состава
        if hits > best_hits:
            best, best_hits = batch, hits
    if best is not None:
        if best_hits * 2 >= n_plan:
            return best  # большинство плана уже в партии
        # та же группа планов (копия/ordinal) с заметным пересечением — своя
        if (
            batch_has_ref(best)
            and block_same_group_as_ref(block, best.get("plan_title"), best.get("plan_date"))
            and owns_in_group(best)
            and best_hits * 10 >= n_plan * 3
        ):
            return best
        # без привязки, склад в имени совпадает и хоть один общий груз — своя
        bsuffix = warehouse_suffix(best.get("name"))
        if not batch_has_ref(best) and bsuffix and block_suffix and bsuffix == block_suffix and best_hits >= 1:
            return best
    # партия этой даты уже за Хоргосом (казахская привязка / закрыта):
    # китайский план — история, зомби не открываем
    for batch in same_date:
        past = stage_for_status(batch.get("status")) == "kazakh" or is_arrived(batch)
        if not past:
            continue
        bsuffix = warehouse_suffix(batch.get("name"))
        if not bsuffix or not block_suffix or bsuffix == block_suffix:
            return batch
    if len(same_date) == 1 and not batch_has_ref(same_date[0]):
        # единственная партия на дату без привязки (открыта вручную до
        # заполнения плана) — своя, если в имени не другой склад и она либо
        # пуста, либо хоть что-то из плана уже содержит
        only = same_date[0]
        bsuffix = warehouse_suffix(only.get("name"))
        if not bsuffix or not block_suffix or bsuffix == block_suffix:
            only_codes = codes_of(only)
            if not only_codes or (only_codes & plan_keys):
                return only
    # последний шанс: фура с подвинутой датой (одноимённые партии другой
    # даты в same_date не попадают — сюда доходим и при непустом same_date)
    return _date_change_candidate()


def diff_against_plan(batch_bls: list, plan: dict) -> dict:
    """Что разошлось: add (нет в партии), update (цифры), extra (нет в плане)."""
    have = {}
    for bl in batch_bls:
        have[normalize_mark(bl.get("code"))] = bl
    to_add, to_update, extras = [], [], []
    for key, entry in plan.items():
        bl = have.get(key)
        if bl is None:
            to_add.append(entry)
            continue
        changes = {}
        if abs(float(bl.get("quantity_places") or 0) - entry["ctn"]) > CTN_EPS:
            changes["places"] = entry["ctn"]
        if abs(float(bl.get("volume_cbm") or 0) - entry["cbm"]) > CBM_EPS:
            changes["cbm"] = entry["cbm"]
        if abs(float(bl.get("weight_kg") or 0) - entry["kg"]) > KG_EPS:
            changes["kg"] = entry["kg"]
        if changes:
            to_update.append({"bl": bl, "plan": entry, "changes": changes})
    for key, bl in have.items():
        if key not in plan:
            extras.append(bl)
    return {"add": to_add, "update": to_update, "extra": extras}


def kazakh_blocks_containing(blocks: list, key: str, exclude_block: dict | None = None,
                             window_from: dict | None = None, window_days: int = 21) -> list:
    """Все казахские блоки, в которых встречается этот код. window_from —
    ограничить планами той же перегрузки: от даты этого блока до
    +window_days (коды клиентов повторяются из рейса в рейс, и план
    прошлого месяца — это ДРУГОЙ груз того же клиента)."""
    hits = []
    lo = hi = None
    if window_from is not None:
        base = date_key(window_from.get("date"))
        try:
            lo = datetime.strptime(base, "%d%m%Y").date()
            hi = lo + timedelta(days=window_days)
        except ValueError:
            lo = hi = None
    for block in blocks:
        if block.get("kind") != "kazakh":
            continue
        if exclude_block is not None and block is exclude_block:
            continue
        if lo is not None:
            try:
                bdate = datetime.strptime(date_key(block.get("date")), "%d%m%Y").date()
            except ValueError:
                continue
            if not (lo <= bdate <= hi):
                continue
        if key in aggregate_block(block):
            hits.append(block)
    return hits


def predict_kazakh_block_owner(block2: dict, candidates: list, blocks: list,
                               batches: list, codes_of):
    """Будущий хозяин ещё НЕ применённого казахского блока: партия, для
    которой find_block_for_batch выбирает именно его. Претендент должен
    УЖЕ быть на казахской стадии — его фура реально стоит на перегрузке:
    это отсекает кыргызскую ветку (казахских планов у неё не бывает,
    stage=china) и партии, ещё не доехавшие до Хоргоса, — их груз
    дождётся фуру и переедет при следующей сверке (или его заберёт их
    собственное применение плана через donor-механику). Прибывшие не в
    счёт. При нескольких претендентах — максимум пересечения с планом,
    при равенстве меньший id (тот же тай-брейк, что и в остальной механике)."""
    best, best_hits = None, -1
    plan2 = set(aggregate_block(block2).keys())
    for cand in candidates:
        if is_arrived(cand) or stage_for_status(cand.get("status")) != "kazakh":
            continue
        got, _why = find_block_for_batch(cand, blocks, codes_of(cand), batches=batches)
        if got is not block2:
            continue
        hits = len(plan2 & codes_of(cand))
        if hits > best_hits or (hits == best_hits and best is not None and cand["id"] < best["id"]):
            best, best_hits = cand, hits
    return best


KYRGYZ_STATUSES = ("Kashgar (Qirg'iz)", "Irkeshtam", "Osh", "Dostlik", "Andijon")


def heading_to_horgos(batch: dict) -> bool:
    """Фура ещё едет к Хоргосу по КИТАЙСКО-казахскому маршруту: её казахский
    план впереди. Кыргызская ветка сюда не входит — казахских планов у неё
    не бывает."""
    if is_arrived(batch) or stage_for_status(batch.get("status")) != "china":
        return False
    return str(batch.get("status") or "").strip() not in KYRGYZ_STATUSES


def pending_kazakh_owner(block2: dict, candidates: list, blocks: list,
                         batches: list, codes_of):
    """Партия, которой этот казахский блок ДОСТАНЕТСЯ, когда её фура дойдёт
    до Хоргоса. Только для формулировки отчёта («⏳ ждёт свою фуру») —
    переезды по этому предсказанию НЕ делаются."""
    best, best_hits = None, -1
    plan2 = set(aggregate_block(block2).keys())
    for cand in candidates:
        if not heading_to_horgos(cand):
            continue
        probe = dict(cand)
        probe["status"] = HORGOS_STATUS
        got, _why = find_block_for_batch(probe, blocks, codes_of(cand), batches=batches)
        if got is not block2:
            continue
        hits = len(plan2 & codes_of(cand))
        if hits > best_hits or (hits == best_hits and best is not None and cand["id"] < best["id"]):
            best, best_hits = cand, hits
    return best


def extra_destination(key: str, block: dict, blocks: list, others: list,
                      codes_of, batches: list | None = None) -> tuple:
    """Куда уедет «лишний» код (остаток китайского плана, отсутствующий в
    казахском плане block) -> (партия | None, блок | None).

    «Сперва убрать китайский план»: чужой груз уезжает СРАЗУ при
    применении нашего казахского плана — сначала в партию с УЖЕ
    применённым планом, содержащим код, иначе к будущему хозяину этого
    плана (тот уже стоит в Хоргосе, но план ему ещё не применяли — ждать
    не нужно, иначе клиенту уйдёт трекинг чужой фуры). Планы ищем только
    в окне этой перегрузки (коды клиентов повторяются из рейса в рейс).
    Второй элемент кортежа — блок, где код найден: если партии нет, а блок
    есть, груз ЖДЁТ свою фуру (она ещё не дошла до Хоргоса) и переедет на
    следующей сверке."""
    containing = kazakh_blocks_containing(blocks, key, exclude_block=block, window_from=block)
    for b2 in containing:
        applied = [
            b for b in block_attached_to(b2, others, kind="kazakh", blocks=blocks, codes_of=codes_of)
            if not is_arrived(b)
        ]
        if applied:
            return applied[0], b2
    all_batches = batches if batches is not None else others
    for b2 in containing:
        owner = predict_kazakh_block_owner(b2, others, blocks, all_batches, codes_of)
        if owner is not None:
            return owner, b2
    # Хозяина в Хоргосе нет — но его фура может быть ещё в пути: тогда груз
    # ЖДЁТ её и НЕ переезжает вперёд неё (переезд в партию, которая ещё в
    # Китае, ломает donor-окно и плодит тихие дубли при правке плана).
    # Пока ждёт — он всё равно не должен получать трекинг чужой фуры:
    # этим занимается вызывающий код, исключая его из рассылки.
    for b2 in containing:
        if pending_kazakh_owner(b2, others, blocks, all_batches, codes_of) is not None:
            return None, b2
    return None, None


def donor_owns_code(donor_batch: dict, key: str, blocks: list, batches: list,
                    current_block: dict, codes_of=None) -> bool:
    """Раздельный груз или переезд? True — у партии-донора есть СВОЙ
    казахский план с этим кодом (часть груза едет её фурой) — тогда код
    остаётся и у донора. Чужой план той же даты, уже привязанный к другой
    партии, доказательством не считается. Одноимённые блоки разрешаются
    по пересечению состава (codes_of), чтобы сдвиг «#N» не путал хозяев."""
    donor_block = None
    if codes_of is not None and batch_has_ref(donor_batch) and (donor_batch.get("plan_kind") or "") == "kazakh":
        donor_block = resolve_ref_block(donor_batch, blocks, "kazakh", codes_of(donor_batch))
    for b2 in kazakh_blocks_containing(blocks, key, exclude_block=current_block):
        if donor_block is not None:
            if b2 is donor_block:
                return True
        elif batch_has_ref(donor_batch) and block_matches_ref(
            b2, donor_batch.get("plan_tab"), donor_batch.get("plan_title"), donor_batch.get("plan_date")
        ):
            return True
        owners = [
            b for b in block_attached_to(b2, batches, kind="kazakh", blocks=blocks, codes_of=codes_of)
            if b["id"] != donor_batch["id"]
        ]
        if owners:
            continue  # этот план — чужой
        if date_key(b2.get("date")) and date_key(b2.get("date")) == date_key(donor_batch.get("name")):
            return True
    return False


def donor_eligible(donor_batch: dict, target_batch: dict, block: dict | None = None,
                   window_days: int = 21) -> bool:
    """Может ли груз ПЕРЕЕХАТЬ из партии-донора при применении казахского
    плана target_batch. Донор — партия, чья фура тоже стоит в Хоргосе на
    ЭТОЙ перегрузке: выехала из Китая не позже даты казахского плана и не
    раньше чем за window_days до неё (коды клиентов повторяются из рейса в
    рейс — следующая фура того же клиента донором быть не может); при
    этом она либо на казахской стадии (ещё НЕ прибыла в Ташкент), либо
    той же даты, что и цель, но со статусом, отстающим от фуры."""
    if is_arrived(donor_batch):
        return False
    dkey = date_key(donor_batch.get("name"))
    if block is not None and dkey:
        try:
            ddate = datetime.strptime(dkey, "%d%m%Y").date()
            bdate = datetime.strptime(date_key(block.get("date")), "%d%m%Y").date()
        except ValueError:
            ddate = bdate = None
        if ddate and bdate and not (bdate - timedelta(days=window_days) <= ddate <= bdate):
            return False
    if stage_for_status(donor_batch.get("status")) == "kazakh":
        return True
    return bool(dkey) and dkey == date_key(target_batch.get("name"))
