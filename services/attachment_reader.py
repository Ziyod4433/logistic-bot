"""Текст из файла, который человек прислал ассистенту в личку.

Форматы: html/htm, txt/md/csv/tsv/json/xml/log, xlsx/xlsm, xls, pdf, docx, pptx.
Всё читается из байтов в памяти: файл не сохраняется на диск и ничего из него
не исполняется (скрипты HTML-страницы берутся только как текст).
"""
from __future__ import annotations

import io
import re
import zipfile
from datetime import date, datetime
from html.parser import HTMLParser
from xml.etree import ElementTree

MAX_FILE_BYTES = 20 * 1024 * 1024      # больше Bot API скачать не даёт
MAX_TEXT = 200_000                     # столько текста храним на одно вложение
_ZIP_MEMBER_MAX = 60 * 1024 * 1024     # защита от «zip-бомбы» в docx/pptx/xlsx
_SHEET_ROWS_MAX = 3000

TEXT_EXTS = {"txt", "md", "csv", "tsv", "json", "xml", "log", "yaml", "yml", "ini", "sql"}
HTML_EXTS = {"html", "htm"}
SUPPORTED_HINT = "html, txt, md, csv, json, xml, xlsx, xls, pdf, docx, pptx"


def kind_of(filename: str, mime: str = "") -> str:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    mime = (mime or "").lower()
    if ext in HTML_EXTS or mime == "text/html":
        return "html"
    if ext in {"xlsx", "xlsm"}:
        return "xlsx"
    if ext == "xls":
        return "xls"
    if ext == "pdf" or mime == "application/pdf":
        return "pdf"
    if ext == "docx":
        return "docx"
    if ext == "pptx":
        return "pptx"
    if ext in TEXT_EXTS or mime.startswith("text/") or mime in {"application/json", "application/xml"}:
        return "text"
    return ""


def decode_text(data: bytes) -> str:
    encodings = ("utf-16", "utf-8-sig") if data[:2] in (b"\xff\xfe", b"\xfe\xff") else ("utf-8-sig",)
    for enc in encodings:
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            pass
    try:
        from charset_normalizer import from_bytes
        best = from_bytes(data).best()
        if best is not None:
            return str(best)
    except Exception:
        pass
    try:
        return data.decode("cp1251")
    except UnicodeDecodeError:
        return data.decode("latin-1", errors="replace")


class _HtmlText(HTMLParser):
    """Видимый текст страницы + отдельно содержимое <script> (у страниц,
    собранных скриптом, данные лежат именно там)."""

    _SKIP = {"style", "noscript", "template", "svg", "head"}
    _BLOCK = {"p", "div", "section", "article", "header", "footer", "main", "aside", "nav", "ul", "ol",
              "table", "thead", "tbody", "tfoot", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "pre",
              "figure", "figcaption", "details", "summary", "dl", "dt", "dd", "form", "fieldset", "hr",
              "button", "label", "option", "select", "textarea"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.scripts: list[str] = []
        self.title = ""
        self._skip = 0
        self._in_script = False
        self._in_title = False

    def handle_starttag(self, tag, attrs):
        if tag == "script":
            self._in_script = True
            self.scripts.append("")
            return
        if tag == "title":
            self._in_title = True
            return
        if tag == "body":
            self._skip = 0          # незакрытый <head> не должен съесть всю страницу
            return
        if tag in self._SKIP:
            self._skip += 1
            return
        if tag in self._BLOCK:
            self.parts.append("\n")
            if tag in {"h1", "h2", "h3", "h4"}:
                self.parts.append("\n## ")
        elif tag == "br":
            self.parts.append("\n")
        elif tag == "li":
            self.parts.append("\n• ")
        elif tag == "tr":
            self.parts.append("\n")
        elif tag in {"td", "th"}:
            self.parts.append(" | ")

    def handle_endtag(self, tag):
        if tag == "script":
            self._in_script = False
        elif tag == "title":
            self._in_title = False
        elif tag in self._SKIP:
            self._skip = max(0, self._skip - 1)
        elif tag in self._BLOCK:
            self.parts.append("\n")

    def handle_data(self, data):
        if self._in_script:
            self.scripts[-1] += data
        elif self._in_title:
            self.title += data
        elif not self._skip:
            self.parts.append(data)


def _tidy(text: str) -> str:
    text = text.replace("\r", "")
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _from_html(data: bytes) -> tuple[str, str]:
    p = _HtmlText()
    p.feed(decode_text(data))
    p.close()
    body = _tidy("".join(p.parts))
    title = _tidy(p.title)
    text = (f"Заголовок страницы: {title}\n\n" if title else "") + body
    note = ""
    scripts = "\n\n".join(s.strip() for s in p.scripts if s.strip())
    # мало видимого текста, а скрипты есть — содержимое, скорее всего, в них
    if scripts and len(body) < 4000:
        text += "\n\n[Текст страницы короткий — ниже содержимое её <script>, там могут быть данные]\n" + scripts
        note = "видимого текста мало, добавлено содержимое скриптов страницы"
    return text, note


def _cell(v) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%d.%m.%Y %H:%M") if (v.hour or v.minute) else v.strftime("%d.%m.%Y")
    if isinstance(v, date):
        return v.strftime("%d.%m.%Y")
    if isinstance(v, float):
        return f"{v:.4f}".rstrip("0").rstrip(".") if v != int(v) else str(int(v))
    return str(v).replace("\n", " ").strip()


def _rows_to_text(name: str, rows) -> tuple[str, int, bool]:
    out, count, cut = [], 0, False
    for row in rows:
        vals = [_cell(v) for v in row]
        while vals and not vals[-1]:
            vals.pop()
        if not any(vals):
            continue
        count += 1
        if count > _SHEET_ROWS_MAX:
            cut = True
            continue
        out.append("\t".join(vals))
    head = f"## Лист «{name}»: {count} непустых строк" + (f" (показаны первые {_SHEET_ROWS_MAX})" if cut else "")
    return head + "\n" + "\n".join(out), count, cut


def _from_xlsx(data: bytes) -> tuple[str, str]:
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    blocks, notes = [], []
    try:
        for ws in wb.worksheets:
            text, _n, cut = _rows_to_text(ws.title, ws.iter_rows(values_only=True))
            blocks.append(text)
            if cut:
                notes.append(f"лист «{ws.title}» обрезан до {_SHEET_ROWS_MAX} строк")
    finally:
        wb.close()
    return "\n\n".join(blocks), "; ".join(notes)


def _from_xls(data: bytes) -> tuple[str, str]:
    import xlrd
    book = xlrd.open_workbook(file_contents=data)
    blocks, notes = [], []
    for sh in book.sheets():
        rows = []
        for r in range(sh.nrows):
            row = []
            for c in range(sh.ncols):
                cell = sh.cell(r, c)
                if cell.ctype == xlrd.XL_CELL_DATE:
                    try:
                        row.append(xlrd.xldate.xldate_as_datetime(cell.value, book.datemode))
                        continue
                    except Exception:
                        pass
                row.append(cell.value if cell.value != "" else None)
            rows.append(row)
        text, _n, cut = _rows_to_text(sh.name, rows)
        blocks.append(text)
        if cut:
            notes.append(f"лист «{sh.name}» обрезан до {_SHEET_ROWS_MAX} строк")
    return "\n\n".join(blocks), "; ".join(notes)


def _from_pdf(data: bytes) -> tuple[str, str]:
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(data))
    pages = []
    for i, page in enumerate(reader.pages, 1):
        try:
            t = _tidy(page.extract_text() or "")
        except Exception:
            t = ""
        if t:
            pages.append(f"--- стр. {i} ---\n{t}")
    if not pages:
        return "", f"в PDF ({len(reader.pages)} стр.) нет текстового слоя — похоже на скан"
    return "\n\n".join(pages), ""


def _zip_xml(zf: zipfile.ZipFile, name: str):
    info = zf.getinfo(name)
    if info.file_size > _ZIP_MEMBER_MAX:
        raise ValueError(f"часть файла {name} слишком большая")
    return ElementTree.fromstring(zf.read(name))


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _from_docx(data: bytes) -> tuple[str, str]:
    zf = zipfile.ZipFile(io.BytesIO(data))
    root = _zip_xml(zf, "word/document.xml")
    lines = []
    for el in root.iter():
        tag = _local(el.tag)
        if tag == "p":
            buf = []
            for sub in el.iter():
                st = _local(sub.tag)
                if st == "t" and sub.text:
                    buf.append(sub.text)
                elif st == "tab":
                    buf.append("\t")
                elif st in {"br", "cr"}:
                    buf.append("\n")
            line = "".join(buf).strip()
            if line:
                lines.append(line)
    return _tidy("\n".join(lines)), ""


def _from_pptx(data: bytes) -> tuple[str, str]:
    zf = zipfile.ZipFile(io.BytesIO(data))
    slides = sorted(
        (n for n in zf.namelist() if re.fullmatch(r"ppt/slides/slide\d+\.xml", n)),
        key=lambda n: int(re.search(r"(\d+)", n.rsplit("/", 1)[-1]).group(1)),
    )
    out = []
    for i, name in enumerate(slides, 1):
        root = _zip_xml(zf, name)
        paras = []
        for p in root.iter():
            if _local(p.tag) == "p":
                t = "".join(s.text or "" for s in p.iter() if _local(s.tag) == "t").strip()
                if t:
                    paras.append(t)
        if paras:
            out.append(f"--- слайд {i} ---\n" + "\n".join(paras))
    return "\n\n".join(out), ""


_READERS = {
    "html": _from_html, "xlsx": _from_xlsx, "xls": _from_xls, "pdf": _from_pdf,
    "docx": _from_docx, "pptx": _from_pptx,
    "text": lambda data: (_tidy(decode_text(data)), ""),
}


def extract(data: bytes, filename: str, mime: str = "") -> dict:
    """{"ok", "kind", "text", "chars", "truncated", "note"} или {"ok": False, "error"}."""
    kind = kind_of(filename, mime)
    if not kind:
        return {"ok": False, "error": f"формат «{filename}» не читаю — поддерживаются: {SUPPORTED_HINT}"}
    if len(data) > MAX_FILE_BYTES:
        return {"ok": False, "error": "файл больше 20 МБ"}
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    # таблицы (Excel, CSV): все строки — в базу вложения, модели — схема и первые строки;
    # текстом тысячи строк не помещались в лимит и не считались (21.10.2026)
    from services import attachment_tables
    if ext in attachment_tables.TABLE_EXTS:
        try:
            tables = attachment_tables.extract_tables(data, filename)
        except Exception:
            tables = []                       # не разобралось таблицей — читаем текстом ниже
        if tables:
            text = attachment_tables.schema_text(tables)
            rows = sum(len(t["rows"]) for t in tables)
            return {"ok": True, "kind": kind, "text": text, "chars": len(text), "truncated": False,
                    "note": f"таблица: {len(tables)} лист(ов), {rows} строк — все доступны через query_attachment",
                    "tables": tables}
    try:
        text, note = _READERS[kind](data)
    except Exception as exc:
        return {"ok": False, "kind": kind, "error": f"не удалось прочитать файл ({type(exc).__name__}: {exc})"}
    text = text or ""
    if not text.strip():
        return {"ok": False, "kind": kind, "error": note or "в файле не нашлось текста"}
    full = len(text)
    truncated = full > MAX_TEXT
    if truncated:
        text = text[:MAX_TEXT]
        note = "; ".join(x for x in (note, f"текст обрезан до {MAX_TEXT} из {full} символов") if x)
    return {"ok": True, "kind": kind, "text": text, "chars": full, "truncated": truncated, "note": note}
