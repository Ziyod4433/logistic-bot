from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Any


def export_csv(filename_prefix: str, rows: list[dict[str, Any]]) -> tuple[str, str]:
    if not rows:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["No data"])
        return f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", output.getvalue()

    columns = list(rows[0].keys())
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _serialize_value(row.get(key)) for key in columns})
    return f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", output.getvalue()


def export_xlsx(filename_prefix: str, rows: list[dict[str, Any]]) -> tuple[str, bytes]:
    try:
        from openpyxl import Workbook
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"Excel eksport uchun openpyxl kerak: {exc}")

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Report"
    if not rows:
        sheet.append(["No data"])
    else:
        columns = list(rows[0].keys())
        sheet.append(columns)
        for row in rows:
            sheet.append([_serialize_value(row.get(column)) for column in columns])

    buffer = io.BytesIO()
    workbook.save(buffer)
    return f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", buffer.getvalue()


def _serialize_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join(str(item) for item in value)
    if isinstance(value, dict):
        return "; ".join(f"{key}: {val}" for key, val in value.items())
    return value
