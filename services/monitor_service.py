from __future__ import annotations

from typing import Any

from services import analytics_service


def get_monitor_payload(args: Any) -> dict:
    return analytics_service.get_monitor(args)
