"""Structured audit event writer."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from extractor.interfaces import Auditor


class JsonlAuditor(Auditor):
    """Appends execution events to daily JSONL audit files."""

    def __init__(self, root: Path) -> None:
        self._audit_dir = root / "audit"
        self._audit_dir.mkdir(parents=True, exist_ok=True)

    def write_event(self, event: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        path = self._audit_dir / f"extraction_audit_{now.date().isoformat()}.jsonl"
        payload = {"recorded_at": now.isoformat(), **event}
        with path.open("a", encoding="utf-8") as handler:
            handler.write(json.dumps(payload, ensure_ascii=False) + "\n")
