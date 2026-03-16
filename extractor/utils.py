"""Shared utility helpers used across modules."""

from __future__ import annotations

import re
import unicodedata


def canonicalize(text: str) -> str:
    """Normalize text for accent-insensitive matching."""

    normalized = unicodedata.normalize("NFKD", text)
    without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return " ".join(without_accents.lower().strip().split())


def canonicalize_column_name(display_name: str) -> str:
    """Convert Jira display names into deterministic record/SQL-safe keys."""

    with_word_boundaries = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", display_name)
    normalized = canonicalize(with_word_boundaries)
    safe = re.sub(r"[^a-z0-9]+", "_", normalized)
    return safe.strip("_")
