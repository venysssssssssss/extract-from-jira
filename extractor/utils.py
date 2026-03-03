"""Shared utility helpers used across modules."""

from __future__ import annotations

import unicodedata


def canonicalize(text: str) -> str:
    """Normalize text for accent-insensitive matching."""

    normalized = unicodedata.normalize("NFKD", text)
    without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return " ".join(without_accents.lower().strip().split())
