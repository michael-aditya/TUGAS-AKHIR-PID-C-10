from __future__ import annotations

from datetime import datetime, timezone


def iso_utc(dt: datetime) -> str:
    """Format a datetime as canonical ISO-8601 UTC string (YYYY-MM-DDTHH:MM:SSZ)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
