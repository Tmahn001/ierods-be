"""Small helpers shared by routers and services."""
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


def to_camel(snake: str) -> str:
    head, *rest = snake.split("_")
    return head + "".join(part.capitalize() for part in rest)


def _convert(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def row_to_dict(row, rename: dict[str, str] | None = None) -> dict:
    """asyncpg Record -> camelCase dict with JSON-safe values."""
    out = {}
    for key, value in dict(row).items():
        name = (rename or {}).get(key, to_camel(key))
        out[name] = _convert(value)
    return out


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def minutes_between(later: datetime | None, earlier: datetime | None) -> float | None:
    if not later or not earlier:
        return None
    return (later - earlier).total_seconds() / 60
