"""Versioned, strict normalization rules shared by 1C import adapters."""

from __future__ import annotations

import math
import re
import unicodedata
from collections.abc import Iterable
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from uuid import UUID, uuid5

NORMALIZATION_VERSION = 1
ANALOG_GROUP_NAMESPACE = UUID("30e73c7c-fc6a-5a8d-93c4-08fe16a0eae4")

_HEADER_TRANSLITERATION = str.maketrans(
    {
        "а": "a",
        "б": "b",
        "в": "v",
        "г": "g",
        "д": "d",
        "е": "e",
        "ё": "e",
        "ж": "zh",
        "з": "z",
        "и": "i",
        "й": "j",
        "к": "k",
        "л": "l",
        "м": "m",
        "н": "n",
        "о": "o",
        "п": "p",
        "р": "r",
        "с": "s",
        "т": "t",
        "у": "u",
        "ф": "f",
        "х": "h",
        "ц": "ts",
        "ч": "ch",
        "ш": "sh",
        "щ": "sch",
        "ъ": "",
        "ы": "y",
        "ь": "",
        "э": "e",
        "ю": "ju",
        "я": "ja",
    }
)

_TRUE_VALUES = frozenset({"1", "true", "yes", "y", "да", "истина"})
_FALSE_VALUES = frozenset({"0", "false", "no", "n", "нет", "ложь"})
_DATE_FORMATS = (
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M",
    "%d.%m.%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)


class NormalizationError(ValueError):
    """A value cannot be normalized without guessing or losing information."""

    def __init__(self, error_code: str, message: str) -> None:
        super().__init__(message)
        self.error_code = error_code


def normalize_text(value: object) -> str | None:
    """Normalize Unicode and trim edges while preserving meaningful inner spaces."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = unicodedata.normalize("NFKC", str(value))
    text = "".join(
        character
        for character in text
        if character != "\x00" and unicodedata.category(character) not in {"Cf", "Cc"}
    ).strip()
    return text or None


def normalize_code(value: object) -> str | None:
    """Normalize an external 1C code without changing its raw ingestion value."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and math.isfinite(value) and value.is_integer():
        return str(int(value))
    if isinstance(value, Decimal) and value.is_finite() and value == value.to_integral():
        return str(value.to_integral())
    text = normalize_text(value)
    if text is None:
        return None
    compact = "".join(character for character in text if not character.isspace())
    return compact.upper() or None


def normalize_header(value: object) -> str:
    """Create deterministic ASCII snake_case for matching known aliases."""
    text = normalize_text(value)
    if text is None:
        raise NormalizationError("EMPTY_HEADER", "Workbook contains an empty header")
    transliterated = text.casefold().translate(_HEADER_TRANSLITERATION)
    normalized = re.sub(r"[^a-z0-9]+", "_", transliterated).strip("_")
    if not normalized:
        raise NormalizationError("EMPTY_HEADER", "Header has no supported characters")
    return normalized


def parse_decimal(value: object) -> Decimal | None:
    """Parse a Russian/Excel decimal exactly; never replace invalid input with zero."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, bool):
        raise NormalizationError("INVALID_DECIMAL", "Boolean is not a decimal")
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, (int, float)):
        parsed = Decimal(str(value))
    else:
        text = normalize_text(value)
        if text is None:
            return None
        compact = re.sub(r"[\s\u00a0\u202f]", "", text)
        if compact.count(",") > 1 or compact.count(".") > 1:
            raise NormalizationError("AMBIGUOUS_DECIMAL", "Too many decimal separators")
        if "," in compact and "." in compact:
            if compact.rfind(",") > compact.rfind("."):
                compact = compact.replace(".", "").replace(",", ".")
            else:
                compact = compact.replace(",", "")
        else:
            compact = compact.replace(",", ".")
        try:
            parsed = Decimal(compact)
        except InvalidOperation as error:
            raise NormalizationError("INVALID_DECIMAL", "Value is not a decimal") from error
    if not parsed.is_finite():
        raise NormalizationError("NON_FINITE_DECIMAL", "Decimal must be finite")
    return parsed


def parse_boolean(value: object) -> bool | None:
    """Parse only an explicit boolean vocabulary."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, (float, Decimal)) and value in {0, 1}:
        return bool(value)
    text = normalize_text(value)
    if text is None:
        return None
    folded = text.casefold()
    if folded in _TRUE_VALUES:
        return True
    if folded in _FALSE_VALUES:
        return False
    raise NormalizationError("INVALID_BOOLEAN", "Value is not an allowed boolean")


def parse_date(value: object, *, excel_datemode: int | None = None) -> date | None:
    """Parse declared date formats, including an explicitly configured Excel epoch."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float, Decimal)):
        if excel_datemode not in {0, 1}:
            raise NormalizationError("EXCEL_DATE_MODE_REQUIRED", "Excel date epoch is unknown")
        base = date(1899, 12, 30) if excel_datemode == 0 else date(1904, 1, 1)
        return base + timedelta(days=int(value))
    text = normalize_text(value)
    if text is None:
        return None
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        pass
    for date_format in _DATE_FORMATS:
        try:
            return datetime.strptime(text, date_format).date()
        except ValueError:
            continue
    raise NormalizationError("INVALID_DATE", "Value does not match an allowed date format")


def parse_datetime(value: object, *, excel_datemode: int | None = None) -> datetime | None:
    """Parse a local 1C timestamp without inventing a timezone or discarding time."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, (int, float, Decimal)):
        if excel_datemode not in {0, 1}:
            raise NormalizationError("EXCEL_DATE_MODE_REQUIRED", "Excel date epoch is unknown")
        base = datetime(1899, 12, 30) if excel_datemode == 0 else datetime(1904, 1, 1)
        return base + timedelta(days=float(value))
    text = normalize_text(value)
    if text is None:
        return None
    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except ValueError:
        pass
    for date_format in _DATE_FORMATS:
        try:
            return datetime.strptime(text, date_format)
        except ValueError:
            continue
    raise NormalizationError("INVALID_DATETIME", "Value does not match an allowed datetime format")


def stable_analog_groups(edges: Iterable[tuple[str, str]]) -> dict[str, UUID]:
    """Return deterministic connected-component IDs for undirected analog edges."""
    graph: dict[str, set[str]] = {}
    for raw_left, raw_right in edges:
        left = normalize_code(raw_left)
        right = normalize_code(raw_right)
        if left is None or right is None:
            raise NormalizationError("EMPTY_ANALOG_CODE", "Analog edge has an empty code")
        if left == right:
            raise NormalizationError("ANALOG_SELF_REFERENCE", "Analog edge is a self-reference")
        graph.setdefault(left, set()).add(right)
        graph.setdefault(right, set()).add(left)

    result: dict[str, UUID] = {}
    visited: set[str] = set()
    for start in sorted(graph):
        if start in visited:
            continue
        component: set[str] = set()
        pending = [start]
        while pending:
            code = pending.pop()
            if code in visited:
                continue
            visited.add(code)
            component.add(code)
            pending.extend(sorted(graph[code] - visited, reverse=True))
        fingerprint = f"v{NORMALIZATION_VERSION}:" + "\x1f".join(sorted(component))
        group_id = uuid5(ANALOG_GROUP_NAMESPACE, fingerprint)
        result.update({code: group_id for code in component})
    return result
