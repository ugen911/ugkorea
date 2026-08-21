"""Streaming CSV reader with explicit encoding and width validation."""

from __future__ import annotations

import codecs
import csv
import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from .contracts import CsvContract, CsvRowRepair

UTF8_BOM = codecs.BOM_UTF8


class ContractMismatchError(ValueError):
    """Raised before row loading when the export header changed."""


@dataclass(frozen=True, slots=True)
class CsvMetadata:
    encoding: str
    headers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AcceptedRow:
    row_number: int
    row_hash: str
    payload: dict[str, str]


@dataclass(frozen=True, slots=True)
class RejectedRow:
    row_number: int
    error_code: str
    safe_message: str


CsvRowResult = AcceptedRow | RejectedRow


def detect_encoding(path: Path, sample_size: int = 65536) -> str:
    """Detect UTF-8/UTF-8 BOM first and use cp1251 only for invalid UTF-8."""
    with path.open("rb") as source:
        sample = source.read(sample_size)
    if sample.startswith(UTF8_BOM):
        return "utf-8-sig"
    decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
    try:
        decoder.decode(sample, final=False)
    except UnicodeDecodeError:
        return "cp1251"
    return "utf-8"


def normalize_header(value: str) -> str:
    return value.removeprefix("\ufeff").strip()


def read_metadata(path: Path) -> CsvMetadata:
    encoding = detect_encoding(path)
    with path.open("r", encoding=encoding, newline="") as source:
        reader = csv.reader(source, delimiter=";")
        try:
            raw_headers = next(reader)
        except StopIteration as error:
            raise ContractMismatchError("CSV file is empty") from error
    return CsvMetadata(
        encoding=encoding,
        headers=tuple(normalize_header(header) for header in raw_headers),
    )


def validate_contract(metadata: CsvMetadata, contract: CsvContract) -> None:
    if metadata.headers != contract.headers:
        raise ContractMismatchError(
            f"Header mismatch for contract {contract.name}; expected "
            f"{len(contract.headers)} columns, received {len(metadata.headers)}"
        )


def iter_rows(path: Path, metadata: CsvMetadata, contract: CsvContract) -> Iterator[CsvRowResult]:
    """Yield validated rows without loading the complete file into memory."""
    validate_contract(metadata, contract)
    with path.open("r", encoding=metadata.encoding, newline="") as source:
        reader = csv.reader(source, delimiter=";")
        next(reader)
        pending_multiline: list[tuple[int, list[str]]] = []
        for row in reader:
            row_number = reader.line_num

            if contract.row_repair is CsvRowRepair.TRAILING_DELIMITERS:
                if len(row) > len(metadata.headers):
                    row = row[: len(metadata.headers) - 1] + [
                        ";".join(row[len(metadata.headers) - 1 :])
                    ]
                yield _row_result(row_number, row, metadata)
                continue

            if contract.row_repair is CsvRowRepair.UNQUOTED_MULTILINE_FIELD:
                field_index = contract.repair_field_index
                assert field_index is not None
                if pending_multiline:
                    expected_tail_width = len(metadata.headers) - field_index
                    if not row:
                        pending_multiline.append((row_number, row))
                        continue
                    if len(row) == expected_tail_width:
                        _, start = pending_multiline[0]
                        field_fragments = [start[-1]]
                        field_fragments.extend("" for _ in pending_multiline[1:])
                        field_fragments.append(row[0])
                        repaired = start[:-1] + ["\n".join(field_fragments)] + row[1:]
                        yield _row_result(row_number, repaired, metadata)
                        pending_multiline.clear()
                        continue
                    for buffered_number, buffered_row in pending_multiline:
                        yield _row_result(buffered_number, buffered_row, metadata)
                    pending_multiline.clear()

                if len(row) == field_index + 1:
                    pending_multiline.append((row_number, row))
                    continue

            yield _row_result(row_number, row, metadata)

        for buffered_number, buffered_row in pending_multiline:
            yield _row_result(buffered_number, buffered_row, metadata)


def _row_result(row_number: int, row: list[str], metadata: CsvMetadata) -> CsvRowResult:
    if len(row) != len(metadata.headers):
        return RejectedRow(
            row_number=row_number,
            error_code="CSV_COLUMN_COUNT",
            safe_message=(f"Expected {len(metadata.headers)} columns, received {len(row)}"),
        )
    if any("\x00" in value for value in row):
        return RejectedRow(
            row_number=row_number,
            error_code="CSV_NULL_BYTE",
            safe_message="Row contains a NUL byte",
        )
    payload = dict(zip(metadata.headers, row, strict=True))
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return AcceptedRow(
        row_number=row_number,
        row_hash=hashlib.sha256(canonical).hexdigest(),
        payload=payload,
    )
