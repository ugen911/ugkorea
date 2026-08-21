"""Manifest-aware, transactional receiver for legacy 1C export tables.

This module is the tested source for the transitional legacy loader.  It never
selects a database or source path implicitly and never logs business payload.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from uuid import UUID

from sqlalchemy import Connection, Engine, create_engine, text

from .contracts import (
    CONTRACT_VERSIONS_BY_NAME,
    OBSERVED_CONTRACTS_BY_NAME,
    CsvContract,
)
from .normalization import normalize_header, parse_datetime
from .reader import AcceptedRow, iter_rows, read_metadata


class ReceiverError(ValueError):
    """A manifest, source artifact or target schema failed a safe gate."""


class PublicationKind(StrEnum):
    SNAPSHOT = "snapshot"
    PERIOD = "period"


@dataclass(frozen=True, slots=True)
class PeriodScope:
    start: date
    end_exclusive: date


@dataclass(frozen=True, slots=True)
class LegacyContractSpec:
    name: str
    table_name: str
    sql_types: dict[str, str]
    period_header: str | None = None
    period_storage: str | None = None

    @property
    def versions(self) -> tuple[CsvContract, ...]:
        return CONTRACT_VERSIONS_BY_NAME[self.name]

    @property
    def observed_contract(self) -> CsvContract:
        return OBSERVED_CONTRACTS_BY_NAME[self.name]

    @property
    def all_target_columns(self) -> tuple[str, ...]:
        columns: list[str] = []
        for contract in self.versions:
            for header in contract.headers:
                column = normalize_header(header)
                if column not in columns:
                    columns.append(column)
        return tuple(columns)

    def target_column(self, header: str) -> str:
        return normalize_header(header)

    def sql_type(self, column: str) -> str:
        return self.sql_types.get(column, "text")


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    spec: LegacyContractSpec
    contract: CsvContract
    file_name: str
    publication: PublicationKind
    scope: PeriodScope | None
    size_bytes: int
    row_count: int
    sha256: str


@dataclass(frozen=True, slots=True)
class ImportManifest:
    run_id: UUID
    mode: str
    captured_at: datetime
    entries: tuple[ManifestEntry, ...]
    manifest_sha256: str


@dataclass(frozen=True, slots=True)
class ApplyResult:
    run_id: UUID
    status: str
    applied_contracts: int
    applied_rows: int


TABLE_BY_CONTRACT = {
    "service_order_works": "avtoraboty_zakaznarjad",
    "customer_returns": "vozvratyotpokupatelej",
    "customer_orders": "zakazypokupatelej",
    "service_order_executors": "ispolniteli_zakaznarjad",
    "contact_information": "kontaktnajainformatsija",
    "sales_adjustments": "korrektirovka_realizatsii",
    "corrections": "korrektirovki",
    "products": "nomenklatura",
    "product_applicability": "nomenklaturaprimenjaemost",
    "receipts": "postuplenija",
    "sales": "prodazhi",
    "realizations": "realizatsija",
    "service_order_items": "tovary_zakaznarjad",
    "lost_demand": "upuschennyjspros",
    "month_end_prices": "tsenynakonetsmesjatsa",
    "inventory_movements": "registrostatkitovarov",
    "mechanic_output": "vyrabotkaslesarej",
}

TIMESTAMP_COLUMNS = {
    "avtoraboty_zakaznarjad": {"data"},
    "vozvratyotpokupatelej": {"data"},
    "zakazypokupatelej": {"data"},
    "korrektirovka_realizatsii": {"data"},
    "korrektirovki": {"data"},
    "ispolniteli_zakaznarjad": {"data"},
    "nomenklatura": {"rs_datavvoda", "datasozdanija"},
    "postuplenija": {"data"},
    "realizatsija": {"data"},
    "tovary_zakaznarjad": {"data", "datazakrytija"},
    "vyrabotkaslesarej": {"datazakrytija"},
}

BIGINT_COLUMNS = {
    "avtoraboty_zakaznarjad": {"kolichestvo"},
    "vozvratyotpokupatelej": {"kolichestvo"},
    "zakazypokupatelej": {"kolichestvo"},
    "ispolniteli_zakaznarjad": {"nomerstroki", "protsent"},
    "postuplenija": {"kolichestvo"},
    "tovary_zakaznarjad": {"kolichestvo"},
    "upuschennyjspros": {"kolichestvo"},
}

DOUBLE_COLUMNS = {
    "korrektirovka_realizatsii": {"kolichestvo"},
    "korrektirovki": {"kolichestvo"},
}

PERIOD_FIELDS = {
    "service_order_works": ("Дата", "timestamp"),
    "customer_returns": ("Дата", "timestamp"),
    "customer_orders": ("Дата", "timestamp"),
    "sales_adjustments": ("Дата", "timestamp"),
    "corrections": ("Дата", "timestamp"),
    "receipts": ("Дата", "timestamp"),
    "sales": ("Период", "text_date"),
    "realizations": ("Дата", "timestamp"),
    "service_order_items": ("Дата", "timestamp"),
    "service_order_executors": ("Дата", "timestamp"),
    "lost_demand": ("Период", "text_date"),
    "month_end_prices": ("Период", "text_datetime"),
    "inventory_movements": ("Период", "text_datetime"),
    "mechanic_output": ("ДатаЗакрытия", "timestamp"),
}


def _build_specs() -> dict[str, LegacyContractSpec]:
    specs: dict[str, LegacyContractSpec] = {}
    for name, table_name in TABLE_BY_CONTRACT.items():
        sql_types = {
            **{
                column: "timestamp without time zone"
                for column in TIMESTAMP_COLUMNS.get(table_name, set())
            },
            **{column: "bigint" for column in BIGINT_COLUMNS.get(table_name, set())},
            **{column: "double precision" for column in DOUBLE_COLUMNS.get(table_name, set())},
        }
        period = PERIOD_FIELDS.get(name)
        specs[name] = LegacyContractSpec(
            name=name,
            table_name=table_name,
            sql_types=sql_types,
            period_header=None if period is None else period[0],
            period_storage=None if period is None else period[1],
        )
    return specs


LEGACY_CONTRACTS = _build_specs()
_SNAPSHOT_CONTRACTS = frozenset(
    {
        "products",
        "product_applicability",
        "contact_information",
    }
)
_ALLOWED_MODES = frozenset({"fast", "incremental", "reconciliation", "full-recovery"})
_ALLOWED_SQL_TYPES = frozenset(
    {"text", "bigint", "double precision", "timestamp without time zone"}
)
_WRITE_CHUNK_SIZE = 5_000


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(path: Path) -> ImportManifest:
    raw_bytes = path.read_bytes()
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReceiverError("Manifest must be valid UTF-8 JSON") from error
    if not isinstance(payload, dict):
        raise ReceiverError("Manifest root must be an object")
    if payload.get("schema_version") != 1 or payload.get("status") != "ready":
        raise ReceiverError("Manifest must have schema_version=1 and status=ready")
    mode = payload.get("mode")
    if mode not in _ALLOWED_MODES:
        raise ReceiverError("Manifest mode is unsupported")
    try:
        run_id = UUID(str(payload["run_id"]))
        captured_at = datetime.fromisoformat(str(payload["captured_at"]))
    except (KeyError, TypeError, ValueError) as error:
        raise ReceiverError("Manifest run_id or captured_at is invalid") from error
    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise ReceiverError("Manifest captured_at must include an explicit UTC offset")
    raw_entries = payload.get("contracts")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise ReceiverError("Manifest contracts must be a non-empty array")
    entries = tuple(_parse_entry(raw_entry) for raw_entry in raw_entries)
    names = [entry.spec.name for entry in entries]
    if len(names) != len(set(names)):
        raise ReceiverError("Manifest contains a duplicate contract")
    return ImportManifest(
        run_id=run_id,
        mode=str(mode),
        captured_at=captured_at,
        entries=entries,
        manifest_sha256=hashlib.sha256(raw_bytes).hexdigest(),
    )


def _parse_entry(raw_entry: object) -> ManifestEntry:
    if not isinstance(raw_entry, dict):
        raise ReceiverError("Manifest contract entry must be an object")
    name = raw_entry.get("name")
    if not isinstance(name, str) or name not in LEGACY_CONTRACTS:
        raise ReceiverError("Manifest contract name is unsupported")
    spec = LEGACY_CONTRACTS[name]
    try:
        version = int(raw_entry["contract_version"])
        contract = next(contract for contract in spec.versions if contract.version == version)
    except (KeyError, TypeError, ValueError, StopIteration) as error:
        raise ReceiverError(f"Manifest contract version is unsupported for {name}") from error
    file_name = raw_entry.get("file_name")
    if file_name != contract.file_name or Path(str(file_name)).name != file_name:
        raise ReceiverError(f"Manifest file name is invalid for {name}")
    try:
        publication = PublicationKind(str(raw_entry["publication"]))
    except (KeyError, ValueError) as error:
        raise ReceiverError(f"Manifest publication is invalid for {name}") from error
    scope = _parse_scope(raw_entry.get("scope"), name)
    if publication is PublicationKind.PERIOD:
        if spec.period_header is None or scope is None:
            raise ReceiverError(f"Period publication requires a supported scope for {name}")
    elif scope is not None:
        raise ReceiverError(f"Snapshot publication must not declare a scope for {name}")
    if name in _SNAPSHOT_CONTRACTS and publication is not PublicationKind.SNAPSHOT:
        raise ReceiverError(f"Contract {name} currently requires full snapshot publication")
    try:
        size_bytes = int(raw_entry["size_bytes"])
        row_count = int(raw_entry["row_count"])
    except (KeyError, TypeError, ValueError) as error:
        raise ReceiverError(f"Manifest size/count is invalid for {name}") from error
    checksum = raw_entry.get("sha256")
    if (
        size_bytes < 0
        or row_count < 0
        or not isinstance(checksum, str)
        or len(checksum) != 64
        or any(character not in "0123456789abcdef" for character in checksum)
    ):
        raise ReceiverError(f"Manifest artifact metadata is invalid for {name}")
    if (
        publication is PublicationKind.SNAPSHOT
        and name
        in {
            "products",
            "product_applicability",
            "contact_information",
        }
        and row_count == 0
    ):
        raise ReceiverError(f"Core snapshot {name} must not be empty")
    return ManifestEntry(
        spec=spec,
        contract=contract,
        file_name=str(file_name),
        publication=publication,
        scope=scope,
        size_bytes=size_bytes,
        row_count=row_count,
        sha256=checksum,
    )


def _parse_scope(raw_scope: object, name: str) -> PeriodScope | None:
    if raw_scope is None:
        return None
    if not isinstance(raw_scope, dict):
        raise ReceiverError(f"Manifest scope is invalid for {name}")
    try:
        start = date.fromisoformat(str(raw_scope["start"]))
        end_exclusive = date.fromisoformat(str(raw_scope["end_exclusive"]))
    except (KeyError, TypeError, ValueError) as error:
        raise ReceiverError(f"Manifest scope dates are invalid for {name}") from error
    if start >= end_exclusive:
        raise ReceiverError(f"Manifest scope must be non-empty for {name}")
    return PeriodScope(start=start, end_exclusive=end_exclusive)


def resolve_artifact(source_dir: Path, entry: ManifestEntry) -> Path:
    root = source_dir.resolve()
    path = (root / entry.file_name).resolve()
    if path.parent != root:
        raise ReceiverError(f"Artifact escaped the configured source directory: {entry.spec.name}")
    return path


def validate_artifact(source_dir: Path, entry: ManifestEntry) -> None:
    path = resolve_artifact(source_dir, entry)
    if not path.is_file():
        raise ReceiverError(f"Artifact is missing for {entry.spec.name}")
    if path.stat().st_size != entry.size_bytes or sha256_file(path) != entry.sha256:
        raise ReceiverError(f"Artifact fingerprint mismatch for {entry.spec.name}")
    metadata = read_metadata(path)
    if metadata.headers != entry.contract.headers:
        raise ReceiverError(f"Artifact header mismatch for {entry.spec.name}")
    accepted = 0
    for row in iter_rows(path, metadata, entry.contract):
        if not isinstance(row, AcceptedRow):
            raise ReceiverError(
                f"Artifact contains a rejected row for {entry.spec.name}: {row.error_code}"
            )
        _validate_row_scope(entry, row.payload)
        accepted += 1
    if accepted != entry.row_count:
        raise ReceiverError(f"Artifact row count mismatch for {entry.spec.name}")


def _validate_row_scope(entry: ManifestEntry, payload: dict[str, str]) -> None:
    if entry.publication is PublicationKind.SNAPSHOT:
        return
    assert entry.scope is not None
    assert entry.spec.period_header is not None
    parsed = parse_datetime(payload.get(entry.spec.period_header))
    if parsed is None:
        raise ReceiverError(f"Period row has an empty event date for {entry.spec.name}")
    if not entry.scope.start <= parsed.date() < entry.scope.end_exclusive:
        raise ReceiverError(f"Period row is outside declared scope for {entry.spec.name}")


def apply_manifest(
    engine: Engine,
    source_dir: Path,
    manifest: ImportManifest,
) -> ApplyResult:
    for entry in manifest.entries:
        validate_artifact(source_dir, entry)
    with engine.begin() as connection:
        _ensure_metadata_tables(connection)
        existing_sha = connection.execute(
            text(
                "SELECT manifest_sha256 FROM public.one_c_import_run "
                "WHERE run_id = :run_id AND status = 'succeeded'"
            ),
            {"run_id": str(manifest.run_id)},
        ).scalar_one_or_none()
        if existing_sha is not None:
            if existing_sha != manifest.manifest_sha256:
                raise ReceiverError("Run id was already applied with another manifest")
            return ApplyResult(
                run_id=manifest.run_id,
                status="already_applied",
                applied_contracts=0,
                applied_rows=0,
            )
        for entry in manifest.entries:
            _apply_entry(connection, source_dir, entry)
        connection.execute(
            text(
                "INSERT INTO public.one_c_import_run "
                "(run_id, mode, captured_at, manifest_sha256, status, contract_count, row_count) "
                "VALUES (:run_id, :mode, :captured_at, :manifest_sha256, 'succeeded', "
                ":contract_count, :row_count)"
            ),
            {
                "run_id": str(manifest.run_id),
                "mode": manifest.mode,
                "captured_at": manifest.captured_at,
                "manifest_sha256": manifest.manifest_sha256,
                "contract_count": len(manifest.entries),
                "row_count": sum(entry.row_count for entry in manifest.entries),
            },
        )
        for entry in manifest.entries:
            connection.execute(
                text(
                    "INSERT INTO public.one_c_import_contract "
                    "(run_id, contract_name, contract_version, publication, scope_start, "
                    "scope_end_exclusive, source_sha256, source_size_bytes, row_count) "
                    "VALUES (:run_id, :contract_name, :contract_version, :publication, "
                    ":scope_start, :scope_end_exclusive, :source_sha256, "
                    ":source_size_bytes, :row_count)"
                ),
                {
                    "run_id": str(manifest.run_id),
                    "contract_name": entry.spec.name,
                    "contract_version": entry.contract.version,
                    "publication": entry.publication.value,
                    "scope_start": None if entry.scope is None else entry.scope.start,
                    "scope_end_exclusive": (
                        None if entry.scope is None else entry.scope.end_exclusive
                    ),
                    "source_sha256": entry.sha256,
                    "source_size_bytes": entry.size_bytes,
                    "row_count": entry.row_count,
                },
            )
    return ApplyResult(
        run_id=manifest.run_id,
        status="succeeded",
        applied_contracts=len(manifest.entries),
        applied_rows=sum(entry.row_count for entry in manifest.entries),
    )


def _ensure_metadata_tables(connection: Connection) -> None:
    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS public.one_c_import_run (
                run_id uuid PRIMARY KEY,
                mode text NOT NULL,
                captured_at timestamp with time zone NOT NULL,
                manifest_sha256 text NOT NULL,
                status text NOT NULL CHECK (status IN ('succeeded')),
                contract_count integer NOT NULL CHECK (contract_count > 0),
                row_count bigint NOT NULL CHECK (row_count >= 0),
                applied_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS public.one_c_import_contract (
                run_id uuid NOT NULL REFERENCES public.one_c_import_run(run_id),
                contract_name text NOT NULL,
                contract_version integer NOT NULL,
                publication text NOT NULL CHECK (publication IN ('snapshot', 'period')),
                scope_start date,
                scope_end_exclusive date,
                source_sha256 text NOT NULL,
                source_size_bytes bigint NOT NULL,
                row_count bigint NOT NULL,
                PRIMARY KEY (run_id, contract_name),
                CHECK ((publication = 'snapshot'
                        AND scope_start IS NULL
                        AND scope_end_exclusive IS NULL)
                    OR (publication = 'period' AND scope_start IS NOT NULL
                        AND scope_end_exclusive > scope_start))
            )
            """
        )
    )


def _apply_entry(
    connection: Connection,
    source_dir: Path,
    entry: ManifestEntry,
) -> None:
    _ensure_target_table(connection, entry)
    if (
        entry.spec.name == "service_order_executors"
        and entry.publication is PublicationKind.PERIOD
    ):
        bootstrap_exists = bool(
            connection.execute(
                text(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM public.one_c_import_contract AS contract "
                    "JOIN public.one_c_import_run AS run ON run.run_id = contract.run_id "
                    "WHERE contract.contract_name = 'service_order_executors' "
                    "AND contract.contract_version >= 2 "
                    "AND contract.publication = 'snapshot' AND run.status = 'succeeded')"
                )
            ).scalar_one()
        )
        if not bootstrap_exists:
            raise ReceiverError("Executor period publication requires a version 2 snapshot")
    stage_name = f"one_c_stage_{entry.spec.table_name}"
    quoted_stage = _quote_identifier(stage_name)
    connection.execute(text(f"DROP TABLE IF EXISTS pg_temp.{quoted_stage}"))
    stage_columns = tuple(entry.spec.target_column(header) for header in entry.contract.headers)
    stage_definition = ", ".join(f"{_quote_identifier(column)} text" for column in stage_columns)
    connection.execute(
        text(f"CREATE TEMP TABLE {quoted_stage} ({stage_definition}) ON COMMIT DROP")
    )
    _load_stage(connection, source_dir, entry, stage_name, stage_columns)
    path = resolve_artifact(source_dir, entry)
    if path.stat().st_size != entry.size_bytes or sha256_file(path) != entry.sha256:
        raise ReceiverError(f"Artifact changed during staging for {entry.spec.name}")
    _quality_gate_stage(connection, entry, stage_name)
    _delete_scope(connection, entry)
    _insert_stage(connection, entry, stage_name, stage_columns)
    _quality_gate_target_scope(connection, entry)


def _ensure_target_table(connection: Connection, entry: ManifestEntry) -> None:
    spec = entry.spec
    definitions = ", ".join(
        f"{_quote_identifier(column)} {spec.sql_type(column)}" for column in spec.all_target_columns
    )
    connection.execute(
        text(
            f"CREATE TABLE IF NOT EXISTS public.{_quote_identifier(spec.table_name)} "
            f"({definitions})"
        )
    )
    actual_rows = connection.execute(
        text(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :table_name
            """
        ),
        {"table_name": spec.table_name},
    ).all()
    actual = {str(row[0]): str(row[1]) for row in actual_rows}
    missing = sorted(set(spec.all_target_columns) - actual.keys())
    if missing == ["data"] and spec.name == "service_order_executors":
        connection.execute(
            text(
                "ALTER TABLE public.ispolniteli_zakaznarjad "
                "ADD COLUMN data timestamp without time zone"
            )
        )
        actual["data"] = "timestamp without time zone"
    elif missing:
        raise ReceiverError(f"Legacy target is missing columns for {spec.name}")
    mismatches = {
        column: (spec.sql_type(column), actual[column])
        for column in spec.all_target_columns
        if spec.sql_type(column) != actual[column]
    }
    if mismatches == {"kod": ("text", "bigint")} and spec.name == "service_order_works":
        if entry.publication is not PublicationKind.SNAPSHOT:
            raise ReceiverError("Work-code bigint migration requires a full snapshot run")
        connection.execute(
            text(
                "ALTER TABLE public.avtoraboty_zakaznarjad "
                "ALTER COLUMN kod TYPE text USING kod::text"
            )
        )
        return
    if mismatches:
        raise ReceiverError(f"Legacy target type mismatch for {spec.name}")


def _load_stage(
    connection: Connection,
    source_dir: Path,
    entry: ManifestEntry,
    stage_name: str,
    stage_columns: tuple[str, ...],
) -> None:
    quoted_columns = ", ".join(_quote_identifier(column) for column in stage_columns)
    parameters = ", ".join(f":{column}" for column in stage_columns)
    statement = text(
        f"INSERT INTO {_quote_identifier(stage_name)} ({quoted_columns}) VALUES ({parameters})"
    )
    chunk: list[dict[str, str | None]] = []
    for payload in _accepted_payloads(source_dir, entry):
        chunk.append(
            {
                entry.spec.target_column(header): _stage_value(
                    payload[header],
                    entry.spec.sql_type(entry.spec.target_column(header)),
                )
                for header in entry.contract.headers
            }
        )
        if len(chunk) >= _WRITE_CHUNK_SIZE:
            connection.execute(statement, chunk)
            chunk.clear()
    if chunk:
        connection.execute(statement, chunk)


def _accepted_payloads(
    source_dir: Path,
    entry: ManifestEntry,
) -> Iterator[dict[str, str]]:
    path = resolve_artifact(source_dir, entry)
    metadata = read_metadata(path)
    for row in iter_rows(path, metadata, entry.contract):
        if not isinstance(row, AcceptedRow):
            raise ReceiverError(f"Artifact changed after validation for {entry.spec.name}")
        yield row.payload


def _stage_value(raw_value: str, sql_type: str) -> str | None:
    value = raw_value.strip()
    if not value:
        return None
    if sql_type == "timestamp without time zone":
        parsed = parse_datetime(value)
        if parsed is None:
            return None
        return parsed.isoformat(sep=" ")
    if sql_type == "bigint":
        try:
            return str(int(value))
        except ValueError as error:
            raise ReceiverError("Source integer value is invalid") from error
    if sql_type == "double precision":
        normalized = value.replace(",", ".")
        try:
            float(normalized)
        except ValueError as error:
            raise ReceiverError("Source decimal value is invalid") from error
        return normalized
    return value


def _quality_gate_stage(
    connection: Connection,
    entry: ManifestEntry,
    stage_name: str,
) -> None:
    stage_count = int(
        connection.execute(
            text(f"SELECT count(*) FROM {_quote_identifier(stage_name)}")
        ).scalar_one()
    )
    if stage_count != entry.row_count:
        raise ReceiverError(f"Staging row count mismatch for {entry.spec.name}")
    if entry.spec.name == "products":
        duplicate_or_empty = int(
            connection.execute(
                text(
                    f"SELECT count(*) - count(DISTINCT kod) + count(*) FILTER "
                    f"(WHERE kod IS NULL) FROM {_quote_identifier(stage_name)}"
                )
            ).scalar_one()
        )
        if duplicate_or_empty:
            raise ReceiverError("Products staging contains an empty or duplicate kod")
    if entry.spec.name == "product_applicability":
        invalid = int(
            connection.execute(
                text(
                    f"SELECT count(*) - count(DISTINCT (kod, model)) + count(*) FILTER "
                    f"(WHERE kod IS NULL OR model IS NULL) "
                    f"FROM {_quote_identifier(stage_name)}"
                )
            ).scalar_one()
        )
        if invalid:
            raise ReceiverError("Applicability staging contains an invalid key")


def _delete_scope(connection: Connection, entry: ManifestEntry) -> None:
    table = f"public.{_quote_identifier(entry.spec.table_name)}"
    if entry.publication is PublicationKind.SNAPSHOT:
        connection.execute(text(f"DELETE FROM {table}"))
        return
    assert entry.scope is not None
    assert entry.spec.period_header is not None
    column = _quote_identifier(entry.spec.target_column(entry.spec.period_header))
    if entry.spec.period_storage == "timestamp":
        expression = column
    elif entry.spec.period_storage == "text_date":
        expression = f"to_date(NULLIF({column}, ''), 'DD.MM.YYYY')"
    elif entry.spec.period_storage == "text_datetime":
        expression = f"to_timestamp(NULLIF({column}, ''), 'DD.MM.YYYY HH24:MI:SS')"
    else:
        raise ReceiverError(f"Period storage is unsupported for {entry.spec.name}")
    connection.execute(
        text(
            f"DELETE FROM {table} WHERE {expression} >= :scope_start AND {expression} < :scope_end"
        ),
        {"scope_start": entry.scope.start, "scope_end": entry.scope.end_exclusive},
    )


def _insert_stage(
    connection: Connection,
    entry: ManifestEntry,
    stage_name: str,
    stage_columns: tuple[str, ...],
) -> None:
    columns = ", ".join(_quote_identifier(column) for column in stage_columns)
    expressions = ", ".join(
        _cast_stage_column(column, entry.spec.sql_type(column)) for column in stage_columns
    )
    connection.execute(
        text(
            f"INSERT INTO public.{_quote_identifier(entry.spec.table_name)} ({columns}) "
            f"SELECT {expressions} FROM {_quote_identifier(stage_name)}"
        )
    )


def _cast_stage_column(column: str, sql_type: str) -> str:
    if sql_type not in _ALLOWED_SQL_TYPES:
        raise ReceiverError("Unsupported target SQL type")
    quoted = _quote_identifier(column)
    if sql_type == "text":
        return quoted
    return f"{quoted}::{sql_type}"


def _quality_gate_target_scope(connection: Connection, entry: ManifestEntry) -> None:
    table = f"public.{_quote_identifier(entry.spec.table_name)}"
    if entry.publication is PublicationKind.SNAPSHOT:
        target_count = int(connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one())
    else:
        assert entry.scope is not None
        assert entry.spec.period_header is not None
        column = _quote_identifier(entry.spec.target_column(entry.spec.period_header))
        if entry.spec.period_storage == "timestamp":
            expression = column
        elif entry.spec.period_storage == "text_date":
            expression = f"to_date(NULLIF({column}, ''), 'DD.MM.YYYY')"
        else:
            expression = f"to_timestamp(NULLIF({column}, ''), 'DD.MM.YYYY HH24:MI:SS')"
        target_count = int(
            connection.execute(
                text(
                    f"SELECT count(*) FROM {table} WHERE {expression} >= :scope_start "
                    f"AND {expression} < :scope_end"
                ),
                {"scope_start": entry.scope.start, "scope_end": entry.scope.end_exclusive},
            ).scalar_one()
        )
    if target_count != entry.row_count:
        raise ReceiverError(f"Published row count mismatch for {entry.spec.name}")


def _quote_identifier(value: str) -> str:
    if not value or not value.replace("_", "").isalnum() or not value.isascii():
        raise ReceiverError("Unsafe SQL identifier")
    return f'"{value}"'


def run_from_paths(
    *,
    database_url: str,
    source_dir: Path,
    manifest_path: Path,
) -> ApplyResult:
    if not database_url:
        raise ReceiverError("Database URL must be supplied explicitly")
    manifest = load_manifest(manifest_path)
    engine = create_engine(database_url, future=True)
    try:
        return apply_manifest(engine, source_dir, manifest)
    finally:
        engine.dispose()
