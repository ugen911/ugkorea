"""Update legacy analytical tables from fixed-path 1C CSV exports."""

from __future__ import annotations

import logging
import os
import re
import sys
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from transliterate import translit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_PARENT = PROJECT_ROOT.parent
if str(PROJECT_PARENT) not in sys.path:
    sys.path.insert(0, str(PROJECT_PARENT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ugkorea.db.database import get_db_engine

from reglament_task.one_c_receiver_runtime.contracts import (
    OBSERVED_CONTRACTS,
    PRUNED_HEADERS_BY_NAME,
)

DEFAULT_LOCAL_EXPORT_DIR = Path(
    r"D:\NAS\заказы\Евгений\Access\Табличные выгрузки1С"
)
DEFAULT_NETWORK_EXPORT_DIR = Path(
    r"\\26.218.196.12\заказы\Евгений\Access\Табличные выгрузки1С"
)
LOG_PATH = PROJECT_ROOT / "data_upload_errors.log"
SIMPLE_SNAPSHOT_CONTRACTS = tuple(
    (
        contract.name,
        contract.file_name,
        PRUNED_HEADERS_BY_NAME.get(contract.name, contract.headers),
    )
    for contract in OBSERVED_CONTRACTS
)
SNAPSHOT_CONTRACTS = {
    "products",
    "product_applicability",
    "contact_information",
}
PERIOD_FIELD_BY_CONTRACT = {
    "service_order_works": "Дата",
    "customer_returns": "Дата",
    "customer_orders": "Дата",
    "sales_adjustments": "Дата",
    "corrections": "Дата",
    "receipts": "Дата",
    "sales": "Период",
    "realizations": "Дата",
    "service_order_items": "Дата",
    "lost_demand": "Период",
    "month_end_prices": "Период",
    "inventory_movements": "Период",
    "mechanic_output": "ДатаЗакрытия",
}
TEXT_PERIOD_CONTRACTS = {
    "sales",
    "lost_demand",
    "month_end_prices",
    "inventory_movements",
}
# A transaction-scoped PostgreSQL advisory lock prevents two scheduler/manual
# receiver runs from deleting the same old window and then both appending it.
RECEIVER_ADVISORY_LOCK_KEY = 1_837_436_591_001
SUPPORTED_CONTRACTS = {contract.name for contract in OBSERVED_CONTRACTS}
if (
    SNAPSHOT_CONTRACTS
    | set(PERIOD_FIELD_BY_CONTRACT)
    | {"service_order_executors"}
) != SUPPORTED_CONTRACTS:
    raise RuntimeError("Simple 1C rolling-window contract classification is incomplete")
if not TEXT_PERIOD_CONTRACTS <= set(PERIOD_FIELD_BY_CONTRACT):
    raise RuntimeError("Text period contracts must be classified as period contracts")

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
)


def to_snake_case(value: str) -> str:
    value = translit(value, "ru", reversed=True)
    return (
        value.lower()
        .replace(" ", "_")
        .replace(".", "_")
        .replace('"', "")
        .replace("'", "")
        .replace(",", "")
        .replace(";", "")
        .replace("!", "")
        .replace("?", "")
    )


def export_directory() -> Path:
    configured = os.getenv("UGKOREA_ONE_C_EXPORT_DIR")
    if configured:
        return Path(configured)
    if DEFAULT_LOCAL_EXPORT_DIR.is_dir():
        return DEFAULT_LOCAL_EXPORT_DIR
    return DEFAULT_NETWORK_EXPORT_DIR


def table_name(file_name: str) -> str:
    stem = Path(file_name).stem.replace("Выгрузка_", "").replace("Выгрузка", "")
    return to_snake_case(stem)


def prepare_exports(
    source_dir: Path,
    *,
    cutoff: datetime,
    window_end: datetime,
) -> list[tuple[str, str, pd.DataFrame, bool]]:
    if not source_dir.is_dir():
        raise FileNotFoundError(f"1C export directory does not exist: {source_dir}")

    missing = [
        file_name
        for _, file_name, _ in SIMPLE_SNAPSHOT_CONTRACTS
        if not (source_dir / file_name).is_file()
    ]
    if missing:
        raise FileNotFoundError("Missing required 1C CSV files: " + ", ".join(missing))

    prepared = []
    total_files = len(SIMPLE_SNAPSHOT_CONTRACTS)
    for position, (contract_name, file_name, expected_headers) in enumerate(
        SIMPLE_SNAPSHOT_CONTRACTS,
        start=1,
    ):
        print(f"[Чтение {position}/{total_files}] {file_name}...", flush=True)
        file_path = source_dir / file_name
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            data = pd.read_csv(file_path, sep=";", on_bad_lines="warn")
        for warning in caught:
            logging.warning("CSV parser warning for %s: %s", file_name, warning.message)

        source_headers = tuple(
            str(column).lstrip("\ufeff").strip() for column in data.columns
        )
        if source_headers != expected_headers:
            raise ValueError(
                f"Unexpected header for {file_name}: "
                f"expected {expected_headers!r}, got {source_headers!r}"
            )
        data.columns = [to_snake_case(column) for column in source_headers]
        data = data.apply(
            lambda column: column.map(
                lambda value: value.strip() if isinstance(value, str) else value
            )
        )

        for column in [name for name in data.columns if "data" in name]:
            data[column] = pd.to_datetime(data[column], errors="coerce", dayfirst=True)

        if contract_name in PERIOD_FIELD_BY_CONTRACT:
            period_column = to_snake_case(PERIOD_FIELD_BY_CONTRACT[contract_name])
            parsed_period = pd.to_datetime(
                data[period_column], errors="coerce", dayfirst=True
            )
            invalid_period_count = int(parsed_period.isna().sum())
            if invalid_period_count:
                raise ValueError(
                    f"{file_name} contains {invalid_period_count} rows "
                    "without a valid period"
                )
            outside_scope_count = int(
                ((parsed_period < cutoff) | (parsed_period >= window_end)).sum()
            )
            if outside_scope_count:
                raise ValueError(
                    f"{file_name} contains {outside_scope_count} rows outside "
                    f"the rolling window [{cutoff:%Y-%m-%d}, {window_end:%Y-%m-%d})"
                )

        destination = table_name(file_name)
        use_index = destination in {"nomenklaturaprimenjaemost", "nomenklatura"}
        if use_index:
            if "kod" not in data.columns:
                raise ValueError(f"Required kod column is missing in {file_name}")
            data.set_index("kod", inplace=True)
        prepared.append((contract_name, destination, data, use_index))
        print(
            f"[Подготовлено {position}/{total_files}] {file_name}: "
            f"{len(data.index)} строк -> таблица '{destination}'.",
            flush=True,
        )

    frames = {contract_name: data for contract_name, _, data, _ in prepared}
    work_refs = set(frames["service_order_works"]["ssylka"].dropna())
    executor_refs = set(frames["service_order_executors"]["ssylka"].dropna())
    orphan_executor_ref_count = len(executor_refs - work_refs)
    if orphan_executor_ref_count:
        raise ValueError(
            "Service-order executor export contains "
            f"{orphan_executor_ref_count} references outside the rolling works export"
        )
    return prepared


def rolling_bounds(reference: datetime | None = None) -> tuple[datetime, datetime]:
    reference = reference or datetime.now()
    cutoff = datetime(reference.year - 1, reference.month, 1)
    window_end = datetime(reference.year, reference.month, reference.day) + timedelta(days=1)
    return cutoff, window_end


def quoted_identifier(value: str) -> str:
    if re.fullmatch(r"[a-z0-9_]+", value) is None:
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return f'"{value}"'


def text_period_expression(column: str) -> str:
    quoted = quoted_identifier(column)
    trimmed = f"BTRIM(REPLACE({quoted}, CHR(160), ' '))"
    return (
        "CASE "
        f"WHEN {trimmed} ~ '^[0-9]{{2}}[.][0-9]{{2}}[.][0-9]{{4}} "
        f"[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$' THEN to_timestamp({trimmed}, "
        "'DD.MM.YYYY HH24:MI:SS') "
        f"WHEN {trimmed} ~ '^[0-9]{{2}}[.][0-9]{{2}}[.][0-9]{{4}}$' "
        f"THEN to_timestamp({trimmed}, 'DD.MM.YYYY') "
        f"WHEN {trimmed} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' "
        f"THEN {trimmed}::timestamp "
        "ELSE NULL END"
    )


def table_row_count(
    connection: object,
    destination: str,
    *,
    where_sql: str | None = None,
    parameters: dict[str, object] | None = None,
) -> int:
    query = f"SELECT COUNT(*) FROM {quoted_identifier(destination)}"
    if where_sql is not None:
        query += f" WHERE {where_sql}"
    return int(connection.execute(text(query), parameters or {}).scalar_one())


def require_table_row_count(
    connection: object,
    destination: str,
    expected: int,
    *,
    phase: str,
    where_sql: str | None = None,
    parameters: dict[str, object] | None = None,
) -> None:
    actual = table_row_count(
        connection,
        destination,
        where_sql=where_sql,
        parameters=parameters,
    )
    if actual != expected:
        raise RuntimeError(
            f"Row-count verification failed for table '{destination}' during "
            f"{phase}: expected {expected}, got {actual}"
        )
    print(
        f"[Проверка] Таблица '{destination}', {phase}: {actual} строк.",
        flush=True,
    )


def update_exports(
    prepared: list[tuple[str, str, pd.DataFrame, bool]],
    engine: object,
    *,
    cutoff: datetime,
    window_end: datetime,
) -> None:
    destinations = {name: destination for name, destination, _, _ in prepared}
    source_files = {
        contract_name: file_name
        for contract_name, file_name, _ in SIMPLE_SNAPSHOT_CONTRACTS
    }
    total_tables = len(prepared)
    window_parameters = {"cutoff": cutoff, "window_end": window_end}
    print("Подготовка транзакции обновления таблиц...", flush=True)
    with engine.begin() as connection:
        print(
            "[Блокировка] Проверка отсутствия другого обновления 1С...",
            flush=True,
        )
        lock_acquired = bool(
            connection.execute(
                text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
                {"lock_key": RECEIVER_ADVISORY_LOCK_KEY},
            ).scalar_one()
        )
        if not lock_acquired:
            raise RuntimeError(
                "Another 1C analytical-table update is already running; "
                "this run was stopped before changing any table"
            )
        print("[Блокировка] Получена.", flush=True)

        period_scopes = {}
        period_preserved_rows = {}
        for contract_name, destination, _, _ in prepared:
            if contract_name in SNAPSHOT_CONTRACTS or contract_name == "service_order_executors":
                continue
            column = to_snake_case(PERIOD_FIELD_BY_CONTRACT[contract_name])
            expression = (
                text_period_expression(column)
                if contract_name in TEXT_PERIOD_CONTRACTS
                else quoted_identifier(column)
            )
            invalid_count = table_row_count(
                connection,
                destination,
                where_sql=f"{expression} IS NULL",
            )
            if invalid_count:
                raise RuntimeError(
                    f"Table '{destination}' contains {invalid_count} rows "
                    f"without a parseable {column}; update was stopped"
                )
            scope = f"{expression} >= :cutoff AND {expression} < :window_end"
            period_scopes[contract_name] = scope
            total_before = table_row_count(connection, destination)
            scope_before = table_row_count(
                connection,
                destination,
                where_sql=scope,
                parameters=window_parameters,
            )
            period_preserved_rows[contract_name] = total_before - scope_before
            print(
                f"[Классификация] Таблица '{destination}': "
                f"{scope_before} строк окна, "
                f"{period_preserved_rows[contract_name]} строк истории.",
                flush=True,
            )

        for contract_name in sorted(SNAPSHOT_CONTRACTS):
            result = connection.execute(
                text(f"DELETE FROM {quoted_identifier(destinations[contract_name])}")
            )
            print(
                f"[Очистка] Таблица '{destinations[contract_name]}': "
                f"удалено {result.rowcount} строк.",
                flush=True,
            )
            require_table_row_count(
                connection,
                destinations[contract_name],
                0,
                phase="после очистки снимка",
            )

        executor_table = quoted_identifier(destinations["service_order_executors"])
        works_table = quoted_identifier(destinations["service_order_works"])
        executor_scope = (
            "ssylka IN "
            f"(SELECT DISTINCT ssylka FROM {works_table} "
            "WHERE data >= :cutoff AND data < :window_end)"
        )
        executor_total_before = table_row_count(
            connection,
            destinations["service_order_executors"],
        )
        executor_scope_before = table_row_count(
            connection,
            destinations["service_order_executors"],
            where_sql=executor_scope,
            parameters=window_parameters,
        )
        executor_preserved_rows = executor_total_before - executor_scope_before
        result = connection.execute(
            text(
                f"DELETE FROM {executor_table} WHERE {executor_scope}"
            ),
            window_parameters,
        )
        print(
            f"[Очистка] Таблица '{destinations['service_order_executors']}': "
            f"удалено {result.rowcount} строк текущего окна.",
            flush=True,
        )
        require_table_row_count(
            connection,
            destinations["service_order_executors"],
            0,
            phase="после очистки окна",
            where_sql=executor_scope,
            parameters=window_parameters,
        )

        for contract_name, destination, _, _ in prepared:
            if contract_name in SNAPSHOT_CONTRACTS or contract_name == "service_order_executors":
                continue
            scope = period_scopes[contract_name]
            result = connection.execute(
                text(
                    f"DELETE FROM {quoted_identifier(destination)} "
                    f"WHERE {scope}"
                ),
                window_parameters,
            )
            print(
                f"[Очистка] Таблица '{destination}': удалено "
                f"{result.rowcount} строк текущего окна.",
                flush=True,
            )
            require_table_row_count(
                connection,
                destination,
                0,
                phase="после очистки окна",
                where_sql=scope,
                parameters=window_parameters,
            )

        for position, (contract_name, destination, data, use_index) in enumerate(
            prepared,
            start=1,
        ):
            file_name = source_files[contract_name]
            print(
                f"[Загрузка {position}/{total_tables}] {file_name} -> "
                f"таблица '{destination}' ({len(data.index)} строк)...",
                flush=True,
            )
            data.to_sql(
                destination,
                connection,
                if_exists="append",
                index=use_index,
            )
            print(
                f"[Записано {position}/{total_tables}] Таблица '{destination}' "
                "записана в текущую транзакцию.",
                flush=True,
            )

        expected_rows = {
            contract_name: len(data.index)
            for contract_name, _, data, _ in prepared
        }
        for contract_name, destination, _, _ in prepared:
            if contract_name in SNAPSHOT_CONTRACTS:
                where_sql = None
                parameters = None
                expected_total = expected_rows[contract_name]
            elif contract_name == "service_order_executors":
                where_sql = executor_scope
                parameters = window_parameters
                expected_total = (
                    executor_preserved_rows + expected_rows[contract_name]
                )
            else:
                where_sql = period_scopes[contract_name]
                parameters = window_parameters
                expected_total = (
                    period_preserved_rows[contract_name]
                    + expected_rows[contract_name]
                )
            require_table_row_count(
                connection,
                destination,
                expected_rows[contract_name],
                phase="после записи",
                where_sql=where_sql,
                parameters=parameters,
            )
            require_table_row_count(
                connection,
                destination,
                expected_total,
                phase="итоговая таблица с сохранённой историей",
            )

    print(
        f"Транзакция обновления {total_tables} таблиц успешно зафиксирована.",
        flush=True,
    )


def main() -> int:
    engine = None
    try:
        engine = get_db_engine()
        if engine is None:
            raise ConnectionError("Legacy database engine is unavailable")

        source_dir = export_directory()
        reference = datetime.now()
        cutoff, window_end = rolling_bounds(reference)
        prepared = prepare_exports(
            source_dir,
            cutoff=cutoff,
            window_end=window_end,
        )
        update_exports(prepared, engine, cutoff=cutoff, window_end=window_end)
    except Exception as error:
        logging.exception("1C CSV rolling-window update failed")
        print(f"1C CSV rolling-window update failed: {type(error).__name__}: {error}")
        return 1
    finally:
        if engine is not None:
            engine.dispose()

    total_rows = sum(len(data.index) for _, _, data, _ in prepared)
    print(
        f"Updated {len(prepared)} analytical tables from {total_rows} CSV rows "
        f"for the rolling window starting {cutoff:%Y-%m-%d}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
