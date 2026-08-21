"""Replace legacy analytical tables from the fixed 1C CSV snapshot."""

from __future__ import annotations

import logging
import os
import sys
import warnings
from pathlib import Path

import pandas as pd
from transliterate import translit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_PARENT = PROJECT_ROOT.parent
if str(PROJECT_PARENT) not in sys.path:
    sys.path.insert(0, str(PROJECT_PARENT))

from ugkorea.db.database import get_db_engine

from .one_c_receiver_runtime.contracts import OBSERVED_CONTRACTS, PRUNED_HEADERS_BY_NAME

DEFAULT_EXPORT_DIR = Path(
    r"D:\NAS\заказы\Евгений\Access\Табличные выгрузки1С"
)
LOG_PATH = PROJECT_ROOT / "data_upload_errors.log"
SIMPLE_SNAPSHOT_CONTRACTS = tuple(
    (
        contract.file_name,
        PRUNED_HEADERS_BY_NAME.get(contract.name, contract.headers),
    )
    for contract in OBSERVED_CONTRACTS
)

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
    return Path(configured) if configured else DEFAULT_EXPORT_DIR


def table_name(file_name: str) -> str:
    stem = Path(file_name).stem.replace("Выгрузка_", "").replace("Выгрузка", "")
    return to_snake_case(stem)


def prepare_exports(source_dir: Path) -> list[tuple[str, pd.DataFrame, bool]]:
    if not source_dir.is_dir():
        raise FileNotFoundError(f"1C export directory does not exist: {source_dir}")

    missing = [
        file_name
        for file_name, _ in SIMPLE_SNAPSHOT_CONTRACTS
        if not (source_dir / file_name).is_file()
    ]
    if missing:
        raise FileNotFoundError("Missing required 1C CSV files: " + ", ".join(missing))

    prepared = []
    for file_name, expected_headers in SIMPLE_SNAPSHOT_CONTRACTS:
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

        destination = table_name(file_name)
        use_index = destination in {"nomenklaturaprimenjaemost", "nomenklatura"}
        if use_index:
            if "kod" not in data.columns:
                raise ValueError(f"Required kod column is missing in {file_name}")
            data.set_index("kod", inplace=True)
        prepared.append((destination, data, use_index))
    return prepared


def main() -> int:
    engine = None
    try:
        source_dir = export_directory()
        prepared = prepare_exports(source_dir)
        engine = get_db_engine()
        if engine is None:
            raise ConnectionError("Legacy database engine is unavailable")

        with engine.begin() as connection:
            for destination, data, use_index in prepared:
                data.to_sql(
                    destination,
                    connection,
                    if_exists="replace",
                    index=use_index,
                )
    except Exception as error:
        logging.exception("1C CSV snapshot replacement failed")
        print(f"1C CSV snapshot replacement failed: {type(error).__name__}: {error}")
        return 1
    finally:
        if engine is not None:
            engine.dispose()

    total_rows = sum(len(data.index) for _, data, _ in prepared)
    print(f"Replaced {len(prepared)} analytical tables from {total_rows} CSV rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
