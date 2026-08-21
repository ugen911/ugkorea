"""Explicit versioned contracts for the observed 1C CSV exports."""

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Protocol


class ImportContract(Protocol):
    """Minimum batch identity shared by CSV and workbook contracts."""

    @property
    def name(self) -> str: ...

    @property
    def file_name(self) -> str: ...

    @property
    def version(self) -> int: ...

    @property
    def sensitive(self) -> bool: ...


class ImportMetadata(Protocol):
    """Minimum source metadata stored in an import batch."""

    @property
    def encoding(self) -> str: ...

    @property
    def headers(self) -> tuple[str, ...]: ...


class CsvRowRepair(StrEnum):
    """Narrow source-aware recovery for known unescaped 1C fields."""

    NONE = "none"
    TRAILING_DELIMITERS = "trailing_delimiters"
    UNQUOTED_MULTILINE_FIELD = "unquoted_multiline_field"


@dataclass(frozen=True, slots=True)
class CsvContract:
    name: str
    file_name: str
    headers: tuple[str, ...]
    version: int = 1
    sensitive: bool = False
    row_repair: CsvRowRepair = CsvRowRepair.NONE
    repair_field_index: int | None = None

    def __post_init__(self) -> None:
        if self.row_repair is CsvRowRepair.UNQUOTED_MULTILINE_FIELD:
            if self.repair_field_index is None or not 0 <= self.repair_field_index < len(
                self.headers
            ):
                raise ValueError("Multiline CSV repair requires a valid field index")
        elif self.repair_field_index is not None:
            raise ValueError("CSV repair field index is only valid for multiline repair")


OBSERVED_CONTRACTS: tuple[CsvContract, ...] = (
    CsvContract(
        name="service_order_works",
        file_name="Выгрузка_Автоработы_ЗаказНаряд.csv",
        headers=(
            "Авторабота",
            "Цена",
            "Количество",
            "Дата",
            "Проведен",
            "Ссылка",
            "ХозОперация",
            "Состояние",
            "Контрагент",
            "ВидРемонта",
            "ИдентификаторРаботы",
            "Код",
            "Марка",
            "Модель",
            "VIN",
            "Автор",
        ),
        sensitive=True,
    ),
    CsvContract(
        name="customer_returns",
        file_name="Выгрузка_ВозвратыОтПокупателей.csv",
        headers=(
            "Дата",
            "Проведен",
            "ХозОперация",
            "ДокументОснование",
            "Номенклатура",
            "Количество",
            "Цена",
            "Автор",
            "Код",
        ),
    ),
    CsvContract(
        name="customer_orders",
        file_name="Выгрузка_ЗаказыПокупателей.csv",
        headers=(
            "Ссылка",
            "ДокументОснование",
            "Автор",
            "Номенклатура",
            "Количество",
            "ЕдиницаИзмерения",
            "Цена",
            "ХозОперация",
            "Проведен",
            "Дата",
            "Контрагент",
            "Код",
        ),
        sensitive=True,
    ),
    CsvContract(
        name="service_order_executors",
        file_name="Выгрузка_Исполнители_ЗаказНаряд.csv",
        headers=("Ссылка", "НомерСтроки", "ИдентификаторРаботы", "Исполнитель", "Процент"),
        sensitive=True,
    ),
    CsvContract(
        name="contact_information",
        file_name="Выгрузка_КонтактнаяИнформация.csv",
        headers=("Имя", "Фамилия", "Отчество", "Ссылка", "НомерТелефона", "Вид", "Представление"),
        version=2,
        sensitive=True,
        row_repair=CsvRowRepair.TRAILING_DELIMITERS,
    ),
    CsvContract(
        name="sales_adjustments",
        file_name="Выгрузка_Корректировка_Реализации.csv",
        headers=(
            "Ссылка",
            "Дата",
            "Проведен",
            "ХозОперация",
            "Менеджер",
            "Номенклатура",
            "Количество",
            "Цена",
            "ДокументОснование",
            "Код",
        ),
        sensitive=True,
    ),
    CsvContract(
        name="corrections",
        file_name="Выгрузка_Корректировки.csv",
        headers=(
            "Ссылка",
            "Номенклатура",
            "Количество",
            "Цена",
            "Дата",
            "Проведен",
            "ХозОперация",
            "Код",
            "ДокументОснование",
        ),
    ),
    CsvContract(
        name="products",
        file_name="Выгрузка_Номенклатура.csv",
        headers=(
            "Ссылка",
            "Код",
            "Артикул",
            "НаименованиеПолное",
            "Производитель",
            "ВидНоменклатуры",
            "ТипНоменклатуры",
            "БазоваяЕдиница",
            "РС_ДатаВвода",
            "Автор",
            "ДатаСоздания",
            "Родитель",
        ),
        version=2,
        row_repair=CsvRowRepair.UNQUOTED_MULTILINE_FIELD,
        repair_field_index=3,
    ),
    CsvContract(
        name="product_applicability",
        file_name="Выгрузка_НоменклатураПрименяемость.csv",
        headers=("Номенклатура", "Модель", "Код"),
    ),
    CsvContract(
        name="receipts",
        file_name="Выгрузка_Поступления.csv",
        headers=(
            "Ссылка",
            "Номенклатура",
            "Количество",
            "Цена",
            "ЦенаРозничная",
            "Дата",
            "Проведен",
            "ХозОперация",
            "Код",
            "Контрагент",
        ),
    ),
    CsvContract(
        name="sales",
        file_name="Выгрузка_Продажи.csv",
        headers=(
            "Номенклатура",
            "ХозОперация",
            "ДокументПродажи",
            "Количество",
            "Сумма",
            "Период",
            "СкладКомпании",
            "Себестоимость",
            "Поставщик",
            "Покупатель",
            "Код",
        ),
        sensitive=True,
    ),
    CsvContract(
        name="realizations",
        file_name="Выгрузка_Реализация.csv",
        headers=(
            "Ссылка",
            "ДокументОснование",
            "Автор",
            "Номенклатура",
            "Количество",
            "ЕдиницаИзмерения",
            "Цена",
            "ХозОперация",
            "Проведен",
            "Дата",
            "Контрагент",
            "РС_ЗаказНаряд",
            "Код",
        ),
        sensitive=True,
    ),
    CsvContract(
        name="service_order_items",
        file_name="Выгрузка_Товары_ЗаказНаряд.csv",
        headers=(
            "Ссылка",
            "Дата",
            "Проведен",
            "Контрагент",
            "Состояние",
            "ХозОперация",
            "ДатаЗакрытия",
            "Номенклатура",
            "ЕдиницаИзмерения",
            "Количество",
            "Цена",
            "ВидРемонта",
            "Код",
            "Автор",
        ),
        sensitive=True,
    ),
    CsvContract(
        name="lost_demand",
        file_name="Выгрузка_УпущенныйСпрос.csv",
        headers=(
            "Период",
            "АртикулДляПоиска",
            "Производитель",
            "Количество",
            "Автор",
            "Номенклатура",
            "Артикул",
            "Код",
        ),
    ),
    CsvContract(
        name="month_end_prices",
        file_name="Выгрузка_ЦеныНаКонецМесяца.csv",
        headers=(
            "Период",
            "Регистратор",
            "Активность",
            "ТипЦены",
            "ПодразделениеКомпании",
            "Цена",
            "НоменклатураКод",
        ),
    ),
    CsvContract(
        name="inventory_movements",
        file_name="ВыгрузкаРегистрОстаткиТоваров.csv",
        headers=(
            "НоменклатураКод",
            "ВидДвижения",
            "Период",
            "Количество",
            "ХозОперация",
            "СкладКомпании",
        ),
    ),
    CsvContract(
        name="mechanic_output",
        file_name="ВыработкаСлесарей.csv",
        headers=(
            "ДатаЗакрытия",
            "Слесарь",
            "Авторабота",
            "ВидРемонта",
            "ДокументПродажи",
            "Количество",
            "Сумма",
        ),
        sensitive=True,
    ),
)

PRUNED_HEADERS_BY_NAME: dict[str, tuple[str, ...]] = {
    "customer_returns": (
        "Дата",
        "Проведен",
        "ХозОперация",
        "ДокументОснование",
        "Количество",
        "Цена",
        "Автор",
        "Код",
    ),
    "customer_orders": (
        "Ссылка",
        "ДокументОснование",
        "Автор",
        "Количество",
        "ЕдиницаИзмерения",
        "Цена",
        "ХозОперация",
        "Проведен",
        "Дата",
        "Контрагент",
        "Код",
    ),
    "contact_information": (
        "Имя",
        "Фамилия",
        "Отчество",
        "Ссылка",
        "НомерТелефона",
        "Вид",
    ),
    "sales_adjustments": (
        "Ссылка",
        "Дата",
        "Проведен",
        "ХозОперация",
        "Менеджер",
        "Количество",
        "Цена",
        "ДокументОснование",
        "Код",
    ),
    "corrections": (
        "Ссылка",
        "Количество",
        "Цена",
        "Дата",
        "Проведен",
        "ХозОперация",
        "Код",
        "ДокументОснование",
    ),
    "products": (
        "Код",
        "Артикул",
        "НаименованиеПолное",
        "Производитель",
        "ВидНоменклатуры",
        "БазоваяЕдиница",
        "ДатаСоздания",
    ),
    "product_applicability": ("Модель", "Код"),
    "realizations": (
        "Ссылка",
        "ДокументОснование",
        "Автор",
        "Количество",
        "ЕдиницаИзмерения",
        "Цена",
        "ХозОперация",
        "Проведен",
        "Дата",
        "Контрагент",
        "РС_ЗаказНаряд",
        "Код",
    ),
    "lost_demand": (
        "Период",
        "АртикулДляПоиска",
        "Производитель",
        "Количество",
        "Автор",
        "Артикул",
        "Код",
    ),
}

EXTENDED_HEADERS_BY_NAME: dict[str, tuple[str, ...]] = {
    "service_order_executors": (
        "Ссылка",
        "Дата",
        "НомерСтроки",
        "ИдентификаторРаботы",
        "Исполнитель",
        "Процент",
    ),
}


def _latest_contract(contract: CsvContract) -> CsvContract:
    extended_headers = EXTENDED_HEADERS_BY_NAME.get(contract.name)
    if extended_headers is not None:
        return replace(
            contract,
            headers=extended_headers,
            version=contract.version + 1,
        )
    headers = PRUNED_HEADERS_BY_NAME.get(contract.name)
    if headers is None:
        return contract
    if contract.name == "contact_information":
        return replace(
            contract,
            headers=headers,
            version=contract.version + 1,
            row_repair=CsvRowRepair.NONE,
            repair_field_index=None,
        )
    if contract.name == "products":
        return replace(
            contract,
            headers=headers,
            version=contract.version + 1,
            repair_field_index=headers.index("НаименованиеПолное"),
        )
    return replace(contract, headers=headers, version=contract.version + 1)


OBSERVED_CONTRACTS_BY_NAME = {contract.name: contract for contract in OBSERVED_CONTRACTS}
CONTRACTS: tuple[CsvContract, ...] = tuple(
    _latest_contract(contract) for contract in OBSERVED_CONTRACTS
)
CONTRACTS_BY_NAME = {contract.name: contract for contract in CONTRACTS}
CONTRACT_VERSIONS_BY_NAME: dict[str, tuple[CsvContract, ...]] = {
    observed.name: (
        (observed, CONTRACTS_BY_NAME[observed.name])
        if observed.version != CONTRACTS_BY_NAME[observed.name].version
        else (observed,)
    )
    for observed in OBSERVED_CONTRACTS
}


def resolve_contract(name: str, headers: tuple[str, ...]) -> CsvContract | None:
    """Resolve an observed header to a supported version without guessing."""
    return next(
        (
            contract
            for contract in CONTRACT_VERSIONS_BY_NAME.get(name, ())
            if contract.headers == headers
        ),
        None,
    )
