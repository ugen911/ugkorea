import os
import pandas as pd
import numpy as np
import re
from pathlib import Path
from ugkorea.db.database import get_db_engine

# Путь к файлу
network_path = r"\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access"
file_name = "Заказ.xlsx"
file_path = Path(network_path) / file_name

# Проверка существования файла
if not file_path.exists():
    raise FileNotFoundError(f"Файл не найден: {file_path}")

# Чтение файла
df = pd.read_excel(file_path)

# Переименование колонок
rename_map = {}
for col in df.columns:
    col_clean = col.strip()  # убираем пробелы и непечатные символы
    if col_clean == 'Код':
        rename_map[col] = 'id_zakaz'
    elif col_clean == 'Код 1С':
        rename_map[col] = 'kod'
    else:
        # Транслитерация
        translit_col = (
            col_clean
            .replace(' ', '_')
            .lower()
            .translate(str.maketrans(
                'абвгдеёжзийклмнопрстуфхцчшщъыьэюя',
                'abvgdeejzijklmnoprstufhccss_y_eua'))
        )
        rename_map[col] = translit_col

# Применяем переименование
df.rename(columns=rename_map, inplace=True)

# Очистка названий колонок от пробелов и непечатных символов
df.columns = [col.strip() for col in df.columns]

# Обработка данных
for date_col in ['data_prinyatiya_zakaza', 'data_scheta']:
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

float_cols = ['zakup', 'kolichestvo', 'summa', 'roznica']
for col in float_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Обработка наценки
if 'nacenka' in df.columns:
    df['nacenka'] = (
        df['nacenka']
        .astype(str)
        .str.replace('%', '', regex=False)
        .str.replace(',', '.', regex=False)
        .astype(float)
        .round(2)
    )

# Приведение остальных колонок к текстовому типу
known_columns = set(['data_prinyatiya_zakaza', 'data_scheta', 'zakup', 'kolichestvo', 'summa', 'roznica', 'nacenka'])
for col in df.columns:
    if col not in known_columns:
        df[col] = df[col].astype(str)

# Загрузка в базу данных
engine = get_db_engine()
df.to_sql('orders', engine, if_exists='replace', index=False)

print("Данные успешно загружены в таблицу 'orders'")
