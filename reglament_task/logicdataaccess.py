import pandas as pd
from pathlib import Path
import re
from ugkorea.db.database import get_db_engine

# ===== ПУТИ И НАСТРОЙКИ =====
network_path = r"\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access"
file_name = "Номенклатура.xlsx"
file_path = Path(network_path) / file_name

if not file_path.exists():
    raise FileNotFoundError(f"Файл не найден: {file_path}")

df = pd.read_excel(file_path)

print("\n📦 Оригинальные названия колонок (до переименования):")
for col in df.columns:
    print(f"  {repr(col)}")

df.columns = [col.strip() for col in df.columns]

# ===== ПЕРЕИМЕНОВАНИЕ КОЛОНОК =====
rename_map = {
    'ЦБ': 'kod',
    'Не используется в заказе': 'ne_ispolzuetsya_v_zakaze',
    'Неликвид': 'nelikvid',
    'Зафиксировать минималки': 'zafiksirovat_minimalki',
    'Лист Ожидания': 'list_ozhidaniya',
    'Минималка местная': 'minimal',  # новая колонка
}
df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

# ===== ОЧИСТКА СТРОКОВЫХ ЗНАЧЕНИЙ =====
def clean_string(val):
    if isinstance(val, str):
        val = val.upper()
        val = re.sub(r'[\s\u200b\u200c\u200d\u200e\u200f\u202f\u00a0\r\n\t]+', '', val)
        return val
    return val

for col in df.select_dtypes(include='object').columns:
    df[col] = df[col].apply(clean_string)

# ===== ЛОГИЧЕСКИЕ КОЛОНКИ =====
bool_cols = ['ne_ispolzuetsya_v_zakaze', 'nelikvid', 'zafiksirovat_minimalki', 'list_ozhidaniya']
for col in bool_cols:
    if col not in df.columns:
        df[col] = False

print("\n🧪 После переименования. Какие логические колонки реально присутствуют:")
for col in bool_cols:
    print(f"  {'✅' if col in df.columns else '❌'} {col}")

print("\n📊 Значения в логических колонках:")
for col in bool_cols:
    print(f"  {col}: {df[col].sum()} True / {df[col].count()} всего")

# ===== ОБРАБОТКА КОЛОНКИ minimal =====
if 'minimal' in df.columns:
    df['minimal'] = pd.to_numeric(df['minimal'], errors='coerce').fillna(0).astype(int)
else:
    df['minimal'] = 0

# ===== ОЧИСТКА И УДАЛЕНИЕ ПУСТЫХ KOD =====
df['kod'] = df['kod'].astype(str).apply(clean_string)
df = df[df['kod'].notna() & (df['kod'] != '')]

# ===== ЗАГРУЗКА ИЗ БАЗЫ И ДОБАВЛЕНИЕ НЕДОСТАЮЩИХ =====
engine = get_db_engine()
nomenklatura_df = pd.read_sql("SELECT kod FROM nomenklaturaold", engine)
nomenklatura_df['kod'] = nomenklatura_df['kod'].astype(str).apply(clean_string)

existing_kods = set(df['kod'])
all_kods = set(nomenklatura_df['kod'])
missing_kods_set = all_kods - existing_kods

# Удаляем пустые, None, пробельные kody
clean_missing_kods = [k for k in missing_kods_set if k and clean_string(k) != '']

if clean_missing_kods:
    missing_kods_df = pd.DataFrame({'kod': clean_missing_kods})
    for col in bool_cols:
        missing_kods_df[col] = False
    missing_kods_df['minimal'] = 0
    df = pd.concat([df, missing_kods_df], ignore_index=True)

print("\n📊 После добавления недостающих kod:")
for col in bool_cols:
    print(f"  {col}: {df[col].sum()} True / {df[col].count()} всего")
print(f"  minimal: {df['minimal'].sum()} сумма / {df['minimal'].count()} всего строк")

# ===== ЗАПИСЬ В БАЗУ =====
df = df[['kod'] + bool_cols + ['minimal']]
df.to_sql('accessdata', engine, if_exists='replace', index=False)

print("✅ Данные успешно загружены в таблицу 'accessdata'")
