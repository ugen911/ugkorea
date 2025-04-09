import pandas as pd
import re
from ugkorea.db.database import get_db_engine

# ===== Очистка строк от мусора =====
def clean_string(val):
    if isinstance(val, str):
        val = val.upper()
        val = re.sub(r'[\s\u200b\u200c\u200d\u200e\u200f\u202f\u00a0\r\n\t]+', '', val)
        return val
    return val

# ===== Подключение к базе =====
engine = get_db_engine()

# ===== Загрузка данных =====
nomenklatura_df = pd.read_sql("SELECT kod FROM nomenklaturaold", engine)
accessdata_df = pd.read_sql("SELECT * FROM accessdata", engine)

# ===== Очистка и подготовка =====
nomenklatura_df['kod'] = nomenklatura_df['kod'].astype(str).apply(clean_string)
accessdata_df['kod'] = accessdata_df['kod'].astype(str).apply(clean_string)

# ===== Исключение пустых и None kod =====
nomenklatura_df = nomenklatura_df[nomenklatura_df['kod'].notna() & (nomenklatura_df['kod'] != '')]

# ===== Поиск недостающих kod =====
existing_kods = set(accessdata_df['kod'])
all_kods = set(nomenklatura_df['kod'])
missing_kods = all_kods - existing_kods

# ===== Добавление недостающих kod =====
if missing_kods:
    print(f"🔄 Добавляем недостающие kody: {len(missing_kods)}")
    new_rows = pd.DataFrame({'kod': list(missing_kods)})
    # логические колонки
    bool_cols = ['ne_ispolzuetsya_v_zakaze', 'nelikvid', 'zafiksirovat_minimalki', 'list_ozhidaniya']
    for col in bool_cols:
        new_rows[col] = False
    # объединение
    accessdata_df = pd.concat([accessdata_df, new_rows], ignore_index=True)
else:
    print("✅ Все kody из nomenklaturaold уже есть в accessdata — добавлять ничего не нужно.")

# ===== Перезапись accessdata =====
accessdata_df.to_sql('accessdata', engine, if_exists='replace', index=False)
print("✅ Таблица 'accessdata' успешно обновлена.")
