import pandas as pd
import re
from sqlalchemy import text
from ugkorea.db.database import get_db_engine

# Функция для замены служебных положений на русский язык
def replace_positions_updated(text_value):
    replacements = {
        'FR': 'передний',
        'RR': 'задний',
        'RH': 'правый',
        'LH': 'левый',
        'IN': 'впуск',
        'EX': 'выпуск'
    }

    def replacement_func(match):
        abbr = match.group(0)
        return replacements.get(abbr, abbr)

    # Обработка сочетаний через /
    pattern = r'\b(?:FR|RR|RH|LH|IN|EX)(?:/(?:FR|RR|RH|LH|IN|EX))*\b'
    text_value = re.sub(pattern, replacement_func, text_value)
    
    # Обработка одиночных случаев
    pattern = r'\b(?:FR|RR|RH|LH|IN|EX)\b'
    text_value = re.sub(pattern, replacement_func, text_value)

    return text_value

# Функция для извлечения первых слов кириллицей
def extract_first_cyrillic_words(text_value):
    match = re.match(r'^[а-яА-ЯёЁ\s]+', text_value)
    return match.group(0).strip() if match else ""

# Функция для извлечения типа детали и удаления нежелательных символов
def extract_type_detail(row, additional_types, additional_words):
    naimenovanie = row['naimenovanie']
    if not naimenovanie:
        return None

    # Специальная проверка для "Датчик ABS"
    if re.match(r'^датчик\s+abs', naimenovanie, re.IGNORECASE):
        return 'датчик abs'

    # Проверка на дополнительные типы деталей
    for type_name, pattern in additional_types.items():
        if type_name == 'ТНВД':
            if re.match(pattern, naimenovanie, re.IGNORECASE):
                return type_name.lower()
        else:
            if re.search(pattern, naimenovanie, re.IGNORECASE):
                return type_name.lower()

    first_cyrillic_words = extract_first_cyrillic_words(naimenovanie)
    uppercase_word_match = re.search(r'\b[A-ZА-ЯЁ]{2,}\b', naimenovanie)
    if uppercase_word_match:
        start_index = uppercase_word_match.start()
    else:
        start_index = naimenovanie.find(' ')
        if start_index == -1:
            start_index = len(naimenovanie)

    type_detail_candidate = naimenovanie[:start_index].strip().lower()
    type_detail_candidate = re.sub(r'\d|\(|\)', '', type_detail_candidate).strip()
    result = re.sub(r'[^а-яА-ЯёЁ\s]', '', type_detail_candidate).strip()
    result = re.sub(r'\b\w\b', '', result).strip()

    if result and not result.startswith(first_cyrillic_words.lower()):
        result = first_cyrillic_words.lower() + ' ' + result

    current_words = set(result.split())
    for word in additional_words:
        if re.search(r'\b' + word, naimenovanie.lower()) and word not in current_words:
            matches = re.findall(r'\b\w*' + word + r'\w*\b', naimenovanie.lower())
            for match in matches:
                if match not in current_words:
                    result += ' ' + match
                    current_words.add(match)

    result = ' '.join(dict.fromkeys(result.split()))
    return result

# Функция для очистки типа детали
def clean_type_detail(text_value):
    if text_value is None:
        return None
    text_value = re.sub(r'\bдв\b|\bориг\b', '', text_value).strip()
    return re.sub(r'\s+', ' ', text_value).strip()

# Функция для дополнения типов деталей
def extend_type_detail(row, unique_types):
    naimenovanie = row['naimenovanie']
    current_type = row['type_detail']
    if not current_type:
        return current_type
    
    current_type_words = set(current_type.split())
    for candidate_type in unique_types:
        candidate_words = set(candidate_type.split())
        if current_type_words < candidate_words and all(word in naimenovanie for word in candidate_words):
            return candidate_type
    return current_type

# Функция для уточнения уникальных типов
def refine_unique_types(row, type_counts):
    current_type = row['type_detail']
    if current_type is None or type_counts[current_type] > 1:
        return current_type
    
    type_words = current_type.split()
    while len(type_words) > 1:
        type_words.pop()
        shortened_type = ' '.join(type_words)
        if type_counts.get(shortened_type, 0) > 1:
            return shortened_type
    return current_type

# Функция для обновления типов фильтров
def update_filter_type(type_detail):
    if type_detail is None:
        return None
    words = type_detail.split()
    if 'фильтр' in words and 'салонный' in words and 'воздушный' not in words:
        return 'фильтр воздушный салонный'
    return type_detail

# Функция для приведения типов деталей товаров с одинаковым gruppa_analogov к одному значению
def unify_types_with_analog_groups(df):
    grouped = df.groupby('gruppa_analogov')
    for name, group in grouped:
        if len(group['type_detail'].unique()) > 1:
            longest_type = max(group['type_detail'], key=len)
            df.loc[df['gruppa_analogov'] == name, 'type_detail'] = longest_type
    return df

if __name__ == "__main__":
    # Получение двигателя БД
    engine = get_db_engine()

    # Загрузка таблиц из базы данных в DataFrame
    groupanalogiold_df = pd.read_sql_table('groupanalogiold', engine, schema='public')
    nomenklaturaold_df = pd.read_sql_table('nomenklaturaold', engine, schema='public')

    # Проверка, что таблица groupanalogiold не пустая
    if groupanalogiold_df.empty:
        raise ValueError("Таблица 'groupanalogiold' пустая.")

    # Приведение типов данных к строковому типу для объединения и очистка пробелов
    nomenklaturaold_df['kod'] = nomenklaturaold_df['kod'].astype(str).str.strip()
    groupanalogiold_df['kod_1s'] = groupanalogiold_df['kod_1s'].astype(str).str.strip()

    # Объединение DataFrame с использованием левого соединения
    merged_df = pd.merge(nomenklaturaold_df, groupanalogiold_df, how='left', left_on='kod', right_on='kod_1s')
    merged_df['naimenovanie'] = merged_df['naimenovanie'].apply(replace_positions_updated)

    # Дополнительные типы деталей для поиска
    additional_types = {
        'ТНВД': r'^ТНВД\b',
        'масло моторное': r'\bмасло моторное\b',
        'масло трансмиссионное': r'\bмасло трансмиссионное\b',
        'масло для гур': r'\bмасло для гур\b',
        'промывочное масло': r'\bпромывочное масло\b',
        'ПГУ сцепления': r'\bПГУ сцепления\b',
        'тара': r'\bТАРА\b',
        'антифриз': r'\bантифриз\b'
    }

    additional_words = ['грм', 'топливн', 'маслонасос', 'коленвал', 'ручейков', 'клинов', 'радиатор', 'коренн', 'шатун', 'термост', 'рычаг', 'двс', 'воздуш', 'маслян']

    # Применение функции для извлечения и обработки типа детали
    merged_df['type_detail'] = merged_df.apply(
        lambda row: extract_type_detail(row, additional_types, additional_words), axis=1)

    # Дополнительная очистка после извлечения типа детали и обновление фильтров
    merged_df['type_detail'] = (merged_df['type_detail']
                                .str.replace(r'[^а-яА-ЯёЁ\s]', '', regex=True)
                                .str.replace(r'\b\w\b', '', regex=True)
                                .str.replace(r'\bмм\b', '', regex=True)
                                .apply(clean_type_detail)
                                .str.lower()
                                .apply(update_filter_type))

    # Новый участок кода для дополнения типов деталей
    unique_types = merged_df['type_detail'].dropna().unique()
    sorted_unique_types = sorted(unique_types, key=lambda x: len(x.split()), reverse=True)

    # Применение дополнения типов деталей
    merged_df['type_detail'] = merged_df.apply(
        lambda row: extend_type_detail(row, sorted_unique_types), axis=1)

    # Проверка типов деталей с уникальными значениями и перезапись при необходимости
    type_counts = merged_df['type_detail'].value_counts(dropna=True)
    merged_df['type_detail'] = merged_df.apply(
        lambda row: refine_unique_types(row, type_counts), axis=1)

    # Приведение типов деталей товаров с одинаковыми gruppa_analogov к одному значению
    merged_df = unify_types_with_analog_groups(merged_df)

    # Подготовка DataFrame для выгрузки
    upload_df = merged_df[['kod', 'type_detail']]

    # Удаление существующей таблицы, если она есть
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS public.typedetailgen"))

    # Выгрузка DataFrame в базу данных
    upload_df.to_sql('typedetailgen', engine, schema='public', if_exists='replace', index=False)

    # Вывод первых нескольких строк обновленного DataFrame для проверки
    print(merged_df.head(20))

    # Проверка строк, где type_detail равно None
    missing_types = merged_df[merged_df['type_detail'].isnull()]
    print("Строки без типа детали:")
    print(missing_types[['naimenovanie']])
