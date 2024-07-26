"""
Этот скрипт выполняет следующие действия:
1. Подключается к базе данных и загружает данные из таблицы `kontaktnajainformatsija`.
2. Обрабатывает данные, создавая колонку `Имя` как объединение значений `imja`, `familija` и `otchestvo`, или использует значение из колонки `ssylka`, если предыдущие колонки отсутствуют.
3. Фильтрует и проверяет номера телефонов, приводя их к корректному формату.
4. Создает и сохраняет таблицу в формате XLSX с колонками: `Имя`, `Группа`, `Быстрый набор`, `IP-телефон`, `Рабочий телефон`, `Мобильный телефон`, `Домашний телефон`, `Другой телефон`.
5. Создает и сохраняет таблицу в формате CSV с колонками: `Ссылка`, `Номер`, добавляя строки для каждого типа телефона (рабочий, мобильный, домашний) с одинаковой ссылкой.

Файлы сохраняются по указанным адресам в двух форматах:
- rt.xlsx
- phoneY31.csv

Если сохранение по какому-либо адресу не удается, выводится сообщение об ошибке.
"""

from ugkorea.db.database import get_db_engine
import pandas as pd
from sqlalchemy import text

# Функция для проверки и преобразования номера телефона
def validate_and_format_phone_number(phone):
    phone = str(phone)
    if len(phone) == 7 and phone.startswith('2'):
        return phone
    elif len(phone) == 11:
        if phone.startswith('8'):
            return '7' + phone[1:]
        elif phone.startswith('7'):
            return phone
    return ''

# Получаем движок базы данных
engine = get_db_engine()

# Загружаем данные из базы данных
query = 'SELECT * FROM public.kontaktnajainformatsija'
df = pd.read_sql(query, engine)

# Создаем колонку 'Имя' как объединение 'imja', 'familija' и 'otchestvo' через пробел и с большой буквы
df['Имя'] = (df['familija'].fillna('') + ' ' + df['imja'].fillna('') + ' ' + df['otchestvo'].fillna('')).str.strip().str.title()

# Если значения `familija`, `imja` и `otchestvo` отсутствуют, используем `ssylka`
df['Имя'] = df.apply(lambda row: row['Имя'] if row['Имя'].strip() else row['ssylka'], axis=1)

# Фильтруем только телефонные номера
df_phones = df[df['vid'] == 'Телефон']

# Создаем новую таблицу с требуемой структурой
columns = ['Имя', 'Группа', 'Быстрый набор', 'IP-телефон', 'Рабочий телефон', 'Мобильный телефон', 'Домашний телефон', 'Другой телефон']
new_df = pd.DataFrame(columns=columns)

# Обрабатываем каждое имя и назначаем телефонные номера соответствующим колонкам
for name, group in df_phones.groupby('Имя'):
    phones = [validate_and_format_phone_number(phone) for phone in group['nomertelefona'].tolist()]
    row = {
        'Имя': name,
        'Группа': '',  # оставляем пустым
        'Быстрый набор': '',  # оставляем пустым
        'IP-телефон': '',  # оставляем пустым
        'Рабочий телефон': phones[0] if len(phones) > 0 else '',
        'Мобильный телефон': phones[1] if len(phones) > 1 else '',
        'Домашний телефон': phones[2] if len(phones) > 2 else '',
        'Другой телефон': ''  # оставляем пустым
    }
    new_df = pd.concat([new_df, pd.DataFrame([row])], ignore_index=True)

# Путь для сохранения файлов в формате XLSX
xlsx_paths = [
    r'C:\Users\evgen\YandexDisk\ЮК\rt.xlsx',
    r'\\26.218.196.12\заказы\Евгений\rt.xlsx'
]

# Сохранение в файл XLSX с проверкой доступности путей
for path in xlsx_paths:
    try:
        new_df.to_excel(path, index=False, engine='openpyxl')
        print(f'Файл успешно сохранен по адресу: {path}')
    except Exception as e:
        print(f'Ошибка при сохранении файла по адресу: {path}')
        print(f'Ошибка: {e}')

# Создание новой таблицы для phoneY31.csv
phone_df = pd.DataFrame(columns=['Ссылка', 'Номер'])

for _, row in new_df.iterrows():
    for col in ['Рабочий телефон', 'Мобильный телефон', 'Домашний телефон']:
        if row[col]:
            phone_row = {'Ссылка': row['Имя'], 'Номер': row[col]}
            phone_df = pd.concat([phone_df, pd.DataFrame([phone_row])], ignore_index=True)

# Путь для сохранения файлов в формате CSV
csv_paths = [
    r'C:\Users\evgen\YandexDisk\ЮК\phoneY31.csv',
    r'\\26.218.196.12\заказы\Евгений\phoneY31.csv'
]

# Сохранение в файл CSV с проверкой доступности путей
for path in csv_paths:
    try:
        phone_df.to_csv(path, index=False, sep=',', encoding='utf-8')
        print(f'Файл успешно сохранен по адресу: {path}')
    except Exception as e:
        print(f'Ошибка при сохранении файла по адресу: {path}')
        print(f'Ошибка: {e}')
