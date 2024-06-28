import pandas as pd
from ugkorea.db.database import get_db_engine

# Подключение к базе данных
engine = get_db_engine()

# Получение всех данных из таблицы ostatkitovarovnakonetskazhdogomesjatsa
query_stock = """
SELECT 
    * 
FROM ostatkitovarovnakonetskazhdogomesjatsa
"""
stock_df = pd.read_sql(query_stock, engine)
stock_df.name = "stock"

import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Предположим, что stock_df уже загружен
# stock_df = pd.read_csv('path_to_your_file.csv')

# Преобразуем столбец 'period' в формат даты и времени
stock_df['period'] = pd.to_datetime(stock_df['period'], format='%d.%m.%Y %H:%M:%S')

# Преобразуем столбец 'kolichestvo' в числовой формат, заменяя пустоты на 0
stock_df['kolichestvo'] = pd.to_numeric(stock_df['kolichestvo'], errors='coerce').fillna(0)

# Фильтруем данные для основного склада компании
print("Фильтрация данных для основного склада компании...")
main_warehouse_data = stock_df[stock_df['skladkompanii'] == 'Основной склад компании']

# Сортируем данные по коду номенклатуры и периоду
print("Сортировка данных по коду номенклатуры и периоду...")
main_warehouse_data = main_warehouse_data.sort_values(by=['nomenklaturakod', 'period'])

# Устанавливаем период для анализа
start_date = main_warehouse_data['period'].min().to_period('M').start_time
end_date = main_warehouse_data['period'].max().to_period('M').end_time

# Создаем серию дат по месяцам в заданном интервале
periods = pd.date_range(start=start_date, end=end_date, freq='ME')

# Функция для обработки одного кода номенклатуры
def process_code(code):
    result = []
    last_known_stock = 0
    print(f"Обработка кода номенклатуры: {code}")
    for period_end in periods:
        print(f"  Период окончания: {period_end}")
        # Фильтруем данные для конкретного кода и до конца текущего месяца
        code_data = main_warehouse_data[(main_warehouse_data['nomenklaturakod'] == code) & (main_warehouse_data['period'] <= period_end)]
        if not code_data.empty:
            # Берем последнюю запись до конца текущего месяца
            last_record = code_data.iloc[-1]
            last_known_stock = last_record['kolichestvo']
            print(f"    Найдена запись: {last_record.to_dict()}")
        else:
            print("    Нет данных до этого периода, устанавливаем остаток в 0")
        result.append({
            'nomenklaturakod': code,
            'period': period_end,
            'kolichestvo': last_known_stock
        })
    return result

# Получаем список уникальных кодов номенклатур
codes = main_warehouse_data['nomenklaturakod'].unique()

# Создаем пул потоков и обрабатываем данные
with ThreadPoolExecutor() as executor:
    results = list(executor.map(process_code, codes))

# Преобразуем список результатов в DataFrame
result_df = pd.DataFrame([item for sublist in results for item in sublist])

# Создаем сводную таблицу для отображения остатков на конец каждого месяца
pivot_table = result_df.pivot_table(index='nomenklaturakod', columns='period', values='kolichestvo', aggfunc='last', fill_value=0)

# Отображаем сводную таблицу
print("Создание сводной таблицы...")
print(pivot_table)

# При необходимости сохранить таблицу в файл
print("Сохранение сводной таблицы в файл...")
pivot_table.to_csv('monthly_closing_stock.csv')

print("Готово!")







