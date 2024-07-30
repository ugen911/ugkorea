"""
Этот скрипт выполняет следующие задачи:

1. Подключение к базе данных и получение данных из таблицы 'registrostatkitovarov'.
2. Фильтрация данных:
   - Удаление строк с отсутствующими значениями в колонке 'kolichestvo'.
   - Оставление только строк, где колонка 'skladkompanii' равна "Основной склад компании".
3. Преобразование данных:
   - Преобразование колонки 'period' в тип данных datetime.
   - Замена запятых на точки в колонке 'kolichestvo' и преобразование в тип данных float.
4. Создание дополнительной колонки 'adjusted_kolichestvo', которая изменяет знак значений 'kolichestvo' в зависимости от колонки 'viddvizhenija'.
5. Расчет начальных остатков на конец первого месяца и последующих месяцев.
6. Создание сводной таблицы, показывающей остатки для каждого товара ('nomenklaturakod') по месяцам.
7. Расчет текущих остатков на данный момент и добавление их в сводную таблицу.
8. Сохранение сводной таблицы с текущими остатками в Excel файл.

Библиотеки, используемые в скрипте:
- pandas: для обработки и анализа данных.
- datetime: для работы с датами и временем.
- ugkorea.db.database: для подключения к базе данных.

Пример использования:
1. Запустите скрипт, убедившись, что у вас есть доступ к базе данных и установлены необходимые библиотеки.
2. После выполнения скрипта, результат будет сохранен в файл 'pivot_table_with_current_balance.xlsx'.

"""

import pandas as pd
from ugkorea.db.database import get_db_engine
from datetime import datetime, timedelta

# Получаем объект подключения к базе данных
engine = get_db_engine()

# SQL-запрос для получения всех данных из таблицы registrostatkitovarov
query = "SELECT * FROM public.registrostatkitovarov"

# Выполняем запрос и сохраняем результат в переменной dataframe
dataframe = pd.read_sql(query, engine)

# Убираем строки, где в колонке kolichestvo значение None
dataframe = dataframe.dropna(subset=['kolichestvo'])

# Оставляем только строки, где в колонке skladkompanii значение "Основной склад компании"
dataframe = dataframe[dataframe['skladkompanii'] == "Основной склад компании"]

# Преобразуем колонку period в тип данных datetime
dataframe['period'] = pd.to_datetime(dataframe['period'], format='%d.%m.%Y %H:%M:%S')

# Заменяем запятые на точки в колонке kolichestvo и преобразуем в тип данных float
dataframe['kolichestvo'] = dataframe['kolichestvo'].str.replace(',', '.').astype(float)

# Сбрасываем индекс
dataframe.reset_index(drop=True, inplace=True)

# Определяем уникальные месяцы в данных
dataframe['month'] = dataframe['period'].dt.to_period('M')
unique_months = sorted(dataframe['month'].unique())

# Создаем столбец, который будет изменять знак kolichestvo в зависимости от viddvizhenija
dataframe['adjusted_kolichestvo'] = dataframe.apply(
    lambda row: -row['kolichestvo'] if row['viddvizhenija'] == 'Расход' else row['kolichestvo'], axis=1)

# Функция для вычисления начального остатка до конца месяца
def calculate_initial_balance(df):
    return df['adjusted_kolichestvo'].sum()

# Рассчитываем начальные остатки на конец первого месяца
first_month_end = unique_months[0].to_timestamp(how='end')
initial_df = dataframe[dataframe['period'] <= first_month_end]

initial_balances = initial_df.groupby('nomenklaturakod').agg({'adjusted_kolichestvo': 'sum'}).reset_index()
initial_balances.columns = ['nomenklaturakod', 'balance']

# Преобразуем результат в DataFrame
result_df = initial_balances.copy()
result_df['month'] = unique_months[0]

# Функция для обновления баланса за месяц
def update_balance(df, balances):
    for _, row in df.iterrows():
        kod = row['nomenklaturakod']
        if kod not in balances:
            balances[kod] = 0  # Добавляем новый код если его нет в балансе
        balances[kod] += row['adjusted_kolichestvo']
    return balances

# Рассчитываем остатки для каждого следующего месяца
balances = initial_balances.set_index('nomenklaturakod')['balance'].to_dict()
for month in unique_months[1:]:
    month_start = month.to_timestamp(how='start')
    month_end = month.to_timestamp(how='end')
    month_df = dataframe[(dataframe['period'] > month_start) & (dataframe['period'] <= month_end)]
    balances = update_balance(month_df, balances)
    month_result = pd.DataFrame(list(balances.items()), columns=['nomenklaturakod', 'balance'])
    month_result['month'] = month
    result_df = pd.concat([result_df, month_result], ignore_index=True)

# Преобразуем результат в сводную таблицу
pivot_table = result_df.pivot(index='nomenklaturakod', columns='month', values='balance').fillna(0)

# Сортируем колонки по возрастанию месяцев
pivot_table = pivot_table.sort_index(axis=1)

# Вычисляем текущую дату и время
current_time = datetime.now()

# Фильтруем данные до текущей даты и времени
current_df = dataframe[dataframe['period'] <= current_time]

# Рассчитываем текущий остаток для каждого nomenklaturakod
current_balances = current_df.groupby('nomenklaturakod').agg({'adjusted_kolichestvo': 'sum'}).reset_index()
current_balances.columns = ['nomenklaturakod', 'current_balance']

# Добавляем текущий остаток в сводную таблицу
pivot_table = pivot_table.merge(current_balances.set_index('nomenklaturakod'), on='nomenklaturakod', how='left')

# Загружаем таблицу в базу данных в схему public с названием stockendmonth
pivot_table.reset_index().to_sql('stockendmonth', engine, schema='public', if_exists='replace', index=False)

# Выводим сообщение о том, что все сделано успешно
print("Данные успешно загружены в таблицу 'stockendmonth' в схеме 'public'.")



