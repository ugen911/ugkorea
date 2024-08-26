import pandas as pd
from ugkorea.db.database import get_db_engine

# Получаем объект подключения к базе данных
print("Подключение к базе данных...")
engine = get_db_engine()

# Проверяем наличие таблицы priceendmonth и данных за последний месяц
query_check = """
SELECT * 
FROM information_schema.tables 
WHERE table_name = 'priceendmonth'
"""

run_update = False

print("Проверка наличия таблицы priceendmonth...")
if not pd.read_sql_query(query_check, engine).empty:
    print("Таблица priceendmonth найдена. Проверка актуальности данных...")
    df_priceendmonth = pd.read_sql_query("SELECT * FROM priceendmonth", engine)
    
    # Преобразуем столбец 'data' в datetime
    df_priceendmonth['data'] = pd.to_datetime(df_priceendmonth['data'], format='%Y-%m')
    
    # Определяем последний месяц в данных
    last_month = (pd.Timestamp.today() - pd.DateOffset(months=1)).strftime('%Y-%m')
    if any(df_priceendmonth['data'].dt.strftime('%Y-%m') == last_month):
        print("Данные актуальны. Обновление не требуется.")
    else:
        # В таблице нет данных за последний месяц, выполняем обновление
        print("Данные не актуальны. Начало обновления...")
        run_update = True
else:
    # Таблица priceendmonth отсутствует, выполняем обновление
    print("Таблица priceendmonth отсутствует. Начало обновления...")
    run_update = True

if run_update:
    print("Получение данных из таблиц nomenklatura, postuplenija, tsenynakonetsmesjatsa, priceold...")
    
    # Определяем запросы для получения данных из каждой таблицы
    query_nomenklatura = "SELECT * FROM public.nomenklatura"
    query_postuplenija = "SELECT * FROM public.postuplenija"
    query_tsenynakonetsmesjatsa = "SELECT * FROM public.tsenynakonetsmesjatsa"
    query_priceold = "SELECT * FROM public.priceold"

    # Получаем данные и преобразуем их в DataFrame
    df_nomenklatura = pd.read_sql_query(query_nomenklatura, engine)
    df_postuplenija = pd.read_sql_query(query_postuplenija, engine)
    df_tsenynakonetsmesjatsa = pd.read_sql_query(query_tsenynakonetsmesjatsa, engine)
    df_priceold = pd.read_sql_query(query_priceold, engine)
    
    print("Обработка данных из таблицы nomenklatura...")
    # Оставляем нужные колонки в df_nomenklatura
    df_nomenklatura = df_nomenklatura[['kod', 'datasozdanija']]
    
    # Заменяем NaT в колонке datasozdanija на '2021-12-31'
    df_nomenklatura['datasozdanija'] = df_nomenklatura['datasozdanija'].fillna('2021-12-31')
    
    # Убираем пробелы и временную часть (если есть) и преобразуем в datetime
    df_nomenklatura['datasozdanija'] = df_nomenklatura['datasozdanija'].str.strip().str.slice(0, 10)
    df_nomenklatura['datasozdanija'] = pd.to_datetime(df_nomenklatura['datasozdanija'], format='%Y-%m-%d')

    print("Обработка данных из таблицы postuplenija...")
    # Оставляем нужные колонки в df_postuplenija и переименовываем tsenaroznichnaja в tsena, фильтруя по proveden == 'Да'
    df_postuplenija = df_postuplenija[df_postuplenija['proveden'] == 'Да']
    df_postuplenija = df_postuplenija[['kod', 'data', 'tsenaroznichnaja']]
    df_postuplenija.rename(columns={'tsenaroznichnaja': 'tsena'}, inplace=True)
    
    # Удаляем строки с пустыми значениями в колонке tsena
    df_postuplenija = df_postuplenija.dropna(subset=['tsena'])
    
    # Преобразуем колонку data в даты
    df_postuplenija['data'] = pd.to_datetime(df_postuplenija['data'], errors='coerce')
    if df_postuplenija['data'].isnull().any():
        print("Ошибка преобразования дат в таблице postuplenija. Проверьте формат данных.")
    
    # Преобразуем колонку tsena во float
    df_postuplenija['tsena'] = df_postuplenija['tsena'].astype(str).str.replace('\xa0', '').str.replace(',', '.').astype(float)

    print("Обработка данных из таблицы tsenynakonetsmesjatsa...")
    # Оставляем нужные колонки в df_tsenynakonetsmesjatsa и переименовываем, фильтруя по tiptseny == 'Основной тип цен продажи'
    df_tsenynakonetsmesjatsa = df_tsenynakonetsmesjatsa[df_tsenynakonetsmesjatsa['tiptseny'] == 'Основной тип цен продажи']
    df_tsenynakonetsmesjatsa = df_tsenynakonetsmesjatsa[['period', 'nomenklaturakod', 'tsena']]
    df_tsenynakonetsmesjatsa.rename(columns={'period': 'data', 'nomenklaturakod': 'kod'}, inplace=True)
    
    # Удаляем строки с пустыми значениями в колонке tsena
    df_tsenynakonetsmesjatsa = df_tsenynakonetsmesjatsa.dropna(subset=['tsena'])
    
    # Преобразуем колонку data в даты
    df_tsenynakonetsmesjatsa['data'] = pd.to_datetime(df_tsenynakonetsmesjatsa['data'], errors='coerce', dayfirst=True)
    if df_tsenynakonetsmesjatsa['data'].isnull().any():
        print("Ошибка преобразования дат в таблице tsenynakonetsmesjatsa. Проверьте формат данных.")
    
    # Преобразуем колонку tsena во float
    df_tsenynakonetsmesjatsa['tsena'] = df_tsenynakonetsmesjatsa['tsena'].astype(str).str.replace('\xa0', '').str.replace(',', '.').astype(float)

    print("Подготовка данных для объединения...")
    # Удаляем лишние пробелы в колонке 'kod'
    df_nomenklatura['kod'] = df_nomenklatura['kod'].str.strip()
    df_postuplenija['kod'] = df_postuplenija['kod'].str.strip()
    df_tsenynakonetsmesjatsa['kod'] = df_tsenynakonetsmesjatsa['kod'].str.strip()
    df_priceold['kod'] = df_priceold['kod'].str.strip()

    # Переименовываем DataFrame в df_tseny
    df_tseny = df_tsenynakonetsmesjatsa

    # Оставляем нужные колонки в df_priceold и переименовываем tsenarozn в tsena
    df_priceold = df_priceold[['kod', 'tsenarozn']]
    df_priceold.rename(columns={'tsenarozn': 'tsena'}, inplace=True)
    
    # Преобразуем колонку tsena во float
    df_priceold['tsena'] = df_priceold['tsena'].astype(str).str.replace('\xa0', '').str.replace(',', '.').astype(float)
    
    # Переименовываем DataFrame в df_priceactual
    df_priceactual = df_priceold.set_index('kod')

    print("Объединение данных из таблиц postuplenija и tsenynakonetsmesjatsa...")
    # Объединяем df_postuplenija и df_tseny
    df_combined = pd.concat([df_postuplenija, df_tseny])

    # Удаляем полные дубли строк
    df_combined = df_combined.drop_duplicates()

    # Определяем самую раннюю дату
    earliest_date = df_combined['data'].min()
    print(f"Самая ранняя дата в данных: {earliest_date}")

    print("Создание диапазона дат для расчета цен на конец каждого месяца...")
    # Создаем диапазон дат с конца каждого месяца, начиная с самой ранней даты до текущей даты
    date_range = pd.date_range(start=earliest_date, end=pd.Timestamp.today(), freq='ME')

    # Создаем DataFrame для хранения цен на конец каждого месяца
    result = pd.DataFrame({'data': date_range.repeat(df_priceactual.shape[0])})
    result['kod'] = list(df_priceactual.index) * len(date_range)
    result = result.sort_values(by=['kod', 'data']).reset_index(drop=True)

    print("Подготовка данных о датах создания товаров...")
    # Подготовка данных о датах создания товаров
    df_nomenklatura = df_nomenklatura.set_index('kod')
    creation_dates = df_nomenklatura['datasozdanija']

    print("Сортировка и индексация данных...")
    # Подготовка таблицы изменений цен для быстрого доступа
    df_combined = df_combined.sort_values(by=['kod', 'data'])
    df_combined = df_combined.set_index(['kod', 'data']).sort_index()

    print("Расчет последней цены до текущей даты...")
    # Функция для получения последней цены до текущей даты
    # Функция для получения последней цены до текущей даты с ограниченным выводом ошибок
    error_count = 0
    max_errors_to_display = 5

    def get_last_price(kod, date):
        global error_count
        try:
            changes = df_combined.loc[kod].loc[:date]
            if not changes.empty:
                return changes.iloc[-1]['tsena']
        except KeyError as e:
            if error_count < max_errors_to_display:
                print(f"KeyError: {e} для koda: {kod} и даты: {date}")
            error_count += 1
        return None

    # Применение функции для каждой строки
    result['tsena'] = result.apply(lambda row: get_last_price(row['kod'], row['data']), axis=1)

    print("Обработка отсутствующих значений (NaN)...")
    # Определяем минимальную дату в таблице creation_dates
    min_creation_date = creation_dates.min()

    # Обновляем логику обработки
    result['tsena'] = result.apply(
        lambda row: row['tsena'] if pd.notnull(row['tsena']) else (
            0 if row['data'] < min_creation_date else 
            None  # Если row['kod'] отсутствует в df_priceactual, значит цена отсутствует
        ), axis=1
    )

    print("Приведение данных к нужному формату и сохранение результата в базу данных...")
    # Приводим результат к нужному формату
    result['data'] = result['data'].dt.strftime('%Y-%m')
    result['tsena'] = result['tsena'].astype(float)

    # Сохраняем результат в таблицу priceendmonth
    result.to_sql('priceendmonth', engine, if_exists='replace', index=False)

    print("Данные успешно сохранены в таблицу priceendmonth.")
