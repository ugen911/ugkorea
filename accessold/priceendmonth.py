import pandas as pd
from ugkorea.db.database import get_db_engine

# Получаем объект подключения к базе данных
engine = get_db_engine()

# Проверяем наличие таблицы priceendmonth и данных за последний месяц
query_check = """
SELECT * 
FROM information_schema.tables 
WHERE table_name = 'priceendmonth'
"""

run_update = False


if not pd.read_sql_query(query_check, engine).empty:
    df_priceendmonth = pd.read_sql_query("SELECT * FROM priceendmonth", engine)
    
    # Определяем последний месяц в данных
    last_month = (pd.Timestamp.today() - pd.DateOffset(months=1)).strftime('%m.%y')
    if last_month in df_priceendmonth.columns:
        print("Данные актуальны. Обновление не требуется.")
    else:
        # В таблице нет данных за последний месяц, выполняем обновление
        run_update = True
else:
    # Таблица priceendmonth отсутствует, выполняем обновление
    run_update = True

if run_update:
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

    # Оставляем нужные колонки в df_nomenklatura
    df_nomenklatura = df_nomenklatura[['kod', 'datasozdanija']]
    # Заменяем NaT в колонке datasozdanija на '2021-12-31' и преобразуем в дату
    df_nomenklatura['datasozdanija'] = df_nomenklatura['datasozdanija'].fillna('2021-12-31')
    df_nomenklatura['datasozdanija'] = pd.to_datetime(df_nomenklatura['datasozdanija'])

    # Оставляем нужные колонки в df_postuplenija и переименовываем tsenaroznichnaja в tsena, фильтруя по proveden == 'Да'
    df_postuplenija = df_postuplenija[df_postuplenija['proveden'] == 'Да']
    df_postuplenija = df_postuplenija[['kod', 'data', 'tsenaroznichnaja']]
    df_postuplenija.rename(columns={'tsenaroznichnaja': 'tsena'}, inplace=True)
    # Удаляем строки с пустыми значениями в колонке tsena
    df_postuplenija = df_postuplenija.dropna(subset=['tsena'])
    # Преобразуем колонку data в даты, а колонку tsena во float
    df_postuplenija['data'] = pd.to_datetime(df_postuplenija['data'])
    df_postuplenija['tsena'] = df_postuplenija['tsena'].astype(str).str.replace('\xa0', '').str.replace(',', '.').astype(float)

    # Оставляем нужные колонки в df_tsenynakonetsmesjatsa и переименовываем, фильтруя по tiptseny == 'Основной тип цен продажи'
    df_tsenynakonetsmesjatsa = df_tsenynakonetsmesjatsa[df_tsenynakonetsmesjatsa['tiptseny'] == 'Основной тип цен продажи']
    df_tsenynakonetsmesjatsa = df_tsenynakonetsmesjatsa[['period', 'nomenklaturakod', 'tsena']]
    df_tsenynakonetsmesjatsa.rename(columns={'period': 'data', 'nomenklaturakod': 'kod'}, inplace=True)
    # Удаляем строки с пустыми значениями в колонке tsena
    df_tsenynakonetsmesjatsa = df_tsenynakonetsmesjatsa.dropna(subset=['tsena'])
    # Преобразуем колонку data в даты, а колонку tsena во float
    df_tsenynakonetsmesjatsa['data'] = pd.to_datetime(df_tsenynakonetsmesjatsa['data'], dayfirst=True)
    df_tsenynakonetsmesjatsa['tsena'] = df_tsenynakonetsmesjatsa['tsena'].astype(str).str.replace('\xa0', '').str.replace(',', '.').astype(float)

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

    # Объединяем df_postuplenija и df_tseny
    df_combined = pd.concat([df_postuplenija, df_tseny])

    # Удаляем полные дубли строк
    df_combined = df_combined.drop_duplicates()

    # Определяем самую раннюю дату
    earliest_date = df_combined['data'].min()

    # Создаем диапазон дат с конца каждого месяца, начиная с самой ранней даты до текущей даты
    date_range = pd.date_range(start=earliest_date, end=pd.Timestamp.today(), freq='ME')

    # Создаем DataFrame для хранения цен на конец каждого месяца
    result = pd.DataFrame()

    # Проходим по каждому товару в актуальных ценах
    for kod in df_priceactual.index.unique():
        # Получаем актуальную цену товара
        if kod in df_priceactual.index:
            current_price = df_priceactual.loc[kod, 'tsena']
        else:
            continue  # Пропустить если код не найден в df_priceactual

        # Отбираем все изменения цен для данного товара
        price_changes = df_combined[df_combined['kod'] == kod].sort_values('data')

        # Устанавливаем текущую цену на актуальную дату
        price_at_date = current_price

        # Заполняем цены на конец каждого месяца
        rows = []
        for date in date_range:
            # Проверяем изменения цен до текущей даты
            changes_until_date = price_changes[price_changes['data'] <= date]

            if not changes_until_date.empty:
                # Если были изменения, берем последнее изменение до текущей даты
                last_change = changes_until_date.iloc[-1]
                price_at_date = last_change['tsena']
            else:
                # Если изменений не было, используем текущую цену (до создания товара)
                creation_date = df_nomenklatura.loc[df_nomenklatura['kod'] == kod, 'datasozdanija'].values[0]
                if date < creation_date:
                    price_at_date = 0  # Цена до создания товара равна 0

            rows.append({'kod': kod, 'data': date, 'tsena': price_at_date})
        
        result = pd.concat([result, pd.DataFrame(rows)], ignore_index=True)

    # Приводим результат к нужному формату
    result['data'] = pd.to_datetime(result['data'])
    result['tsena'] = result['tsena'].astype(float)

    # Создаем сводную таблицу
    result_pivot = result.pivot_table(index='kod', columns=result['data'].dt.strftime('%m.%y'), values='tsena', aggfunc='first', fill_value=0)

    # Сортируем столбцы по возрастанию даты
    sorted_columns = sorted(result_pivot.columns, key=lambda x: pd.to_datetime(x, format='%m.%y'))
    result_pivot = result_pivot.reindex(columns=sorted_columns)

    print("result_pivot (сводная таблица):")
    print(result_pivot.head())

    # Сохраняем результат в таблицу priceendmonth
    result_pivot.to_sql('priceendmonth', engine, if_exists='replace', index=False)

    print("Данные успешно сохранены в таблицу priceendmonth.")
