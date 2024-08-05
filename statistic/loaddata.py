import pandas as pd
from ugkorea.db.database import get_db_engine

# Функция для удаления пробелов
def trim_whitespace(df):
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df

def load_and_process_data():
    # Подключение к базе данных
    engine = get_db_engine()

    # Загрузка данных из таблиц
    nomenklaturaold = pd.read_sql_table('nomenklaturaold', engine)
    nomenklatura = pd.read_sql_table('nomenklatura', engine)
    stockold = pd.read_sql_table('stockold', engine)
    priceold = pd.read_sql_table('priceold', engine)
    prodazhi = pd.read_sql_table('prodazhi', engine)
    postuplenija = pd.read_sql_table('postuplenija', engine)
    stockendmonth = pd.read_sql_table('stockendmonth', engine)
    groupanalogiold = pd.read_sql_table('groupanalogiold', engine)
    typedetailgen = pd.read_sql_table('typedetailgen', engine)

    # Удаление пробелов сначала и конца у каждой строковой колонки
    nomenklaturaold = trim_whitespace(nomenklaturaold)
    nomenklatura = trim_whitespace(nomenklatura)
    stockold = trim_whitespace(stockold)
    priceold = trim_whitespace(priceold)
    prodazhi = trim_whitespace(prodazhi)
    postuplenija = trim_whitespace(postuplenija)
    stockendmonth = trim_whitespace(stockendmonth)
    groupanalogiold = trim_whitespace(groupanalogiold)
    typedetailgen = trim_whitespace(typedetailgen)

    # Фильтрация и выбор колонок для nomenklaturaold
    nomenklaturaold = nomenklaturaold[nomenklaturaold['pometkaudalenija'] == 'Нет']
    nomenklaturaold = nomenklaturaold[['kod', 'artikul', 'proizvoditel', 'edizm']]

    # Выбор колонок и преобразование дат для nomenklatura
    nomenklatura = nomenklatura[['kod', 'datasozdanija', 'roditel']]
    nomenklatura['datasozdanija'] = nomenklatura['datasozdanija'].replace('', '31.12.2021')
    nomenklatura['datasozdanija'] = pd.to_datetime(nomenklatura['datasozdanija'], format='%d.%m.%Y', errors='coerce')
    nomenklatura['datasozdanija'] = nomenklatura['datasozdanija'].fillna(pd.Timestamp('2021-12-31'))

    # Фильтрация и выбор колонок для prodazhi
    prodazhi = prodazhi[['kod', 'kolichestvo', 'period']]
    prodazhi['datasales'] = pd.to_datetime(prodazhi['period'], format='%d.%m.%Y', errors='coerce')
    prodazhi.drop(columns=['period'], inplace=True)

    # Преобразование kolichestvo в строки, замена запятых на точки, замена 'None' и NaN на 0 и преобразование в float для prodazhi
    prodazhi['kolichestvo'] = prodazhi['kolichestvo'].astype(str).str.replace(',', '.').replace('None', '0').fillna('0').astype(float)

    # Группировка prodazhi по datasales и kod с суммированием kolichestvo
    prodazhi = prodazhi.groupby(['datasales', 'kod']).agg({'kolichestvo': 'sum'}).reset_index()

    # Фильтрация и выбор колонок для postuplenija
    postuplenija = postuplenija[postuplenija['proveden'] == 'Да']
    postuplenija = postuplenija[['kod', 'kolichestvo', 'data']]
    postuplenija['data'] = pd.to_datetime(postuplenija['data']).dt.strftime('%d.%m.%Y')
    postuplenija['data'] = pd.to_datetime(postuplenija['data'], format='%d.%m.%Y')

    # Преобразование kolichestvo в строки, замена запятых на точки, замена 'None' и NaN на 0 и преобразование в float для postuplenija
    postuplenija['kolichestvo'] = postuplenija['kolichestvo'].astype(str).str.replace(',', '.').replace('None', '0').fillna('0').astype(float)

    # Группировка postuplenija по kod и data с суммированием kolichestvo
    postuplenija = postuplenija.groupby(['kod', 'data']).agg({'kolichestvo': 'sum'}).reset_index()

    # Преобразование stockendmonth в вертикальный формат
    stockendmonth_long = pd.melt(stockendmonth, id_vars=['nomenklaturakod'], var_name='date', value_name='balance')

    # Переименование колонки nomenklaturakod в kod
    stockendmonth_long.rename(columns={'nomenklaturakod': 'kod'}, inplace=True)

    # Проверка значений в колонке 'date' и преобразование их в строки
    stockendmonth_long['date'] = stockendmonth_long['date'].apply(lambda x: str(x) if not pd.isna(x) else '')

    # Преобразование значений в колонке 'date' в формат datetime
    stockendmonth_long['date'] = pd.to_datetime(stockendmonth_long['date'], format='%Y-%m', errors='coerce')

    # Переименование колонки kod_1s в kod в таблице groupanalogiold
    groupanalogiold.rename(columns={'kod_1s': 'kod'}, inplace=True)

    # Объединение таблиц
    nomenklatura_merged = pd.merge(nomenklaturaold, nomenklatura, on='kod', how='left')
    nomenklatura_merged = pd.merge(nomenklatura_merged, stockold, on='kod', how='left').fillna(0)
    nomenklatura_merged = pd.merge(nomenklatura_merged, priceold, on='kod', how='left').fillna(0)
    nomenklatura_merged = pd.merge(nomenklatura_merged, groupanalogiold, on='kod', how='left')
    nomenklatura_merged = pd.merge(nomenklatura_merged, typedetailgen, on='kod', how='left')

    return nomenklatura_merged, stockendmonth_long, postuplenija, prodazhi

