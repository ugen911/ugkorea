import pandas as pd
import numpy as np
import logging

from ugkorea.db.database import get_db_engine
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


engine = get_db_engine()

import pandas as pd
import numpy as np

def clean_and_convert_summa(df):
    # Удаляем пробелы, которые используются как разделители тысяч, и заменяем запятые на точки
    df['summa'] = df['summa'].astype(str).str.replace('\s', '', regex=True).str.replace(',', '.')
    # Преобразуем в числовой тип данных
    df['summa'] = pd.to_numeric(df['summa'], errors='coerce').fillna(0)
    return df

def perform_abc_xyz_analysis(engine):
    # Загрузка данных из таблиц
    prodazhi_df = pd.read_sql_table('prodazhi', con=engine)
    nomenklatura_df = pd.read_sql_table('nomenklatura', con=engine)

    # Удаление пробелов и нежелательных символов в колонке kod
    prodazhi_df['kod'] = prodazhi_df['kod'].str.strip()
    nomenklatura_df['kod'] = nomenklatura_df['kod'].str.strip()

    # Фильтрация nomenklatura по vidnomenklatury == "Товар"
    filtered_nomenklatura = nomenklatura_df[nomenklatura_df['vidnomenklatury'] == "Товар"][['kod']]

    # Фильтрация prodazhi на основе kod из отфильтрованной nomenklatura
    filtered_prodazhi = prodazhi_df[prodazhi_df['kod'].isin(filtered_nomenklatura['kod'])].copy()

    # Преобразование kolichestvo и summa
    filtered_prodazhi['kolichestvo'] = pd.to_numeric(filtered_prodazhi['kolichestvo'], errors='coerce').fillna(0)
    filtered_prodazhi = clean_and_convert_summa(filtered_prodazhi)

    # Преобразование period в дату
    filtered_prodazhi['period'] = pd.to_datetime(filtered_prodazhi['period'], format='%d.%m.%Y')
    filtered_prodazhi.to_csv("filtered_prodazhi.csv")
    # Фильтрация данных за последние 13 месяцев от текущей даты
    current_date = pd.Timestamp.now()
    last_13_months = current_date - pd.DateOffset(months=13)
    last_13_months_data = filtered_prodazhi[filtered_prodazhi['period'] >= last_13_months]

    # ABC анализ по сумме
    total_summa = last_13_months_data.groupby('kod')['summa'].sum()
    total_summa_sorted = total_summa.sort_values(ascending=False)
    cumulative_sum = total_summa_sorted.cumsum()
    total_cumulative_sum = cumulative_sum.iloc[-1]

    # Определение ABC категорий
    abc_labels = pd.cut(cumulative_sum, 
                        bins=[0, 0.05 * total_cumulative_sum, 0.8 * total_cumulative_sum, 0.95 * total_cumulative_sum, total_cumulative_sum], 
                        labels=['A1', 'A', 'B', 'C'])

    # XYZ анализ по количеству в процентном соотношении
    total_kolichestvo = last_13_months_data.groupby('kod')['kolichestvo'].sum()
    total_kolichestvo_sorted = total_kolichestvo.sort_values(ascending=False)
    cumulative_kolichestvo = total_kolichestvo_sorted.cumsum()
    total_cumulative_kolichestvo = cumulative_kolichestvo.iloc[-1]

    # Определение XYZ категорий аналогично ABC анализу
    xyz_labels = pd.cut(cumulative_kolichestvo, 
                        bins=[0, 0.05 * total_cumulative_kolichestvo, 0.8 * total_cumulative_kolichestvo, 0.95 * total_cumulative_kolichestvo, total_cumulative_kolichestvo], 
                        labels=['X1', 'X', 'Y', 'Z'])

    # Объединение ABC и XYZ анализов в один DataFrame
    abc_xyz_analysis = pd.DataFrame({
        'ABC': abc_labels,
        'XYZ': xyz_labels
    }, index=total_summa_sorted.index)

    # Проверка и заполнение категорий для товаров, которые были проданы хотя бы раз
    sold_items = last_13_months_data['kod'].unique()
    for item in sold_items:
        if item not in abc_xyz_analysis.index:
            abc_xyz_analysis.loc[item] = {'ABC': 'C', 'XYZ': 'Z'}

    return abc_xyz_analysis



statistic = perform_abc_xyz_analysis(engine=engine)