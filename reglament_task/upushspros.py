import os
import re
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from transliterate import translit
from openpyxl import load_workbook
from openpyxl.styles import Alignment
from inputimeout import inputimeout, TimeoutOccurred
from db.database import get_db_engine



def fetch_table_data(engine, table_name):
    try:
        query = text(f"SELECT * FROM {table_name}")
        with engine.connect() as connection:
            result = pd.read_sql(query, connection)
        return result
    except SQLAlchemyError as e:
        print(f"Ошибка при выполнении запроса к таблице {table_name}: {e}")
        return None

def find_analogs(row, df):
    if pd.notna(row['gruppa_analogov']):
        analogs = df[(df['gruppa_analogov'] == row['gruppa_analogov']) & (df['osnsklad'] > 0)]
        if not analogs.empty:
            return ', '.join(analogs['kod'].astype(str) + ' - ' + analogs['proizvoditel'].astype(str) + ' - ' + analogs['osnsklad'].astype(str))
    return None    

def analyze_data(data, period_weeks, months=12):
    end_date = datetime.now()
    start_date_months = end_date - timedelta(days=months*30)
    start_date_weeks = end_date - timedelta(weeks=period_weeks)
    
    data['period'] = pd.to_datetime(data['period'], format='%d.%m.%Y')

    filtered_data_weeks = data[(data['period'] >= start_date_weeks) & (data['period'] <= end_date)]
    unique_codes_weeks = filtered_data_weeks['kod'].unique()

    filtered_data_months = data[(data['period'] >= start_date_months) & (data['period'] <= end_date)]
    filtered_data_months = filtered_data_months[filtered_data_months['kod'].isin(unique_codes_weeks)]

    grouped_data_months = filtered_data_months.groupby('kod').agg({
        'period': ['count', lambda x: ', '.join(sorted(x.dt.strftime('%d.%m.%Y')))]
    }).reset_index()
    grouped_data_months.columns = ['kod', 'Запросы (месяцы)', 'Даты запросов (месяцы)']

    grouped_data_weeks = filtered_data_weeks.groupby('kod').agg({
        'period': ['count', lambda x: ', '.join(sorted(x.dt.strftime('%d.%m.%Y')))]
    }).reset_index()
    grouped_data_weeks.columns = ['kod', 'Запросы (недели)', 'Даты запросов (недели)']

    result = pd.merge(grouped_data_months, grouped_data_weeks, on='kod', how='inner')
    result = result.sort_values(by=['Запросы (месяцы)', 'kod'], ascending=[False, True])
    
    return result

def process_article(article):
    if pd.isna(article):
        return article
    article = re.sub(r'[^\w\s]', '', article)
    article = translit(article, 'ru', reversed=True)
    return article

def check_presence_weeks_in_months(row, filtered_data_months):
    article = row['period']
    matches = filtered_data_months[filtered_data_months['period'] == article]
    if not matches.empty:
        dates = ', '.join(matches['period'].dt.strftime('%d.%m.%Y'))
        return pd.Series([len(matches), dates])
    return pd.Series([0, ''])

def adjust_worksheet(worksheet):
    for row in worksheet.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True)
    
    for col in worksheet.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(cell.value)
            except:
                pass
        adjusted_width = (max_length + 2)
        worksheet.column_dimensions[column].width = adjusted_width

def get_user_input(prompt, default_value, timeout=5):
    try:
        user_input = inputimeout(prompt=prompt, timeout=timeout)
        if user_input.strip() == "":
            return default_value
        return int(user_input)
    except TimeoutOccurred:
        return default_value
    except ValueError:
        print(f"Неверный ввод. Используется значение по умолчанию: {default_value}")
        return default_value

def main():
    months = get_user_input("Введите количество месяцев (по умолчанию 12): ", 12)
    weeks = get_user_input("Введите количество недель (по умолчанию 1): ", 1)
    output_file = r'\\26.218.196.12\заказы\Евгений\УпущенныйСпрос.xlsx'
    
    engine = get_db_engine()

    if engine:
        nomenklatura_df = fetch_table_data(engine, 'nomenklaturaold')
        price_df = fetch_table_data(engine, 'priceold')
        stock_df = fetch_table_data(engine, 'stockold')
        groupanalogi_df = fetch_table_data(engine, 'groupanalogiold')

        if nomenklatura_df is not None and price_df is not None and stock_df is not None and groupanalogi_df is not None:
            nomenklatura_df = nomenklatura_df.drop_duplicates(subset=['kod'])
            price_df = price_df.drop_duplicates(subset=['kod'])
            stock_df = stock_df.drop_duplicates(subset=['kod'])
            groupanalogi_df = groupanalogi_df.drop_duplicates(subset=['kod_1s', 'gruppa_analogov'])
            
            nomenklatura_df['kod'] = nomenklatura_df['kod'].astype(str).str.strip()
            price_df['kod'] = price_df['kod'].astype(str).str.strip()
            stock_df['kod'] = stock_df['kod'].astype(str).str.strip()
            groupanalogi_df['kod_1s'] = groupanalogi_df['kod_1s'].astype(str).str.strip()
            
            merged_df = nomenklatura_df.merge(price_df[['kod', 'tsenazakup','tsenarozn']], on='kod', how='left')
            merged_df = merged_df.merge(stock_df[['kod', 'osnsklad']], on='kod', how='left')
            merged_df = merged_df.merge(groupanalogi_df[['kod_1s', 'gruppa_analogov']], left_on='kod', right_on='kod_1s', how='left')
            merged_df.drop(columns=['kod_1s'], inplace=True)

            actual_price = merged_df
            data = fetch_table_data(engine, 'upuschennyjspros')

            actual_price['kod'] = actual_price['kod'].str.strip().astype(str)
            actual_price['Аналоги'] = actual_price.apply(lambda row: find_analogs(row, actual_price), axis=1)

            end_date = datetime.now()
            start_date = end_date - timedelta(days=months*30)
            data['period'] = pd.to_datetime(data['period'], format='%d.%m.%Y')
            filtered_data = data[(data['period'] >= start_date) & (data['period'] <= end_date)]
            grouped_data = filtered_data.groupby('kod').agg({
                'period': ['count', lambda x: ', '.join(x.dt.strftime('%d.%m.%Y'))]
            }).reset_index()
            grouped_data.columns = ['kod', 'Запросы', 'Даты запросов']
            grouped_data = grouped_data.sort_values(by=['Запросы', 'kod'], ascending=[False, True])

            filtered_actual_price = actual_price[actual_price['osnsklad'] == 0].copy()
            grouped_data['kod'] = grouped_data['kod'].str.strip()
            filtered_actual_price['kod'] = filtered_actual_price['kod'].str.strip()
            merged_data = pd.merge(grouped_data, filtered_actual_price, left_on='kod', right_on='kod', how='inner')

            union_spros = merged_data
            spros_weeks = analyze_data(data, period_weeks=weeks, months=months)
            spros_weeks['kod'] = spros_weeks['kod'].str.strip().astype(str)
            merged_data = pd.merge(spros_weeks, actual_price, on='kod', how='left')
            weeks_spros = merged_data.drop(columns=['pometkaudalenija', 'osnsklad', 'gruppa_analogov'])

            data.loc[data['kod'].isna(), 'artikuldljapoiska'] = data.loc[data['kod'].isna(), 'artikuldljapoiska'].apply(process_article)
            actual_price_cleaned = actual_price['artikul'].apply(lambda x: re.sub(r'[^\w\s]', '', str(x)))
            data['artikuldljapoiska_cleaned'] = data['artikuldljapoiska'].apply(lambda x: re.sub(r'[^\w\s]', '', str(x)) if pd.notna(x) else x)
            articul_set = set(actual_price_cleaned)
            data['exists_in_actual_price'] = data['artikuldljapoiska_cleaned'].apply(lambda x: x in articul_set if pd.notna(x) else False)
            data_filtered = data[data['kod'].isna() & ~data['exists_in_actual_price']].drop(columns=['exists_in_actual_price', 'kod'])
            data_filtered = data_filtered[['period', 'kolichestvo', 'avtor', 'artikuldljapoiska_cleaned']]
            data_filtered = data_filtered.rename(columns={'artikuldljapoiska_cleaned': 'artikul'})
            data_filtered['period'] = pd.to_datetime(data_filtered['period'], format='%d.%m.%Y')

            end_date = data_filtered['period'].max()
            start_date = end_date - pd.DateOffset(months=months)
            filtered_data = data_filtered[(data_filtered['period'] >= start_date) & (data_filtered['period'] <= end_date)]
            grouped_data = filtered_data.groupby('artikul').agg(
                Запросы=('artikul', 'count'),
                Даты_запросов=('period', lambda x: ', '.join(x.dt.strftime('%d.%m.%Y')))
            ).reset_index()
            grouped_data = grouped_data.sort_values(by='Запросы', ascending=False)

            end_date_weeks = data_filtered['period'].max()
            start_date_weeks = end_date_weeks - pd.DateOffset(weeks=weeks)
            filtered_data_weeks = data_filtered[(data_filtered['period'] >= start_date_weeks) & (data_filtered['period'] <= end_date_weeks)]
            start_date_months = end_date_weeks - pd.DateOffset(months=months)
            filtered_data_months = data_filtered[(data_filtered['period'] >= start_date_months) & (data_filtered['period'] <= end_date_weeks)]

            result = filtered_data_weeks.copy()
            result[['Количество за месяцы', 'Даты запросов за месяцы']] = result.apply(lambda row: check_presence_weeks_in_months(row, filtered_data_months), axis=1)
            result = result[['period', 'kolichestvo', 'avtor', 'artikul', 'Количество за месяцы', 'Даты запросов за месяцы']]

            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                union_spros.to_excel(writer, sheet_name='общий', index=False)
                weeks_spros.to_excel(writer, sheet_name='запериод', index=False)
                grouped_data.to_excel(writer, sheet_name='НПобщий', index=False)
                result.to_excel(writer, sheet_name='НПпериод', index=False)
                actual_price.to_excel(writer, sheet_name='ОбщийПрайссгруппами', index=False)

            wb = load_workbook(output_file)
            for sheet_name in wb.sheetnames:
                adjust_worksheet(wb[sheet_name])
            wb.save(output_file)
            print(f"Файл успешно создан и сохранен как '{output_file}'")

if __name__ == "__main__":
    main()
