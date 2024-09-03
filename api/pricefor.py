from ugkorea.db.database import get_db_engine
import pandas as pd
import re

def get_final_df():
    """
    Функция для получения объединенного и отфильтрованного DataFrame из таблиц nomenklaturaold и stockold,
    исключая строки с производителем Noname.
    """
    # Получаем объект подключения к базе данных
    engine = get_db_engine()

    # Выгрузка данных из таблиц
    nomenklaturaold_df = pd.read_sql_table('nomenklaturaold', engine)
    stockold_df = pd.read_sql_table('stockold', engine)

    # Фильтруем строки, где pometkaudalenija имеет значение 'Нет', производитель не пустой и не равен 'Noname'
    nomenklaturaold_filtered = nomenklaturaold_df[
        (nomenklaturaold_df['pometkaudalenija'] == 'Нет') & 
        (nomenklaturaold_df['proizvoditel'].notna()) & 
        (nomenklaturaold_df['proizvoditel'] != '') &
        (nomenklaturaold_df['proizvoditel'] != 'Noname')
    ]

    # Преобразуем столбец osnsklad в тип данных float
    stockold_df['osnsklad'] = stockold_df['osnsklad'].astype(float)

    # Выполняем внутреннее соединение по полю 'kod'
    merged_df = pd.merge(nomenklaturaold_filtered, stockold_df, on='kod', how='inner')

    # Оставляем только необходимые колонки
    final_df = merged_df[['kod', 'artikul', 'proizvoditel', 'osnsklad']]

    # Фильтруем строки, где artikul содержит только латинские буквы и цифры
    final_df = final_df[final_df['artikul'].apply(lambda x: bool(re.match(r'^[A-Za-z0-9]+$', x)))]

    # Проверяем на наличие маленькой латинской буквы в конце артикула
    rows_to_add = []
    for index, row in final_df.iterrows():
        if re.match(r'.*[a-z]$', row['artikul']):
            # Создаем копию строки с артикула без последней буквы
            new_row = row.copy()
            new_row['artikul'] = new_row['artikul'][:-1]
            rows_to_add.append(new_row)

    # Добавляем новые строки в DataFrame
    if rows_to_add:
        final_df = pd.concat([final_df, pd.DataFrame(rows_to_add)], ignore_index=True)

    # Переименовываем столбец osnsklad в stock
    final_df = final_df.rename(columns={'osnsklad': 'stock'})

    return final_df

if __name__ == "__main__":
    df = get_final_df()
    print("Final Table Head:")
    print(df.head())
