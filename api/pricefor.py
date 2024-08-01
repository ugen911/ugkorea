from ugkorea.db.database import get_db_engine
import pandas as pd

def get_final_df():
    """
    Функция для получения объединенного и отфильтрованного DataFrame из таблиц nomenklaturaold и stockold.
    """
    # Получаем объект подключения к базе данных
    engine = get_db_engine()

    # Выгрузка данных из таблиц
    nomenklaturaold_df = pd.read_sql_table('nomenklaturaold', engine)
    stockold_df = pd.read_sql_table('stockold', engine)

    # Фильтруем строки, где pometkaudalenija имеет значение 'Нет'
    nomenklaturaold_filtered = nomenklaturaold_df[nomenklaturaold_df['pometkaudalenija'] == 'Нет']

    # Преобразуем столбец osnsklad в тип данных float
    stockold_df['osnsklad'] = stockold_df['osnsklad'].astype(float)

    # Выполняем внутреннее соединение по полю 'kod'
    merged_df = pd.merge(nomenklaturaold_filtered, stockold_df, on='kod', how='inner')

    # Оставляем только необходимые колонки и фильтруем по osnsklad > 0
    final_df = merged_df[['kod', 'artikul', 'proizvoditel', 'osnsklad']]
    final_df = final_df[final_df['osnsklad'] > 0]

    # Переименовываем столбец osnsklad в stock
    final_df = final_df.rename(columns={'osnsklad': 'stock'})

    return final_df

if __name__ == "__main__":
    df = get_final_df()
    print("Final Table Head:")
    print(df.head())
