from sqlalchemy import create_engine
import pandas as pd

# Параметры подключения к базе данных
db_user = 'postgres'
db_password = '89232808797'  # Замените на ваш реальный пароль
db_host = 'localhost'
db_port = '5432'
db_name = 'ugkorea'
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

# Создание движка SQLAlchemy
engine = create_engine(db_url)

# Имена таблиц
table_names = [
    'berg', 'shateekat', 'shatepodolsk', 'favorit', 'forumcenter', 'forumnvs', 'tiss'
]

try:
    # Подключение к базе данных
    with engine.connect() as connection:
        print("Подключение к базе данных прошло успешно.")

        # Получаем максимальную дату из всех таблиц
        max_dates = []
        for table_name in table_names:
            max_date_query = f"SELECT MAX(дата) AS max_date FROM {table_name}"
            max_date = pd.read_sql(max_date_query, connection)['max_date'].max()
            max_dates.append(max_date)
        global_max_date = max(max_dates)

        # Формирование и выполнение запросов для объединения данных
        df_list = []
        for table_name in table_names:
            data_query = f"SELECT * FROM {table_name} WHERE дата = '{global_max_date}'"
            df_partial = pd.read_sql(data_query, connection)
            df_list.append(df_partial)

        # Объединение всех частей датафрейма
        final_df = pd.concat(df_list, ignore_index=True)

except Exception as e:
    print(f"Ошибка: {e}")

print(final_df)
