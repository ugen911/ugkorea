import pandas as pd
import os

# Подключаемся к базе данных и загружаем таблицу
from ugkorea.db.database import get_db_engine

engine = get_db_engine()
query = "SELECT * FROM deliveryminprice"
deliveryminprice_df = pd.read_sql(query, engine)

# Пути для сохранения файла
network_path = (
    r"\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\deliveryminprice.xlsx"
)
local_path = r"D:\NAS\заказы\Евгений\New\Файлы для работы Access\deliveryminprice.xlsx"


# Функция для сохранения файла по доступному пути
def save_to_available_path(df, paths):
    for path in paths:
        directory = os.path.dirname(path)
        if os.path.exists(directory):
            df.to_excel(path, index=False)
            print(f"Файл успешно сохранен по адресу: {path}")
            return
    print("Не удалось сохранить файл. Все указанные пути недоступны.")


# Сохраняем файл по доступному пути
save_to_available_path(deliveryminprice_df, [network_path, local_path])
