import pandas as pd
from ugkorea.db.database import get_db_engine

# Подключение к базе данных
engine = get_db_engine()

# Получение всех данных из таблицы ostatkitovarovnakonetskazhdogomesjatsa
query_stock = """
SELECT 
    * 
FROM ostatkitovarovnakonetskazhdogomesjatsa
"""
stock_df = pd.read_sql(query_stock, engine)
stock_df.name = "stock"

# Получение всех данных из таблицы tsenynakonetsmesjatsa
query_price = """
SELECT 
    * 
FROM tsenynakonetsmesjatsa
"""
price_df = pd.read_sql(query_price, engine)
price_df.name = "price"

# Преобразование типов данных
stock_df['kolichestvo'] = stock_df['kolichestvo'].str.replace(',', '.').astype(float).fillna(0).astype(int)

# Преобразование типов данных для price_df
price_df['data'] = pd.to_datetime(price_df['data'], errors='coerce')
price_df['tsena'] = price_df['tsena'].str.replace('\xa0', '').str.replace(' ', '').str.replace(',', '.').astype(float)

# Сохранение первых 200 строк stock_df в CSV файл
stock_df.to_csv('stock.csv', index=False)

# Вывод первых 20 строк для проверки
print("\nПервые 20 строк данных stock_df после преобразования типов:")
print(stock_df.head(20))

print("\nПервые 20 строк данных price_df после преобразования типов:")
print(price_df.head(20))
