from ugkorea.db.database import get_db_engine
import pandas as pd

# Подключаемся к базе данных
engine = get_db_engine()

# Определяем запросы для получения данных из каждой таблицы
query_nomenklatura = "SELECT * FROM public.nomenklatura"
query_postuplenija = "SELECT * FROM public.postuplenija"
query_tsenynakonetsmesjatsa = "SELECT * FROM public.tsenynakonetsmesjatsa"
query_priceold = "SELECT * FROM public.priceold"

# Загружаем данные в DataFrame
nomenklatura_df = pd.read_sql(query_nomenklatura, engine)
postuplenija_df = pd.read_sql(query_postuplenija, engine)
tsenynakonetsmesjatsa_df = pd.read_sql(query_tsenynakonetsmesjatsa, engine)
priceold_df = pd.read_sql(query_priceold, engine)

# Выводим типы данных каждой колонки
print("Типы данных для nomenklatura:")
print(nomenklatura_df.dtypes)
print("\nТипы данных для postuplenija:")
print(postuplenija_df.dtypes)
print("\nТипы данных для tsenynakonetsmesjatsa:")
print(tsenynakonetsmesjatsa_df.dtypes)
print("\nТипы данных для priceold:")
print(priceold_df.dtypes)

# Сохраняем первые 1000 строк в CSV файлы
nomenklatura_df.head(1000).to_csv("nomenklatura.csv", index=False)
postuplenija_df.head(1000).to_csv("postuplenija.csv", index=False)
tsenynakonetsmesjatsa_df.head(1000).to_csv("tsenynakonetsmesjatsa.csv", index=False)
priceold_df.head(1000).to_csv("priceold.csv", index=False)

# Выводим первые 20 строк в терминал
print("\nПервые 20 строк для nomenklatura:")
print(nomenklatura_df.head(20))
print("\nПервые 20 строк для postuplenija:")
print(postuplenija_df.head(20))
print("\nПервые 20 строк для tsenynakonetsmesjatsa:")
print(tsenynakonetsmesjatsa_df.head(20))
print("\nПервые 20 строк для priceold:")
print(priceold_df.head(20))
