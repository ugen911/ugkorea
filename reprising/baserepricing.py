from ugkorea.reprising.basefiltering import prepare_filtered_data
from ugkorea.db.database import get_db_engine
from ugkorea.reprising.notapireprice import not_api_calculate_new_prices

# Подключаемся к базе данных
engine = get_db_engine()

# Получаем отфильтрованные данные и остальные таблицы
filtered_df, priceendmonth, salespivot, stockendmonth, suppliespivot, postuplenija = (
    prepare_filtered_data(engine)
)

# Пересчитываем цены для прайсов от поставщиков
filtered_df = not_api_calculate_new_prices(filtered_df, salespivot, base_percent=1.53, reduced_base_percent=1.4)

filtered_df.to_csv("filtered_df.csv")
