from ugkorea.reprising.basefiltering import prepare_filtered_data
from ugkorea.db.database import get_db_engine
from ugkorea.reprising.notapireprice import not_api_calculate_new_prices
from ugkorea.reprising.apireprice import calculate_new_prices_for_api
# Подключаемся к базе данных
engine = get_db_engine()

# Получаем отфильтрованные данные и остальные таблицы
filtered_df, priceendmonth, salespivot, stockendmonth, suppliespivot, postuplenija = (
    prepare_filtered_data(engine)
)

# Пересчитываем цены для прайсов от поставщиков
filtered = not_api_calculate_new_prices(filtered_df, salespivot, base_percent=1.53, reduced_base_percent=1.45)
df = calculate_new_prices_for_api(
    filtered, salespivot, suppliespivot, base_percent=1.6, reduced_base_percent=1.45
)
df.to_csv("filtered_df.csv")
