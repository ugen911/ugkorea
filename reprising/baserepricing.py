from ugkorea.reprising.basefiltering import prepare_filtered_data
from ugkorea.db.database import get_db_engine
from ugkorea.reprising.notapireprice import not_api_calculate_new_prices
from ugkorea.reprising.apireprice import calculate_new_prices_for_api
from ugkorea.reprising.uppricebyclass import adjust_prices_by_class
# Подключаемся к базе данных
engine = get_db_engine()

# Получаем отфильтрованные данные и остальные таблицы
filtered_df, priceendmonth, salespivot, stockendmonth, suppliespivot, postuplenija = (
    prepare_filtered_data(engine)
)

# Пересчитываем цены для прайсов от поставщиков
filtered = not_api_calculate_new_prices(filtered_df, salespivot, base_percent=1.53, reduced_base_percent=1.45)

# Базовая наценка от api
df = calculate_new_prices_for_api(
    filtered, salespivot, suppliespivot, base_percent=1.6, reduced_base_percent=1.45
)

# Регулируем цену относительно медианной цены классов которые процениваются по поставщикам и api у тех позиций который вообще new_price не рассчитался
df_new = adjust_prices_by_class(
    filtered_df=df, salespivot=salespivot, suppliespivot=suppliespivot
)
df_new.to_csv("filtered_df.csv")


# Находим позиции от которых нужно избавиться (ДАта создания отсутствует или старше 2 лет, нет поступлений последний год) уже рассматриваем позиции с new_price и смотрим чтобы new_price не улетал от Median по классу выше чем на 50% если нет продаж и покупок
# если есть  продажа в какой-то из месяцев и одна или 2 штуки то не выше чем на 70% если есть продажи стабильно от 2 месяцев то считаем цену от 70% от median_price

# Проверить если у товаров с A A1 при этом X1 X поднятие цены за последние 3 года коррелирует с падением продаж при этом наценка относительно median_price > 50%
# то пробуем снижать цену не протеворечит проверки на максимум и среднюю


# ПРоверяем изменение цен по годам priceendmonth и stockendmonth должны быть положительные если все таки new_price до сих под пуст должны обеспечить индексацию товаров на 7% год если товар не дорогостоящий не выше 5000 рублей в розницу 

# Преверяем цены у конкурентов, пытаемся подняться или спуститься под них

# Проверяем группы аналогов где есть продающиеся позиции и нет, те что не продаются выносим в отдельный список, выгружаем в папку формируем минимальные цены на распродажу, данный список может только пополняться не может убывать, делай через таблицу в БД

# Проверяем в группах аналогов есть ли оригиналы с разными номерами и одинаковым брендом по очень разной цене, пробуем уменьшить цену в верхней позиции.
# Так же проверяем чтобы аналог не мог стоить дороже оригинала
