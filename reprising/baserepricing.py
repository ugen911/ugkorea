from ugkorea.reprising.basefiltering import prepare_filtered_data
from ugkorea.db.database import get_db_engine
from ugkorea.reprising.notapireprice import not_api_calculate_new_prices
from ugkorea.reprising.apireprice import calculate_new_prices_for_api
from ugkorea.reprising.uppricebyclass import adjust_prices_by_class
from ugkorea.reprising.nonliquid import adjust_new_price_for_non_liquid, adjust_prices_without_delprice, adjust_new_price_by_peer_median
from ugkorea.reprising.inflation import indeksation
from ugkorea.reprising.analogkonkurentbalance import main as rebalance
from ugkorea.reprising.upload import regtament_view, export_data
from ugkorea.reprising.loaddata import (
    exclude_kods_from_file,
    load_brands_and_text,
    filter_dataframe,
)
from ugkorea.reprising.dopcorrect import correct_new_price_filters
from ugkorea.reprising.minmiddleprice import adjust_new_price as minmiddle

# Подключаемся к базе данных
engine = get_db_engine()

# Получаем отфильтрованные данные и остальные таблицы
filtered_df, priceendmonth, salespivot, stockendmonth, suppliespivot, postuplenija = (
    prepare_filtered_data(engine)
)


print("Пересчитываем цены для прайсов от поставщиков")
# Пересчитываем цены для прайсов от поставщиков
filtered = not_api_calculate_new_prices(filtered_df, salespivot, base_percent=1.55, reduced_base_percent=1.5)
print("Базовая наценка от api")
#filtered.to_csv("filtered_not_api_df.csv")
# Базовая наценка от api
df = calculate_new_prices_for_api(filtered, salespivot, suppliespivot, base_percent=1.55, reduced_base_percent=1.5)
#df.to_csv("filtered_df_api.csv")


# print("adjust_prices_by_class")
# Регулируем цену относительно медианной цены классов которые процениваются по поставщикам и api у тех позиций который вообще new_price не рассчитался
df_new = adjust_prices_by_class(filtered_df=df, salespivot=salespivot, suppliespivot=suppliespivot)
# df_new.to_csv("filtered_df_by_class.csv")

# Находим позиции от которых нужно избавиться (ДАта создания отсутствует или старше 2 лет, нет поступлений последние 18 месяцев) уже рассматриваем позиции с new_price есть и смотрим чтобы new_price не улетал от Median по классу ориг неориг выше чем на 50% если нет продаж и покупок
# если есть  продажа в какой-то из месяцев и одна или 2 штуки то не выше чем на 70% если есть продажи стабильно от 2 месяцев то считаем цену от 70% от median_price. с new_price есть,  delprice или median_price есть api в sklad есть
# - расписать условия нет поступлений 12 месяцев и какие продажи
# - нет поступлений 24 месяца и больше и продажи какие  распиши вилки
print('nonliquid')
res = adjust_new_price_for_non_liquid(df_new, salespivot)
#df_new.to_csv("df_new_adjust_new_price_for_non_liquid.csv")
df_1 = adjust_prices_without_delprice(res)
gh = adjust_new_price_by_peer_median(df_1)
# df_1.to_csv("filtered_non_liquid__without_delprice_df.csv")
# Проверить если у товаров с A A1 при этом X1 X поднятие цены за последние 3 года коррелирует с падением продаж при этом наценка относительно median_price > 50%
# то пробуем снижать цену не протеворечит проверки на максимум и среднюю. Можно рассчитать сезонку по медиане и +- от медианы по каждому месяцу общих продаж учесть сезонность и если падение продаж при повышении цены значительно выше сезонки то значит есть зависимость от цены

# ПРоверяем изменение цен по месяцам в priceendmonth и stockendmonth должны быть положительные если все таки new_price до сих под пуст должны обеспечить индексацию товаров на 7% год если товар не дорогостоящий не выше 5000 рублей в розницу если выше пусть считает 3 наценки считает смотря на изменение цен в priceandmonth и нацениеваем на срок
print("inflation")
df_2 = indeksation(gh, priceendmonth)
# df_2.to_csv("filtered_df_indeksation.csv")

# Преверяем цены у конкурентов, пытаемся подняться или спуститься под них

# Проверяем группы аналогов где есть продающиеся позиции и нет, те что не продаются выносим в отдельный список, выгружаем в папку формируем минимальные цены на распродажу, данный список может только пополняться не может убывать, делай через таблицу в БД

# Проверяем в группах аналогов есть ли оригиналы с разными номерами и одинаковым брендом по очень разной цене, пробуем уменьшить цену в верхней позиции.
# Так же проверяем чтобы аналог не мог стоить дороже оригинала
print('rebalace...')
df_3 = rebalance(df_2, engine=engine)
# df_3.to_csv("filtered_df_rebalance_analog.csv")

df2 = correct_new_price_filters(df_3)
#df_3.to_csv("filtered_df_correct_new_price_filters.csv")

df4 = minmiddle(df2)
print('- brands, texts')
brand, text = load_brands_and_text()
k = filter_dataframe(df4, brand, text)

# Убираем позиции из датафрейма которые не надо переоценивать
f = exclude_kods_from_file(k)


reglament_views = regtament_view(filtered_df=f)
# regtament_views.to_excel("reglament_views.xlsx", index=False, freeze_panes=(1, 0))
export_data(reglament_views)
