from ugkorea.reprising.basefiltering import prepare_filtered_data
from ugkorea.db.database import get_db_engine
from ugkorea.reprising.notapireprice import not_api_calculate_new_prices
from ugkorea.reprising.apireprice import calculate_new_prices_for_api
from ugkorea.reprising.uppricebyclass import adjust_prices_by_class
from ugkorea.reprising.nonliquid import adjust_new_price_for_non_liquid, adjust_prices_without_delprice
from ugkorea.reprising.inflation import indeksation
from ugkorea.reprising.analogkonkurentbalance import main as rebalance
import numpy as np
import pandas as pd
import openpyxl

# Подключаемся к базе данных
engine = get_db_engine()

# Получаем отфильтрованные данные и остальные таблицы
filtered_df, priceendmonth, salespivot, stockendmonth, suppliespivot, postuplenija = (
    prepare_filtered_data(engine)
)
print("Пересчитываем цены для прайсов от поставщиков")
# Пересчитываем цены для прайсов от поставщиков
filtered = not_api_calculate_new_prices(filtered_df, salespivot, base_percent=1.53, reduced_base_percent=1.45)
print("Базовая наценка от api")
# filtered.to_csv("filtered_df.csv")
# Базовая наценка от api
df = calculate_new_prices_for_api(
    filtered, salespivot, suppliespivot, base_percent=1.6, reduced_base_percent=1.45
)

# df.to_csv("filtered_df.csv")


# print("adjust_prices_by_class")
# Регулируем цену относительно медианной цены классов которые процениваются по поставщикам и api у тех позиций который вообще new_price не рассчитался
df_new = adjust_prices_by_class(filtered_df=df, salespivot=salespivot, suppliespivot=suppliespivot)
# df_new.to_csv("filtered_df.csv")

# Находим позиции от которых нужно избавиться (ДАта создания отсутствует или старше 2 лет, нет поступлений последние 18 месяцев) уже рассматриваем позиции с new_price есть и смотрим чтобы new_price не улетал от Median по классу ориг неориг выше чем на 50% если нет продаж и покупок
# если есть  продажа в какой-то из месяцев и одна или 2 штуки то не выше чем на 70% если есть продажи стабильно от 2 месяцев то считаем цену от 70% от median_price. с new_price есть,  delprice или median_price есть api в sklad есть
# - расписать условия нет поступлений 12 месяцев и какие продажи
# - нет поступлений 24 месяца и больше и продажи какие  распиши вилки
print('nonliquid')
res = adjust_new_price_for_non_liquid(df_new, salespivot)
df_1 = adjust_prices_without_delprice(res)
# df_1.to_csv("filtered_df.csv")
# Проверить если у товаров с A A1 при этом X1 X поднятие цены за последние 3 года коррелирует с падением продаж при этом наценка относительно median_price > 50%
# то пробуем снижать цену не протеворечит проверки на максимум и среднюю. Можно рассчитать сезонку по медиане и +- от медианы по каждому месяцу общих продаж учесть сезонность и если падение продаж при повышении цены значительно выше сезонки то значит есть зависимость от цены

# ПРоверяем изменение цен по месяцам в priceendmonth и stockendmonth должны быть положительные если все таки new_price до сих под пуст должны обеспечить индексацию товаров на 7% год если товар не дорогостоящий не выше 5000 рублей в розницу если выше пусть считает 3 наценки считает смотря на изменение цен в priceandmonth и нацениеваем на срок
print("inflation")
df_2 = indeksation(df_1, priceendmonth)
# df_2.to_csv("filtered_df.csv")

# Преверяем цены у конкурентов, пытаемся подняться или спуститься под них

# Проверяем группы аналогов где есть продающиеся позиции и нет, те что не продаются выносим в отдельный список, выгружаем в папку формируем минимальные цены на распродажу, данный список может только пополняться не может убывать, делай через таблицу в БД

# Проверяем в группах аналогов есть ли оригиналы с разными номерами и одинаковым брендом по очень разной цене, пробуем уменьшить цену в верхней позиции.
# Так же проверяем чтобы аналог не мог стоить дороже оригинала
print('rebalace...')
df_3 = rebalance(df_2, engine=engine)
# df_3.to_csv("filtered_df.csv")
# Сделать увеличенную наценку если товар продается только в сервис добавить + вилку в цене на товары до 2000р


def regtament_view(filtered_df):
    """
    Функция для обработки filtered_df, оставляет колонки в заданной последовательности,
    вычисляет наценку и разницу цен, добавляет их в отдельные колонки, сортирует данные,
    фильтрует строки, где price_diff не равен 0, и добавляет итоговую сумму в последнюю строку.

    Parameters:
    filtered_df (pd.DataFrame): Основной датафрейм с информацией о позициях.

    Returns:
    pd.DataFrame: Обработанный датафрейм с дополнительными колонками.
    """
    # Задаем последовательность колонок
    columns_order = [
        "kod",
        "artikul",
        "proizvoditel",
        "gruppa_analogov",
        "naimenovanie",
        "edizm",
        "delprice",
        "delsklad",
        "median_price",
        "middleprice",
        "maxprice",
        "tsenazakup",
        "konkurents",
        "ostatok",
        "abc",
        "xyz",
        "tsenarozn",
        "new_price",
    ]

    # Оставляем только нужные колонки и в указанном порядке
    filtered_df = filtered_df[columns_order].copy()

    # Вычисляем наценку (new_price к delprice), если delprice не пустой
    filtered_df.loc[:, "markup_to_delprice"] = filtered_df.apply(
        lambda row: (
            (row["new_price"] - row["delprice"]) / row["delprice"] * 100
            if pd.notna(row["delprice"]) and row["delprice"] > 0
            else np.nan
        ),
        axis=1,
    )

    # Вычисляем фактическую наценку (new_price к tsenazakup), если tsenazakup не пустой
    filtered_df.loc[:, "fact_markup"] = filtered_df.apply(
        lambda row: (
            (row["new_price"] - row["tsenazakup"]) / row["tsenazakup"] * 100
            if pd.notna(row["tsenazakup"]) and row["tsenazakup"] > 0
            else np.nan
        ),
        axis=1,
    )

    # Вычисляем разницу new_price - tsenarozn
    filtered_df.loc[:, "price_diff"] = (
        filtered_df["new_price"] - filtered_df["tsenarozn"]
    )

    # Вычисляем процентное отклонение (new_price - tsenarozn) / tsenarozn * 100
    filtered_df.loc[:, "percent_diff"] = filtered_df.apply(
        lambda row: (
            (row["new_price"] - row["tsenarozn"]) / row["tsenarozn"] * 100
            if pd.notna(row["tsenarozn"]) and row["tsenarozn"] > 0
            else np.nan
        ),
        axis=1,
    )

    # Вычисляем разницу (new_price - tsenarozn) * ostatok
    filtered_df.loc[:, "price_diff_mult_ostatok"] = (
        filtered_df["price_diff"] * filtered_df["ostatok"]
    )

    # Сортируем по naimenovanie, затем по gruppa_analogov, и затем по proizvoditel
    filtered_df = filtered_df.sort_values(
        by=["naimenovanie", "gruppa_analogov", "proizvoditel"]
    )

    # Фильтруем только те строки, где price_diff не равен 0
    filtered_df = filtered_df[filtered_df["price_diff"] != 0]

    # Вычисляем сумму по колонке price_diff_mult_ostatok
    total_sum = filtered_df["price_diff_mult_ostatok"].sum()
    print(f"Total sum of price_diff_mult_ostatok: {total_sum}")

    # Добавляем строку с итоговой суммой в конец датафрейма
    total_sum_row = {col: "" for col in filtered_df.columns}
    total_sum_row["kod"] = "Итоговая сумма"
    total_sum_row["price_diff_mult_ostatok"] = total_sum

    # Присоединяем итоговую строку к датафрейму
    filtered_df = pd.concat(
        [filtered_df, pd.DataFrame([total_sum_row])], ignore_index=True
    )

    return filtered_df


regtament_views = regtament_view(filtered_df=df_3)
regtament_views.to_excel("regtament_views.xlsx", index=False)
