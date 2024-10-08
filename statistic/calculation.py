import logging
from ugkorea.statistic.loaddata import get_final_data, load_and_process_data, perform_abc_xyz_analysis
from ugkorea.db.database import get_db_engine
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_additional_data(engine):
    # Получение текущей даты для фильтрации последних 13 месяцев
    current_date = datetime.now()
    thirteen_months_ago = (current_date - pd.DateOffset(months=13)).strftime("%Y-%m")

    # Загрузка данных из priceendmonth за последние 13 месяцев
    priceendmonth_query = f"""
        SELECT *
        FROM priceendmonth
        WHERE data >= '{thirteen_months_ago}'
    """
    priceendmonth = pd.read_sql(priceendmonth_query, engine)

    # Загрузка данных из stockendmonth за последние 13 месяцев
    stockendmonth_query = f"""
        SELECT *
        FROM stockendmonth
        WHERE month >= '{thirteen_months_ago}'
    """
    stockendmonth = pd.read_sql(stockendmonth_query, engine)
    stockendmonth.rename(columns={"nomenklaturakod": "kod"}, inplace=True)

    # Загрузка данных из suppliespivot за последние 13 месяцев
    suppliespivot_query = f"""
        SELECT *
        FROM suppliespivot
        WHERE year_month >= '{thirteen_months_ago}'
    """
    suppliespivot = pd.read_sql(suppliespivot_query, engine)

    # Загрузка данных из deliveryminprice
    deliveryminprice_query = "SELECT * FROM deliveryminprice"
    deliveryminprice = pd.read_sql(deliveryminprice_query, engine)

    return priceendmonth, stockendmonth, suppliespivot, deliveryminprice


def calculate_sales_metrics(
    sales_data: pd.DataFrame, union_data: pd.DataFrame, deliveryminprice: pd.DataFrame) -> pd.DataFrame:
    # Use the current date as the reference date
    reference_date = datetime.now()
    current_period = pd.Period(reference_date.strftime("%Y-%m"), freq="M")

    # Отладочная информация
    print("Начинаем расчет метрик продаж...")

    # Заполняем значения price текущего месяца из tsenarozn, если они пустые
    # Функция для заполнения текущего месяца значениями price и balance
    def fill_current_month_data(row):
        # Проверка условий для заполнения значения 'price'
        if (
            pd.isna(row["price"])
            and row["year_month"] == current_period
            and pd.notna(row["total_sales"])
            and pd.notna(row["balance"])
        ):
            kod = row["kod"]
            # Получаем значение tsenarozn из union_data для текущего kod
            price_value = union_data.loc[union_data["kod"] == kod, "tsenarozn"].values
            row["price"] = price_value[0] if len(price_value) > 0 else row["price"]

        # Проверка условий для заполнения значения 'balance'
        if row["year_month"] == current_period:
            kod = row["kod"]
            # Получаем значение osnsklad из union_data для текущего kod
            balance_value = union_data.loc[union_data["kod"] == kod, "osnsklad"].values
            if len(balance_value) > 0 and balance_value[0] > 0:
                row["balance"] = balance_value[0]

        return row

    sales_data = sales_data.apply(fill_current_month_data, axis=1)

    # Calculate relevant periods directly as Period objects
    twelve_months_ago = current_period - 12
    three_months_ago = current_period - 3

    # Periods for August, September, and October of previous years
    periods_last_year = [pd.Period(f"{reference_date.year - 1}-{month:02d}", freq='M') for month in range(8, 11)]
    periods_two_years_ago = [period - 12 for period in periods_last_year]
    periods_three_years_ago = [period - 24 for period in periods_last_year]

    # Initialize an empty DataFrame to store the results
    metrics = []
    print("Происходит расчет метрик средних продаж, отклонений, суммарных продаж в разные периоды, время с последней продажи и т.п...")

    # Group by 'kod' and calculate the required sums in a single pass
    for kod, group in sales_data.groupby('kod'):

        # Fill NaN values in 'total_sales' and 'balance' with 0 for sales calculations
        group['total_sales_filled'] = group['total_sales'].fillna(0)
        group['balance_filled'] = group['balance'].fillna(0)

        # Total sales calculations considering all months
        total_sales_last_12 = group[(group['year_month'] > twelve_months_ago) & (group['year_month'] <= current_period)]['total_sales_filled'].sum()
        total_sales_last_3 = group[(group['year_month'] > three_months_ago) & (group['year_month'] <= current_period)]['total_sales_filled'].sum()

        # Добавление максимального и минимального значений продаж за последние 12 месяцев
        max_sales_last_12 = group[
            (group["year_month"] > twelve_months_ago)
            & (group["year_month"] <= current_period)
        ]["total_sales_filled"].max()
        min_sales_last_12 = group[
            (group["year_month"] > twelve_months_ago)
            & (group["year_month"] <= current_period)
            & (group["total_sales_filled"] > 0)
        ]["total_sales_filled"].min()

        sum_sales_last_year = group[group['year_month'].isin(periods_last_year)]['total_sales_filled'].sum()
        sum_sales_two_years_ago = group[group['year_month'].isin(periods_two_years_ago)]['total_sales_filled'].sum()
        sum_sales_three_years_ago = group[group['year_month'].isin(periods_three_years_ago)]['total_sales_filled'].sum()

        # Filter for the last 12 months and include months where there were sales or balance was > 0
        filtered_last_12_months = group[(group['year_month'] > twelve_months_ago) & (group['year_month'] <= current_period) & 
                                        ((group['balance_filled'] > 0) | (group['total_sales_filled'] > 0))].copy()

        # Calculate mean and standard deviation of total_sales for the filtered months
        if not filtered_last_12_months.empty:
            mean_sales = filtered_last_12_months['total_sales_filled'].sum() / filtered_last_12_months.shape[0]
            std_sales = filtered_last_12_months['total_sales_filled'].std()
        else:
            mean_sales = std_sales = 0

        # Count months without sales over the entire period in sales_data
        if group['total_sales_filled'].sum() == 0:
            # If no sales at all, set months_without_sales to the number of unique months
            months_without_sales = group['year_month'].nunique()
        elif group[(group['balance_filled'] > 0) & ((group['total_sales'].isna()) | (group['total_sales_filled'] <= 0))].empty:
            # If there were sales in every month, set months_without_sales to 0
            months_without_sales = 0
        else:
            # Otherwise, count months without sales where balance > 0 and sales are <= 0 or NaN
            months_without_sales = group[
                (group['balance_filled'] > 0) & ((group['total_sales'].isna()) | (group['total_sales_filled'] <= 0))
            ].shape[0]

        # Calculate the number of months since the last sale
        last_sale_period = group[group['total_sales_filled'] > 0]['year_month'].max()
        if pd.notna(last_sale_period):
            months_since_last_sale = current_period - last_sale_period
            months_since_last_sale = months_since_last_sale.n
        else:
            # If no sales were found, set months_since_last_sale to the total number of months in the observation
            months_since_last_sale = group['year_month'].nunique()

        metrics.append(
            {
                "kod": kod,
                "total_sales_last_12_months": total_sales_last_12,
                "total_sales_last_3_months": total_sales_last_3,
                "sum_sales_last_year": sum_sales_last_year,
                "sum_sales_two_years_ago": sum_sales_two_years_ago,
                "sum_sales_three_years_ago": sum_sales_three_years_ago,
                "mean_sales_last_12_months": mean_sales,
                "std_sales_last_12_months": std_sales,
                "months_without_sales": months_without_sales,
                "months_since_last_sale": months_since_last_sale,
                "max_sales_last_12_months": max_sales_last_12,
                "min_sales_last_12_months": min_sales_last_12,
            }
        )

    # Convert the results into a DataFrame
    sales_metrics = pd.DataFrame(metrics)

    # Merge with union_data
    print("Объединение данных union_data с рассчитанными метриками...")
    union_data = union_data.merge(sales_metrics, on='kod', how='left')

    # Calculate 'min_stock' based on the given logic
    def calculate_min_stock(row):
        if pd.isna(row['mean_sales_last_12_months']) or pd.isna(row['std_sales_last_12_months']):
            return 0  # Return 0 if no data for calculations

        if row['abc'] in ['A', 'A1']:
            return round((row['mean_sales_last_12_months'] * 1 + row['std_sales_last_12_months']) + 0.49)
        elif row['abc'] == 'B':
            return round((row['mean_sales_last_12_months'] * 1 + row['std_sales_last_12_months']) + 0.49)
        elif row['mean_sales_last_12_months'] == 0:
            return 1
        else:
            return round((row['mean_sales_last_12_months'] + row['std_sales_last_12_months']) + 0.49)

    print("Вычисление начальных значений min_stock...")
    union_data['min_stock'] = union_data.apply(calculate_min_stock, axis=1)

    # Calculate 'min_stock' for each gruppa_analogov based on the same logic
    def calculate_min_stock_for_group(gruppa_analogov, union_data, sales_data):
        # Filter union_data for the given gruppa_analogov
        group_data = union_data[union_data["gruppa_analogov"] == gruppa_analogov]

        # Use sales_data to calculate mean and std only for this gruppa_analogov
        sales_group_data = sales_data[sales_data["kod"].isin(group_data["kod"])].copy()

        # Fill missing values in 'total_sales' and 'balance' with 0
        sales_group_data["total_sales_filled"] = sales_group_data["total_sales"].fillna(0)
        sales_group_data["balance_filled"] = sales_group_data["balance"].fillna(0)

        mean_sales_last_12_months = (
            sales_group_data["total_sales_filled"].sum() / sales_group_data.shape[0]
            if sales_group_data.shape[0] > 0
            else 0
        )
        std_sales_last_12_months = (
            sales_group_data["total_sales_filled"].std()
            if sales_group_data.shape[0] > 0
            else 0
        )

        if pd.isna(mean_sales_last_12_months) or pd.isna(std_sales_last_12_months):
            return 0

        if group_data["abc"].iloc[0] in ["A", "A1"]:
            return round((mean_sales_last_12_months * 1 + std_sales_last_12_months) + 0.49)
        elif group_data["abc"].iloc[0] == "B":
            return round((mean_sales_last_12_months * 1 + std_sales_last_12_months) + 0.49)
        elif mean_sales_last_12_months == 0:
            return 1
        else:
            return round((mean_sales_last_12_months + std_sales_last_12_months) + 0.49)

    print("Вычисление начальных значений min_stock для каждой gruppa_analogov...")
    # Get unique gruppa_analogov from union_data and calculate min_stock for each
    unique_groups = union_data['gruppa_analogov'].unique()
    min_stock_group_values = {
        group: calculate_min_stock_for_group(group, union_data, sales_data) for group in unique_groups
    }

    # Apply calculated min_stock_group values to the union_data
    union_data['min_stock_group'] = union_data['gruppa_analogov'].map(min_stock_group_values)

    def recalculate_for_group_analogues(union_data, sales_data):
        # Шаг 1: Выбираем группы аналогов с 2 и более позициями
        group_counts = union_data.groupby('gruppa_analogov')['kod'].count()
        valid_groups = group_counts[group_counts >= 2].index

        # Фильтруем данные по валидным группам
        union_data_filtered = union_data[union_data['gruppa_analogov'].isin(valid_groups)].copy()

        # Шаг 2: Сложить min_stock для всех позиций одной группы аналогов и сравнить с min_stock_group
        group_min_stock_sum = union_data_filtered.groupby('gruppa_analogov')['min_stock'].sum().reset_index()
        group_min_stock_sum = group_min_stock_sum[group_min_stock_sum['min_stock'] >= 5]

        union_data_filtered = pd.merge(union_data_filtered, group_min_stock_sum, on='gruppa_analogov', how='inner', suffixes=('', '_sum'))

        # Шаг 3: Фильтруем только те группы, у которых сумма min_stock больше на 30% чем min_stock_group
        union_data_filtered['min_stock_diff'] = (union_data_filtered['min_stock_sum'] - union_data_filtered['min_stock_group']) / union_data_filtered['min_stock_group'] * 100
        kods_for_recalculation = union_data_filtered[union_data_filtered['min_stock_diff'] > 30]['kod'].unique()

        print(f"Найдено {len(kods_for_recalculation)} kod для перерасчетов.")

        # Шаг 4: Пересчет mean_sales_last_12_months и std_sales_last_12_months для выбранных kod
        def recalculate_sales_metrics_for_kod(group, sales_data):
            # Получаем список kod из группы аналогов, которые в наличии
            kod_in_stock = group[group['osnsklad'] > 0]['kod'].unique()

            # Если нет позиций в наличии, пропускаем эту группу
            if len(kod_in_stock) == 0:
                return union_data

            # Ищем месяцы, где были продажи или баланс > 0 для каждого kod
            def find_valid_months(kod):
                valid_months = sales_data[(sales_data['kod'] == kod) &
                                      ((sales_data['balance'] > 0) | (sales_data['total_sales'] > 0))]['year_month'].unique()
                return valid_months

            # Находим месяцы, где позиции были в наличии
            kod_to_valid_months = {kod: find_valid_months(kod) for kod in kod_in_stock}

            # Находим общие месяцы, где несколько kod были в наличии
            common_months = set(kod_to_valid_months[kod_in_stock[0]])  # Начинаем с первого kod
            for kod in kod_in_stock[1:]:
                common_months.intersection_update(kod_to_valid_months[kod])

            # Если не найдено общих месяцев, используем текущий месяц
            if len(common_months) == 0:
                common_months = [current_period]

            # Фильтруем sales_data по общим месяцам
            sales_filtered = sales_data[(sales_data['kod'].isin(kod_in_stock)) &
                                        (sales_data['year_month'].isin(common_months))]

            # Пересчет метрик для каждого kod
            for kod in kod_in_stock:
                kod_sales_data = sales_filtered[sales_filtered['kod'] == kod]
                mean_sales = kod_sales_data['total_sales'].mean() if not kod_sales_data.empty else 0
                std_sales = kod_sales_data['total_sales'].std() if not kod_sales_data.empty else 0

                # Обновляем значения в union_data
                union_data.loc[union_data['kod'] == kod, 'mean_sales_last_12_months'] = mean_sales
                union_data.loc[union_data['kod'] == kod, 'std_sales_last_12_months'] = std_sales

            return union_data

        for gruppa_analogov in union_data_filtered['gruppa_analogov'].unique():
            group = union_data_filtered[union_data_filtered['gruppa_analogov'] == gruppa_analogov]
            union_data = recalculate_sales_metrics_for_kod(group, sales_data)

        # Теперь вместо recalculate_min_stock_after_metrics используем calculate_min_stock
        union_data.loc[union_data['kod'].isin(kods_for_recalculation), 'min_stock'] = \
            union_data.loc[union_data['kod'].isin(kods_for_recalculation)].apply(calculate_min_stock, axis=1)

        print("Перерасчет завершен.")

        return union_data

    print("Пересчитываем средние продажи и отклонения...")
    union_data = recalculate_for_group_analogues(union_data, sales_data)

    # Присоединение таблицы deliveryminprice по полю kod
    print("Присоединение таблицы deliveryminprice...")
    deliveryminprice = deliveryminprice.rename(columns={"price": "deliveryprice"})
    union_data = union_data.merge(
        deliveryminprice[["kod", "deliveryprice"]], on="kod", how="left"
    )

    # Adjust 'min_stock' to be even if certain phrases are in 'naimenovanie'
    def make_min_stock_even(row):
        phrases = [
            "пружина амортизатора",
            "пружина задней подвески",
            "rh/lh",
            "lh/rh",
            "fr/rr",
            "втулка поперечного стабилизатора",
            "рем. ком. суппорта",
            "подшипник опоры стойки",
            "опора задней стойки",
            "опора передней стойки",
        ]
        naimenovanie_lower = row["naimenovanie"].lower()  # Приводим naimenovanie к нижнему регистру

        # Проверка на определенные фразы и приведение к четному значению
        if any(phrase in naimenovanie_lower for phrase in phrases) and row["min_stock"] % 2 != 0:
            return row["min_stock"] + 1  # Сделать четным, добавив 1

        # Проверка для строк, начинающихся со "Свеча зажигания" или "Свеча накала"
        if naimenovanie_lower.startswith("свеча зажигания") or naimenovanie_lower.startswith("свеча накала"):
            if row["min_stock"] % 4 != 0:
                return (row["min_stock"] // 4 + 1) * 4  # Округление до кратного 4 в большую сторону

        return row["min_stock"]

    print("Корректировка min_stock на четное значение для определенных наименований...")
    union_data["min_stock"] = union_data.apply(make_min_stock_even, axis=1)

    # Adjust 'min_stock' based on sales data from the previous year
    def adjust_min_stock(row):
        current_month = current_period.month
        last_year_same_month = pd.Period(f"{current_period.year - 1}-{current_month:02d}", freq='M')
        last_year_month_minus_1 = last_year_same_month - 1
        last_year_month_plus_1 = last_year_same_month + 1

        # Find sales for the specified periods
        relevant_sales = sales_data[
            (sales_data['kod'] == row['kod']) &
            (sales_data['year_month'].isin([last_year_same_month, last_year_month_minus_1, last_year_month_plus_1]))
        ]['total_sales'].max()

        if pd.notna(relevant_sales) and relevant_sales > row['min_stock']:
            return relevant_sales
        return row['min_stock']

    print("Корректировка min_stock на основе данных о продажах за прошлый год...")
    union_data['min_stock'] = union_data.apply(adjust_min_stock, axis=1)

    def adjust_min_stock_based_on_margin(row):
        # Рассчитываем наценку, если цены присутствуют
        if pd.isna(row['deliveryprice']) or pd.isna(row['tsenarozn']):
            margin = None
        else:
            margin = (row['tsenarozn'] - row['deliveryprice']) / row['deliveryprice'] * 100

        # Проверка условия для корректировки на минимальное значение
        if (pd.isna(margin) or (margin < 36 and row['tsenarozn'] <= 10000)) and pd.notna(row['min_sales_last_12_months']) and row['min_sales_last_12_months'] > 0:
            return row['min_sales_last_12_months']

        # Проверка условия для корректировки на максимальное значение, только если margin не NaN
        elif pd.notna(margin) and margin > 70 and pd.notna(row['max_sales_last_12_months']) and row['max_sales_last_12_months'] > 0:
            return row['max_sales_last_12_months']

        # Если условия не выполнены, возвращаем текущее значение min_stock
        else:
            return row['min_stock']

    print("Корректировка min_stock на основе наценки и наличия цены поставщика...")
    union_data['min_stock'] = union_data.apply(adjust_min_stock_based_on_margin, axis=1)

    def correct_min_stock_based_on_balance_and_sales(union_data, sales_data):
        # Текущий период
        current_period = pd.Period(datetime.now().strftime("%Y-%m"), freq="M")
        twelve_months_ago = current_period - 12

        # Проходим по каждой позиции для проверки условий
        def check_and_correct(row):
            # Получаем данные за последний год для конкретного kod
            kod_sales_data = sales_data[(sales_data["kod"] == row["kod"]) & 
                                    (sales_data["year_month"] > twelve_months_ago) & 
                                    (sales_data["year_month"] <= current_period)]

            # Условие: balance всегда > 0 и продажи были в одном месяце (неважно, сколько продали)
            if (kod_sales_data["balance"] > 0).all() and kod_sales_data["total_sales"].gt(0).sum() == 1:
                row["min_stock"] = 1  # Устанавливаем min_stock в 1

            return row

        # Применяем корректировку по каждому ряду
        union_data = union_data.apply(check_and_correct, axis=1)

        # Применяем корректировку make_min_stock_even для этих позиций
        union_data["min_stock"] = union_data.apply(make_min_stock_even, axis=1)

        return union_data

    union_data = correct_min_stock_based_on_balance_and_sales(union_data, sales_data)

    print("Завершено!")
    return union_data


if __name__ == "__main__":
    engine = get_db_engine()

    _, _, _, deliveryminprice = load_additional_data(engine)

    # Загрузка данных
    sales_data, nomenklatura_ml = get_final_data(engine)
    nomenklatura_merged, stockendmonth, priceendmonth, postuplenija, prodazhi = load_and_process_data(engine)
    abc_xyz_analysis = perform_abc_xyz_analysis(engine)

    # Объединение данных
    merged_data = pd.merge(nomenklatura_ml, nomenklatura_merged[['kod', 'naimenovanie', 'artikul', 'edizm', 'datasozdanija', 'osnsklad', 'tsenarozn']], on='kod', how='left')
    union_data = pd.merge(merged_data, abc_xyz_analysis, on='kod', how='left')

    # Вычисление метрик продаж
    union_data = calculate_sales_metrics(sales_data, union_data, deliveryminprice)

    # Сохранение данных в базу данных,
    
    print("Сохранение результатов в базу данных...")
    union_data.to_sql('full_statistic', con=engine, if_exists='replace', index=False)
    print("Данные успешно сохранены в таблицу 'full_statistic'.")
