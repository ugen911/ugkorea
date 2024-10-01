from flask import Blueprint, render_template, request, redirect, url_for
from ugkorea.db.database import get_db_engine
from sqlalchemy import text
from datetime import datetime
import plotly.graph_objs as go
import plotly.io as pio
import base64
import io

# Создаем Blueprint для маршрутов, связанных с номенклатурой
bp = Blueprint("nomenklatura", __name__, url_prefix="/nomenklatura")

# Подключение к базе данных
engine = get_db_engine()


@bp.route("/<string:kod>")
def detail_nomenclature(kod):
    """
    Отображает подробную информацию об элементе номенклатуры и статистику продаж.
    """
    with engine.connect() as connection:
        # Выполняем объединенный запрос к базе данных для текущего элемента
        item_query = text(
            """
            SELECT 
                TRIM(n.kod) AS kod,
                n.artikul,
                n.naimenovaniepolnoe,
                n.proizvoditel,
                n.vidnomenklatury,
                n.bazovajaedinitsa,
                n.datasozdanija,
                td.type_detail,
                no.stellazh,
                no.pometkaudalenija,
                po.tsenazakup,
                po.tsenarozn,
                mp.middleprice,
                mp.maxprice,
                so.osnsklad,
                so.zakazy_sklad
            FROM nomenklatura n
            LEFT JOIN typedetailgen td ON TRIM(n.kod) = TRIM(td.kod)
            LEFT JOIN nomenklaturaold no ON TRIM(n.kod) = TRIM(no.kod)
            LEFT JOIN priceold po ON TRIM(n.kod) = TRIM(po.kod)
            LEFT JOIN middlemaxprice mp ON TRIM(n.kod) = TRIM(mp.kod)
            LEFT JOIN stockold so ON TRIM(n.kod) = TRIM(so.kod)
            WHERE n.vidnomenklatury = 'Товар' AND TRIM(n.kod) = :kod
        """
        )
        item = connection.execute(item_query, {"kod": kod.strip()}).fetchone()

        # Проверяем, нашли ли элемент
        if item is None:
            return "Элемент номенклатуры не найден."

        # Преобразуем дату создания в удобный формат
        datasozdanija_formatted = (
            item.datasozdanija.strftime("%d.%m.%Y") if item.datasozdanija else "N/A"
        )

        # Выполняем запрос для получения статистики из full_statistic
        stats_query = text(
            """
            SELECT "ABC", "XYZ", "total_sales_last_12_months", "total_sales_last_3_months", 
                   "sum_sales_two_years_ago", "sum_sales_three_years_ago", 
                   "mean_sales_last_12_months", "months_without_sales", 
                   "months_since_last_sale", "min_stock"
            FROM full_statistic
            WHERE kod = :kod
        """
        )
        stats = connection.execute(stats_query, {"kod": kod.strip()}).fetchone()

        # Выполняем запрос для получения данных за последние 24 месяца из таблиц stockendmonth, priceendmonth, salespivot, suppliespivot
        monthly_data_query = text(
            """
            SELECT 
                sm.month AS month, COALESCE(sm.balance, 0) AS balance, 
                COALESCE(pm.tsena, 0) AS price, 
                COALESCE(sp.kolichestvo, 0) AS sales_quantity, COALESCE(sp.summa, 0) AS sales_sum,
                COALESCE(spp.kolichestvo, 0) AS supply_quantity, COALESCE(spp.summa, 0) AS supply_sum
            FROM 
                (SELECT DISTINCT month FROM stockendmonth WHERE nomenklaturakod = :kod
                 UNION 
                 SELECT DISTINCT data AS month FROM priceendmonth WHERE kod = :kod
                 UNION 
                 SELECT DISTINCT year_month AS month FROM salespivot WHERE kod = :kod
                 UNION 
                 SELECT DISTINCT year_month AS month FROM suppliespivot WHERE kod = :kod
                 ORDER BY month DESC LIMIT 24) AS m
            LEFT JOIN stockendmonth sm ON m.month = sm.month AND TRIM(sm.nomenklaturakod) = :kod
            LEFT JOIN priceendmonth pm ON m.month = pm.data AND TRIM(pm.kod) = :kod
            LEFT JOIN salespivot sp ON m.month = sp.year_month AND TRIM(sp.kod) = :kod
            LEFT JOIN suppliespivot spp ON m.month = spp.year_month AND TRIM(spp.kod) = :kod
            ORDER BY m.month ASC
        """
        )
        monthly_data = connection.execute(
            monthly_data_query, {"kod": kod.strip()}
        ).fetchall()

        # Логика для поиска следующего и предыдущего элемента
        next_item_query = text(
            """
            SELECT kod FROM nomenklatura 
            WHERE vidnomenklatury = 'Товар' AND naimenovaniepolnoe > :name 
            ORDER BY naimenovaniepolnoe ASC LIMIT 1
        """
        )
        next_item = connection.execute(
            next_item_query, {"name": item.naimenovaniepolnoe}
        ).fetchone()

        prev_item_query = text(
            """
            SELECT kod FROM nomenklatura 
            WHERE vidnomenklatury = 'Товар' AND naimenovaniepolnoe < :name 
            ORDER BY naimenovaniepolnoe DESC LIMIT 1
        """
        )
        prev_item = connection.execute(
            prev_item_query, {"name": item.naimenovaniepolnoe}
        ).fetchone()

    # Формируем данные для графика
    months = [row[0] for row in monthly_data]
    sales_quantity = [row[3] for row in monthly_data]
    supply_quantity = [row[5] for row in monthly_data]
    prices = [row[2] for row in monthly_data]
    balances = [row[1] for row in monthly_data]

    # Создаем интерактивный график с помощью plotly
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=months,
            y=sales_quantity,
            mode="lines+markers",
            name="Количество продаж",
            line=dict(color="blue"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=months,
            y=supply_quantity,
            mode="lines+markers",
            name="Количество закупок",
            line=dict(color="orange"),
        )
    )

    # Добавляем данные об остатках и ценах как аннотации
    for i, month in enumerate(months):
        fig.add_annotation(
            x=month,
            y=sales_quantity[i],
            text=f"Остаток: {balances[i]}<br>Цена: {prices[i]}",
            showarrow=True,
            arrowhead=2,
        )

    fig.update_layout(
        title="Статистика продаж и закупок за последние 24 месяца",
        xaxis_title="Месяц",
        yaxis_title="Количество",
        legend=dict(x=0, y=1.1, orientation="h"),
        margin=dict(l=20, r=20, t=50, b=50),
    )

    # Сохранение графика в буфер
    graph_html = pio.to_html(fig, full_html=False)

    # Используем шаблон 'nomenclature/detail.html' для отображения информации и статистики
    return render_template(
        "nomenclature/detail.html",
        item=item,
        stats=stats,
        datasozdanija_formatted=datasozdanija_formatted,
        graph_html=graph_html,
        next_item=next_item,
        prev_item=prev_item,
    )


@bp.route("/")
def list_nomenclature():
    """
    Перенаправляет на первый элемент номенклатуры или отображает сообщение, если элементов нет.
    """
    with engine.connect() as connection:
        # Получаем первый элемент из таблицы nomenklatura
        result = connection.execute(
            text("SELECT kod FROM nomenklatura ORDER BY kod ASC LIMIT 1")
        )
        first_item = result.fetchone()

    if first_item:
        # Перенаправляем на страницу деталей первого элемента
        return redirect(url_for("nomenklatura.detail_nomenclature", kod=first_item.kod))
    else:
        # Если в базе данных нет элементов, отображаем сообщение
        return "В базе данных нет элементов номенклатуры."
