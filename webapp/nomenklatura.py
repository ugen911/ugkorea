from flask import Blueprint, render_template, request, redirect, url_for
from ugkorea.db.database import get_db_engine
from sqlalchemy import text
from datetime import datetime
import matplotlib.pyplot as plt
import io
import base64


# Создаем Blueprint для маршрутов, связанных с номенклатурой
bp = Blueprint('nomenklatura', __name__, url_prefix='/nomenklatura')

# Подключение к базе данных
engine = get_db_engine()

@bp.route('/<string:kod>')
def detail_nomenclature(kod):
    """
    Отображает подробную информацию об элементе номенклатуры и статистику продаж.
    """
    with engine.connect() as connection:
        # Выполняем объединенный запрос к базе данных для текущего элемента
        item_query = text("""
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
        """)
        item = connection.execute(item_query, {'kod': kod.strip()}).fetchone()
        
        # Проверяем, нашли ли элемент
        if item is None:
            return "Элемент номенклатуры не найден."
        
        # Преобразуем дату создания в удобный формат
        datasozdanija_formatted = item.datasozdanija.strftime("%d.%m.%Y") if item.datasozdanija else "N/A"

        # Выполняем запрос для получения статистики из full_statistic
        stats_query = text("""
            SELECT "ABC", "XYZ", "total_sales_last_12_months", "total_sales_last_3_months", 
                   "sum_sales_two_years_ago", "sum_sales_three_years_ago", 
                   "mean_sales_last_12_months", "months_without_sales", 
                   "months_since_last_sale", "min_stock"
            FROM full_statistic
            WHERE kod = :kod
        """)
        stats = connection.execute(stats_query, {'kod': kod.strip()}).fetchone()

        # Выполняем запрос для получения данных за последние 24 месяца из таблиц stockendmonth, priceendmonth, salespivot, suppliespivot
        monthly_data_query = text("""
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
        """)
        monthly_data = connection.execute(monthly_data_query, {'kod': kod.strip()}).fetchall()

        # Логика для поиска следующего и предыдущего элемента
        next_item_query = text("""
            SELECT kod FROM nomenklatura 
            WHERE vidnomenklatury = 'Товар' AND naimenovaniepolnoe > :name 
            ORDER BY naimenovaniepolnoe ASC LIMIT 1
        """)
        next_item = connection.execute(next_item_query, {'name': item.naimenovaniepolnoe}).fetchone()
        
        prev_item_query = text("""
            SELECT kod FROM nomenklatura 
            WHERE vidnomenklatury = 'Товар' AND naimenovaniepolnoe < :name 
            ORDER BY naimenovaniepolnoe DESC LIMIT 1
        """)
        prev_item = connection.execute(prev_item_query, {'name': item.naimenovaniepolnoe}).fetchone()
    # Формируем данные для графика
    months = [row[0] for row in monthly_data]  # Индекс 0 соответствует "month"
    sales_sum = [row[4] for row in monthly_data]  # Индекс 4 соответствует "sales_sum"
    supply_sum = [row[6] for row in monthly_data]  # Индекс 6 соответствует "supply_sum"
    prices = [row[2] for row in monthly_data]  # Индекс 2 соответствует "price"
    balances = [row[1] for row in monthly_data]  # Индекс 1 соответствует "balance"

    # Создаем график
    plt.figure(figsize=(10, 5))
    plt.plot(months, sales_sum, label='Сумма продаж', marker='o')
    plt.plot(months, supply_sum, label='Сумма закупок', marker='o')
    plt.xticks(rotation=45)
    plt.legend()
    plt.title('Статистика продаж и закупок за последние 24 месяца')
    plt.xlabel('Месяц')
    plt.ylabel('Сумма (руб)')
    plt.tight_layout()

    # Сохранение графика в буфер
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    graph_url = base64.b64encode(img.getvalue()).decode('utf8')
    plt.close()

    # Используем шаблон 'nomenclature/detail.html' для отображения информации и статистики
    return render_template(
        'nomenclature/detail.html', 
        item=item, 
        stats=stats, 
        datasozdanija_formatted=datasozdanija_formatted,
        graph_url=graph_url,
        prices=prices,
        balances=balances,
        months=months,
        next_item=next_item,
        prev_item=prev_item,
        zip=zip  # Добавляем zip в контекст
)



@bp.route('/')
def list_nomenclature():
    """
    Перенаправляет на первый элемент номенклатуры или отображает сообщение, если элементов нет.
    """
    with engine.connect() as connection:
        # Получаем первый элемент из таблицы nomenklatura
        result = connection.execute(text("SELECT kod FROM nomenklatura ORDER BY kod ASC LIMIT 1"))
        first_item = result.fetchone()
    
    if first_item:
        # Перенаправляем на страницу деталей первого элемента
        return redirect(url_for('nomenklatura.detail_nomenclature', kod=first_item.kod))
    else:
        # Если в базе данных нет элементов, отображаем сообщение
        return "В базе данных нет элементов номенклатуры."


@bp.route('/add', methods=['GET', 'POST'])
def add_nomenclature():
    """
    Добавляет новый элемент номенклатуры.
    """
    if request.method == 'POST':
        # Получаем данные из формы
        kod = request.form['kod']
        name = request.form['naimenovaniepolnoe']
        proizvoditel = request.form.get('proizvoditel', '')
        
        # Добавляем новый элемент в базу данных
        with engine.connect() as connection:
            try:
                connection.execute(
                    text("INSERT INTO nomenklatura (kod, naimenovaniepolnoe, proizvoditel) VALUES (:kod, :name, :proizvoditel)"),
                    {'kod': kod, 'name': name, 'proizvoditel': proizvoditel}
                )
                return redirect(url_for('nomenklatura.list_nomenclature'))
            except Exception as e:
                print(f"Ошибка при добавлении номенклатуры: {e}")
                return render_template('nomenclature/add.html', error=str(e))
    
    # Показ формы для добавления элемента
    return render_template('nomenclature/add.html')


