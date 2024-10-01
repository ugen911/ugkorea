from flask import Blueprint, render_template, request, redirect, url_for
from ugkorea.db.database import get_db_engine
from sqlalchemy import text

# Создаем Blueprint для маршрутов, связанных с номенклатурой
bp = Blueprint('nomenklatura', __name__, url_prefix='/nomenklatura')

# Подключение к базе данных
engine = get_db_engine()

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

@bp.route('/<string:kod>')
def detail_nomenclature(kod):
    """
    Отображает подробную информацию об элементе номенклатуры, объединяя данные из нескольких таблиц.
    """
    with engine.connect() as connection:
        # Выполняем объединенный запрос к базе данных для текущего элемента
        query = text("""
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
        
        item = connection.execute(query, {'kod': kod.strip()}).fetchone()
        
        # Проверяем, нашли ли элемент
        if item is None:
            return "Элемент номенклатуры не найден."
        
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

    # Используем шаблон 'nomenclature/detail.html' для отображения подробной информации
    return render_template('nomenclature/detail.html', item=item, next_item=next_item, prev_item=prev_item)



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


