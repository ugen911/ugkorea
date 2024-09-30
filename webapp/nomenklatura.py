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
    Отображает подробную информацию об элементе номенклатуры.
    """
    with engine.connect() as connection:
        # Получаем детали элемента номенклатуры по kod
        result = connection.execute(text("SELECT * FROM nomenklatura WHERE kod = :kod"), {'kod': kod})
        item = result.fetchone()
        
        # Находим следующий и предыдущий элемент для навигации
        next_item = connection.execute(
            text("SELECT kod FROM nomenklatura WHERE kod > :kod ORDER BY kod ASC LIMIT 1"), {'kod': kod}
        ).fetchone()
        prev_item = connection.execute(
            text("SELECT kod FROM nomenklatura WHERE kod < :kod ORDER BY kod DESC LIMIT 1"), {'kod': kod}
        ).fetchone()
    
    # Используем шаблон 'nomenclature/detail.html' для отображения деталей
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

@bp.route('/<string:kod>/edit', methods=['GET', 'POST'])
def edit_nomenclature(kod):
    """
    Редактирует элемент номенклатуры.
    """
    with engine.connect() as connection:
        # Получаем текущие данные элемента
        item = connection.execute(text("SELECT * FROM nomenklatura WHERE kod = :kod"), {'kod': kod}).fetchone()
        
        if request.method == 'POST':
            # Обновляем данные элемента
            new_name = request.form['naimenovaniepolnoe']
            new_artikul = request.form.get('artikul', None)
            new_proizvoditel = request.form.get('proizvoditel', None)

            try:
                connection.execute(
                    text("UPDATE nomenklatura SET naimenovaniepolnoe = :name, artikul = :artikul, proizvoditel = :proizvoditel WHERE kod = :kod"),
                    {'name': new_name, 'artikul': new_artikul, 'proizvoditel': new_proizvoditel, 'kod': kod}
                )
                return redirect(url_for('nomenklatura.detail_nomenclature', kod=kod))
            except Exception as e:
                print(f"Ошибка при редактировании номенклатуры: {e}")
                return render_template('nomenclature/edit.html', item=item, error=str(e))

    # Показ формы для редактирования элемента
    return render_template('nomenclature/edit.html', item=item)

@bp.route('/<string:kod>/delete', methods=['POST'])
def delete_nomenclature(kod):
    """
    Удаляет элемент номенклатуры.
    """
    with engine.connect() as connection:
        try:
            connection.execute(text("DELETE FROM nomenklatura WHERE kod = :kod"), {'kod': kod})
            return redirect(url_for('nomenklatura.list_nomenclature'))
        except Exception as e:
            print(f"Ошибка при удалении номенклатуры: {e}")
            return redirect(url_for('nomenklatura.detail_nomenclature', kod=kod, error=str(e)))
