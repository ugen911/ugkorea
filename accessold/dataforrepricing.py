import os
import pandas as pd
from ugkorea.db.database import get_db_engine

# Получаем движок базы данных
engine = get_db_engine()

# SQL запрос для выборки всех строк из таблицы bergapi в схеме prices
query = 'SELECT * FROM api.bergapi;'

# Выполнение запроса и загрузка данных в DataFrame
df = pd.read_sql(query, engine)

# Создаем новую таблицу в требуемом формате
df_final = pd.DataFrame({
    'НомерСтрокиПоиска': range(1, len(df) + 1),  # Нумерация строк
    'БрендПоиска': df['proizvoditel'],  # Данные из kolumny 'proizvoditel'
    'АртикулПоиска': df['artikul'],  # Данные из kolumny 'artikul'
    'НаименованиеПоиска': df['kod'],  # Данные из kolumny 'kod'
    'Потребность': 1,  # Везде 1
    'Поставщик': 'BERG',  # Везде BERG
    'Бренд': df['proizvoditel'],  # Данные из kolumny 'proizvoditel'
    'Артикул': df['artikul'],  # Данные из kolumny 'artikul'
    'АртикулПоставщика': df['artikul'],  # Данные из kolumny 'artikul'
    'Наименование': 'Деталь',  # Везде указать "Деталь"
    'Состояние': 'Заказ',  # Везде указать "Заказ"
    'Срок': df['average_period'].astype(str) + ' дн.',  # К каждому числу добавить " дн."
    'Склад': 'Партнерский склад',  # Везде указать "Партнерский склад"
    'СкладПоставщика': df['warehouse_name'],  # Данные из kolumny 'warehouse_name'
    'Наличие': df['quantity'].astype(str) + ' шт.',  # Данные из 'quantity' + " шт."
    'КОформлению': 0,  # Везде указать 0
    'ЦенаЗначение': df['price'],  # Данные из kolumny 'price'
    'ЦенаПродажи': 0,  # Везде указать 0
    'ЦенаВалюта': 'р.',  # Везде указать "р."
    'ДопИнфо': df['reliability'].astype(str) + '% - вероятность поставки'  # Данные из 'reliability' + " % - вероятность поставки"
})

# Попытка сохранить в первую директорию
first_path = r'D:\NAS\заказы\Евгений\New\Файлы для работы Access\Прайсы для переоценки\ПроценкаОбщая.xlsx'
second_path = r'\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\Прайсы для переоценки\ПроценкаОбщая.xlsx'

# Определяем путь для сохранения
save_path = first_path if os.path.exists(os.path.dirname(first_path)) else second_path

# Функция для сохранения файла
def save_excel(path):
    with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
        df_final.to_excel(writer, sheet_name='Проценка_230213_155510', index=False)
        # Удаляем все остальные листы кроме указанного
        if 'Sheet1' in writer.book.sheetnames:
            writer.book.remove(writer.book['Sheet1'])

# Попытка сохранения файла
try:
    save_excel(save_path)
    print(f"Файл успешно сохранен по пути: {save_path}")
except PermissionError:
    # При ошибке пробуем сохранить файл с другим именем
    alternative_path = os.path.join(os.path.dirname(save_path), 'ПроценкаОбщая1.xlsx')
    try:
        save_excel(alternative_path)
        print(f"Файл был сохранен с другим именем по пути: {alternative_path}")
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")
