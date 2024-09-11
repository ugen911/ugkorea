import os
import pandas as pd
from sqlalchemy import MetaData, Table, Column, String, Boolean, Integer, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
from ugkorea.db.database import get_db_engine

# Подключаем движок базы данных
engine = get_db_engine()

# Устанавливаем сессию для работы с базой данных
Session = sessionmaker(bind=engine)
session = Session()

# Создаем объект MetaData для схемы access
metadata = MetaData(schema='access')

# Инициализируем инспектор для проверки наличия таблиц
inspector = inspect(engine)

# Указываем локальный путь к файлу Excel
file_path = r'C:\Users\evgen\OneDrive\Documents\Access\Номенклатура 1С.xlsx'

# Проверяем, существует ли путь
if not os.path.exists(file_path):
    print(f"Файл {file_path} не существует.")
else:
    try:
        # Читаем файл Excel
        print(f"Чтение файла: {file_path}")
        df = pd.read_excel(file_path, sheet_name=0)  # sheet_name=0 для первого листа

        if df.empty:
            print(f"Файл {file_path} пустой, завершаем обработку.")
        else:
            # Преобразование строк "ИСТИНА" и "ЛОЖЬ" в булевы значения True и False
            df = df.replace({'ИСТИНА': True, 'ЛОЖЬ': False})

            # Имя таблицы
            table_name = 'Номенклатура_1С'

            # Проверяем, существует ли таблица
            if inspector.has_table(table_name, schema='access'):
                print(f"Таблица {table_name} существует, удаляем её...")
                table = Table(table_name, metadata, autoload_with=engine)
                table.drop(engine)

            # Определение таблицы с правильными типами данных на основе скриншота
            table = Table(
                table_name, metadata,
                Column('ЦБ', String(255)),
                Column('Оригинальный артикул', String(255)),
                Column('Артикул', String(255)),
                Column('Стеллаж', String(255)),
                Column('Наименование', String(255)),
                Column('Производитель', String(255)),
                Column('"Еденица измерения"', String(255)),  # Оставляем как "Еденица измерения" в кавычках
                Column('Розничная цена', Numeric),
                Column('Закуп 1С', Numeric),
                Column('Не используется в заказе', Boolean),
                Column('Неликвид', Boolean),
                Column('Помечено на удаление', Boolean),
                Column('Новая позиция (менее 6мес)', Boolean),
                Column('Только в Корее', Boolean),
                Column('Можно в РФ', Boolean),
                Column('Зафиксировать минималки', Boolean),
                Column('Категория ABC', String(255)),
                Column('Категория XYZ', String(255)),
                Column('Остаток', Integer),
                Column('Минималка местная', Integer),
                Column('Минималка Корея', Integer),
                Column('Корейский закуп последний', Numeric),
                Column('Сделаны фото', Boolean),
                Column('Счетчик', Integer),
                Column('Б/У', String(255)),
                Column('ОстатокЗаказы', Integer),
                Column('Лист Ожидания', Boolean),
                extend_existing=True,  # Применение extend_existing на уровне таблицы
                schema='access'
            )

            # Создаем таблицу
            metadata.create_all(engine)
            print(f"Таблица {table_name} создана.")

            # Проверьте, что название колонки "Еденица измерения" соответствует в DataFrame
            df.columns = [col if col != 'Еденица измерения' else '"Еденица измерения"' for col in df.columns]

            # Загрузка данных порциями
            chunk_size = 500  # Размер порции данных
            total_rows = len(df)
            print(f"Загрузка данных порциями, всего строк: {total_rows}, размер порции: {chunk_size}")

            for i, chunk in enumerate(range(0, total_rows, chunk_size)):
                df_chunk = df.iloc[chunk:chunk + chunk_size]
                print(f"Загружаем порцию {i + 1}, строки с {chunk} по {chunk + len(df_chunk)}...")
                df_chunk.to_sql(table_name, con=engine, schema='access', if_exists='append', index=False)
                print(f"Порция {i + 1} загружена успешно.")

            print(f"Данные успешно загружены в таблицу {table_name}.")

    except SQLAlchemyError as e:
        print(f"Ошибка при работе с таблицей {table_name}: {str(e)}")
    except Exception as e:
        print(f"Общая ошибка при обработке файла {file_path}: {str(e)}")

# Закрываем сессию
session.close()
print("Обработка завершена.")
