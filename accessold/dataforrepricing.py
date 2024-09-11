import os
import pandas as pd
from sqlalchemy import MetaData, Table
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

# Указываем локальный путь к папке, где находятся файлы Excel
folder_path = r'C:\Users\evgen\OneDrive\Documents\Access'

# Проверяем, существует ли путь
if not os.path.exists(folder_path):
    print(f"Указанный путь {folder_path} не существует.")
else:
    # Получаем список всех файлов Excel в папке
    excel_files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
    
    if len(excel_files) == 0:
        print(f"В директории {folder_path} не найдено файлов .xlsx.")
    else:
        print(f"Найдено файлов: {len(excel_files)}")

        # Проходим по каждому файлу и загружаем данные в базу
        for file in excel_files:
            file_path = os.path.join(folder_path, file)
            
            print(f"Обработка файла: {file_path}")
            
            try:
                # Читаем первый лист файла Excel
                print(f"Чтение файла: {file_path}")
                df = pd.read_excel(file_path, sheet_name=0)  # sheet_name=0 для первого листа
                
                if df.empty:
                    print(f"Файл {file_path} пустой, пропускаем.")
                    continue

                # Убираем расширение из имени файла, чтобы использовать его как имя таблицы
                table_name = os.path.splitext(file)[0]

                # Преобразование строк "ИСТИНА" и "ЛОЖЬ" в булевы значения True и False
                df = df.replace({'ИСТИНА': True, 'ЛОЖЬ': False})

                # Проверяем, существует ли таблица
                if inspector.has_table(table_name, schema='access'):
                    print(f"Таблица {table_name} существует, удаляем её...")
                    table = Table(table_name, metadata, autoload_with=engine)
                    table.drop(engine)

                # Создаем таблицу автоматически с распознаванием типов данных
                print(f"Загружаем данные в таблицу {table_name}...")
                
                # Попробуем загрузить данные порциями, если DataFrame большой
                chunk_size = 500  # Размер порции данных
                total_rows = len(df)
                print(f"Загрузка данных порциями, всего строк: {total_rows}, размер порции: {chunk_size}")

                # Разделение загрузки на порции
                for i, chunk in enumerate(range(0, total_rows, chunk_size)):
                    df_chunk = df.iloc[chunk:chunk + chunk_size]
                    print(f"Загружаем порцию {i + 1}, строки с {chunk} по {chunk + len(df_chunk)}...")
                    df_chunk.to_sql(table_name, con=engine, schema='access', if_exists='append', index=False)
                    print(f"Порция {i + 1} загружена успешно.")

                print(f"Данные успешно загружены в таблицу {table_name}.")

            except SQLAlchemyError as e:
                print(f"Ошибка при работе с таблицей {table_name}: {str(e)}")
            except Exception as e:
                print(f"Общая ошибка при обработке файла {file}: {str(e)}")

# Закрываем сессию
session.close()
print("Обработка завершена.")
