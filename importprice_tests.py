#!/c/Users/evgen/repo/ugkorea/venv/Scripts/python.exe


from sqlalchemy import create_engine
import sys
import subprocess


# Путь к интерпретатору Python из текущего виртуального окружения
python_executable = sys.executable

# Параметры подключения к PostgreSQL
db_user = 'postgres'
db_password = '89232808797'
db_host = 'localhost'
db_port = '5432'
db_name = 'ugkorea'
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

# Подключение к PostgreSQL
engine = create_engine(db_url)

# Проверка подключения
if engine:
    print("Подключение к PostgreSQL успешно установлено.")
else:
    print("Не удалось подключиться к PostgreSQL.")

# Выполнить файл Mimportprice.py
try:
    # Замените путь на ваш путь к файлу Mimportprice.py
    file_path = r"C:\Users\evgen\repo\ugkorea\Mimportprice.py"
    subprocess.run([python_executable, file_path])
except FileNotFoundError:
    print("Файл Mimportprice.py не найден.")