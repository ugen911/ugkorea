import pyodbc

server = '85.92.118.21,45500'
database = 'master'  # Используйте 'master' для проверки подключения
username = 'sa'
password = 'tkbfUTbT73F44hn'  # или 'qwe1234rt341!' для SQL Server 2012

try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    )
    print("Connection successful!")
    conn.close()
except Exception as e:
    print(f"Error connecting to SQL Server: {e}")
