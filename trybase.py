def get_db_engine_avto6():
    db_user = os.getenv('DB_USER2', 'postgres')
    db_password = os.getenv('DB_PASSWORD2', '785612')
    db_host = os.getenv('DB_HOST2', '26.218.196.12')
    db_port = os.getenv('DB_PORT2', '5432')
    db_name = os.getenv('DB_NAME2', 'avto6')
    db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    
    try:
        engine = create_engine(db_url)
        # Если подключение успешно, выведите сообщение об успешном подключении
        print("Успешное подключение к базе данных.")
        return engine
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        return None
    


def test_db_connection():
    engine = get_db_engine_avto6()
    if engine is not None:
        try:
            # Использование Connection для выполнения запроса
            with engine.connect() as connection:
                result = connection.execute(text("SELECT tablename FROM pg_tables WHERE schemaname='public';"))
                tables = [row[0] for row in result]
                print("Таблицы в базе данных:", tables)
        except SQLAlchemyError as e:
            print("Ошибка при выполнении запроса:", e)

test_db_connection()