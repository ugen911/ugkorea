from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from ugkorea.db.database import get_db_engine  # Импорт функции для подключения к базе данных
from sqlalchemy.exc import SQLAlchemyError

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)
    
    # Получаем SQLAlchemy engine через вашу функцию
    engine = get_db_engine()
    
    if engine is None:
        # Если подключение к базе данных не удалось
        print("Не удалось подключиться к базе данных. Приложение завершит работу.")
        return None

    # Устанавливаем URI подключения для SQLAlchemy
    app.config['SQLALCHEMY_DATABASE_URI'] = str(engine.url)
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    # Инициализируем SQLAlchemy с использованием настроек приложения
    db.init_app(app)

    # Импортируем и регистрируем маршруты
    from . import routes
    app.register_blueprint(routes.bp)

    return app

