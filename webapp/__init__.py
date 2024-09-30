from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from ugkorea.db.database import get_db_engine

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)
    
    # Получаем SQLAlchemy engine через вашу функцию
    engine = get_db_engine()
    
    if engine is None:
        print("Не удалось подключиться к базе данных. Приложение завершит работу.")
        return None

    # Настройка подключения SQLAlchemy к базе данных
    app.config['SQLALCHEMY_DATABASE_URI'] = str(engine.url)
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)

    # Импортируем и регистрируем Blueprint для номенклатуры
    from .nomenklatura import bp as nomenklatura_bp
    app.register_blueprint(nomenklatura_bp)

    return app
