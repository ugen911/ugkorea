import os
from ugkorea.db.database import get_db_engine
from ugkorea.mail.accesstomail import send_email_via_mailru
import pandas as pd

# Получаем подключение к базе данных
engine = get_db_engine()

# Проверка наличия папки output и создание ее, если не существует
output_dir = 'output'  # <-- Добавлено: название папки для сохранения файла
os.makedirs(output_dir, exist_ok=True)

# Загружаем данные в DataFrame
df_nomenklaturaold = pd.read_sql("SELECT kod, artikul, proizvoditel, naimenovanie, edizm FROM nomenklaturaold", engine)
df_priceold = pd.read_sql("SELECT kod, tsenarozn FROM priceold", engine)
df_stockold = pd.read_sql("SELECT kod, osnsklad FROM stockold", engine)
df_photoadress = pd.read_sql("SELECT kod, adress FROM photoadress", engine)
df_nomenklaturaprimenjaemost = pd.read_sql("SELECT kod, model FROM nomenklaturaprimenjaemost", engine)

# Обработка df_nomenklaturaprimenjaemost: группировка по kod и объединение model в строку через запятую
df_nomenklaturaprimenjaemost_grouped = df_nomenklaturaprimenjaemost.groupby('kod')['model'].apply(lambda x: ', '.join(filter(None, x))).reset_index()

# Очистка данных от пробелов и других возможных скрытых символов
df_nomenklaturaold['kod'] = df_nomenklaturaold['kod'].str.strip()
df_priceold['kod'] = df_priceold['kod'].str.strip()
df_stockold['kod'] = df_stockold['kod'].str.strip()
df_photoadress['kod'] = df_photoadress['kod'].str.strip()
df_nomenklaturaprimenjaemost_grouped['kod'] = df_nomenklaturaprimenjaemost_grouped['kod'].str.strip()

# Преобразование tsenarozn во float в df_priceold
df_priceold['tsenarozn'] = df_priceold['tsenarozn'].astype(float)

# Объединение всех DataFrame с df_nomenklaturaold
df_final = df_nomenklaturaold.merge(df_priceold, on='kod', how='left')
df_final = df_final.merge(df_stockold, on='kod', how='left')
df_final = df_final.merge(df_photoadress, on='kod', how='left')
df_final = df_final.merge(df_nomenklaturaprimenjaemost_grouped, on='kod', how='left')


# Создание новой колонки "Б/У" на основе naimenovanie
df_final['Б/У'] = df_final['naimenovanie'].apply(lambda x: 'Б/У' if 'б/у' in x.lower() else 'Новый')

# Замена значений в колонке osnsklad и создание новой колонки "Наличие"
df_final['Наличие'] = df_final['osnsklad'].apply(lambda x: 'В наличии' if x > 0 else 'Отсутствует')

# Фильтрация позиций, которые есть в наличии
df_final = df_final[df_final['Наличие'] == 'В наличии']

# Удаление оригинальной колонки osnsklad, так как она заменена на "Наличие"
df_final.drop(columns=['osnsklad', 'kod'], inplace=True)

# Переименование колонок
df_final.rename(columns={
    'artikul': 'Артикул',
    'proizvoditel': 'Производитель',
    'naimenovanie': 'Наименование',
    'edizm': 'Единица измерения',
    'tsenarozn': 'Розничная цена',
    'adress': 'Адреса',
    'model': 'Применяемость'
}, inplace=True)

# Создание колонки "Описание"
df_final['Описание'] = df_final['Наименование'] + "\n\n" + \
    "Автоцентр «Южная Корея» предлагает большой ассортимент автозапчастей для корейских автомобилей. В наличии более 20 000 наименований.\n" + \
    "Наличие и цены на сайте являются актуальными. При необходимости сделаем фото и проконсультируем по применяемости запчасти.\n" + \
    "После того, как Вы оформите заказ, с Вами свяжется менеджер и уточнит все вопросы, подскажет стоимость и сроки доставки.\n" + \
    "Осуществляем отправку во все регионы России транспортными компаниями CDEK, Деловые Линии, Энергия, ПЭК, ЖДЭ, КИТ и др. по согласованию. Не работаем с Почтой России и наложенным платежом."

# Сохранение DataFrame в формате Excel (XLSX)
xlsx_path = os.path.join(output_dir, 'Прайс_дром_обновленные_с_фото.xlsx')
df_final.to_excel(xlsx_path, index=False)

# Объявляем список адресов электронной почты
to_emails = [
    'UgKorea24-price-45877-1cb81c3ae5a3@baza.farpost.ru'
]
subject = 'Прайс Дром Обновленные с Фото'
body = 'Во вложении обновленный прайс с фото.'

# Цикл по списку адресов электронной почты
for to_email in to_emails:
    send_email_via_mailru(to_email, subject, body, xlsx_path)
