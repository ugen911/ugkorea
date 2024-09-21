#!/usr/bin/env python3

import imaplib
import datetime
from ugkorea.mail.config import email, password

# Данные для подключения
email_user = email
email_pass = password

# Словарь для отображения пользовательских имен папок
folder_names = {
    "inbox": "Входящие",
    "&BB4EQgQ,BEAEMAQyBDsENQQ9BD0ESwQ1-": "Исходящие"
}

# Функция для удаления писем старше определённого количества дней с отладочными выводами
def delete_old_emails(email_user, email_pass, folders=["inbox", "&BB4EQgQ,BEAEMAQyBDsENQQ9BD0ESwQ1-"], days=30):
    try:
        # Подключение к почтовому серверу
        print("Подключение к почтовому серверу...")
        mail = imaplib.IMAP4_SSL("imap.mail.ru")
        mail.login(email_user, email_pass)
        print("Успешное подключение к почтовому серверу.")

        # Получаем дату для сравнения (сегодня минус N дней)
        date_cutoff = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%d-%b-%Y")
        print(f"Ищем письма старше {days} дней (до {date_cutoff}).")

        # Проход по каждой указанной папке
        for folder in folders:
            folder_display_name = folder_names.get(folder, folder)  # Используем человеко-понятное имя папки
            print(f"\nРабота с папкой '{folder_display_name}'...")
            
            # Выбор папки
            result = mail.select(folder)
            if result[0] != 'OK':
                print(f"Не удалось выбрать папку '{folder_display_name}'. Пропуск.")
                continue

            # Поиск писем старше указанного срока
            result, data = mail.search(None, f'BEFORE {date_cutoff}')
            if result == "OK":
                email_ids = data[0].split()
                total_emails = len(email_ids)
                print(f"Найдено писем для удаления: {total_emails} в папке '{folder_display_name}'")

                if total_emails == 0:
                    print(f"Нет писем для удаления в папке '{folder_display_name}'.")
                else:
                    # Удаление писем
                    for num in email_ids:
                        mail.store(num, '+FLAGS', '\\Deleted')
                        print(f"Письмо с ID {num.decode()} в папке '{folder_display_name}' помечено для удаления.")

                    # Окончательное удаление писем
                    mail.expunge()
                    print(f"Удалено писем: {total_emails} из папки '{folder_display_name}'")

            else:
                print(f"Ошибка поиска писем в папке '{folder_display_name}'.")

        # Закрытие соединения
        mail.logout()
        print("Соединение закрыто.")

    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")

# Основной блок
if __name__ == "__main__":
    # Вызов функции с удалением писем во входящих и исходящих
    delete_old_emails(email_user, email_pass)
