#!/usr/bin/env python3

import imaplib
import datetime
from ugkorea.mail.config import email, password

# Данные для подключения
email_user = email
email_pass = password

# Функция для удаления писем старше 10 дней с отладочными выводами
def delete_old_emails(email_user, email_pass, folder="inbox", days=5):
    try:
        # Подключение к почтовому серверу
        print("Подключение к почтовому серверу...")
        mail = imaplib.IMAP4_SSL("imap.mail.ru")
        mail.login(email_user, email_pass)
        print("Успешное подключение к почтовому серверу.")

        # Выбор папки (входящие письма)
        mail.select(folder)
        print(f"Выбрана папка '{folder}'.")

        # Получаем дату для сравнения (сегодня минус 10 дней)
        date_cutoff = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%d-%b-%Y")
        print(f"Ищем письма старше {days} дней (до {date_cutoff}).")

        # Поиск писем старше 10 дней
        result, data = mail.search(None, f'BEFORE {date_cutoff}')

        if result == "OK":
            email_ids = data[0].split()
            total_emails = len(email_ids)
            print(f"Найдено писем для удаления: {total_emails}")

            if total_emails == 0:
                print("Нет писем для удаления.")
            else:
                # Удаление писем
                for num in email_ids:
                    mail.store(num, '+FLAGS', '\\Deleted')
                    print(f"Письмо с ID {num.decode()} помечено для удаления.")

                # Окончательное удаление писем
                mail.expunge()
                print(f"Удалено писем: {total_emails}")

        else:
            print("Ошибка поиска писем.")

        # Закрытие соединения
        mail.logout()
        print("Соединение закрыто.")

    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")

# Основной блок
if __name__ == "__main__":
    # Вызов функции
    delete_old_emails(email_user, email_pass)
