import os
import datetime
import pytz
import imaplib
import email
from email.header import decode_header
from ugkorea.mail.config import email as address, password

def clean_output_folder(folder_path):
    """Очистка папки от файлов."""
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f"Не удалось удалить {file_path}: {e}")

def get_filtered_messages_mail_ru(senders, email_user, email_pass, one_day_ago):
    """Извлечение сообщений с сервера Mail.ru."""
    try:
        mail = imaplib.IMAP4_SSL("imap.mail.ru")
        mail.login(email_user, email_pass)
        mail.select("inbox")  # Открываем папку "Входящие"
        filtered_messages = []

        # Получаем все сообщения, пришедшие за последние сутки
        status, messages = mail.search(None, '(SINCE "{}")'.format(one_day_ago.strftime("%d-%b-%Y")))
        if status != 'OK':
            print("Ошибка поиска сообщений")
            return []

        for num in messages[0].split():
            status, msg_data = mail.fetch(num, "(RFC822)")
            if status != 'OK':
                print(f"Ошибка получения письма {num}")
                continue

            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    sender = msg["From"].lower()
                    if any(s in sender for s in senders):
                        filtered_messages.append(msg)

        mail.logout()
        return sorted(filtered_messages, key=lambda m: m["Date"], reverse=True)  # Сортируем по дате
    except Exception as e:
        print(f"Ошибка при подключении к Mail.ru: {e}")
        return []

def decode_mime_words(s):
    """Декодирование MIME-заголовков."""
    return ''.join(
        word.decode(encoding or 'utf-8') if isinstance(word, bytes) else word
        for word, encoding in decode_header(s)
    )

def save_attachments_mail_ru(messages, save_folder):
    """Сохранение вложений из писем Mail.ru."""
    attachments_saved = {}
    
    if not messages:
        print("Нет сообщений для обработки")
        return

    print(f"Обработка {len(messages)} сообщений...")
    
    for msg in messages:
        sender = decode_mime_words(msg["From"]).lower()
        if "noreply@berg.ru" == sender and "прайс" not in msg["Subject"].lower():
            continue
        received_time = msg["Date"]
        print(f"Обработка письма от {sender} с датой {received_time}...")
        
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_maintype() == "multipart":
                    continue
                if part.get("Content-Disposition") is None:
                    continue
                filename = part.get_filename()
                if filename:
                    filename = decode_mime_words(filename)
                    full_path = os.path.join(save_folder, filename)
                    try:
                        with open(full_path, "wb") as f:
                            f.write(part.get_payload(decode=True))
                        # Группируем по отправителю и имени файла
                        attachments_saved.setdefault(sender, {}).setdefault(filename, []).append((full_path, received_time))
                        print(f"Сохранено вложение от {sender} с именем файла {filename}")
                    except Exception as e:
                        print(f"Ошибка при сохранении файла {filename}: {e}")
    
    keep_only_latest_files(attachments_saved)

def keep_only_latest_files(attachments_saved):
    """Оставляем только последние файлы для каждого имени вложения от одного отправителя."""
    for sender, files_by_name in attachments_saved.items():
        for filename, files in files_by_name.items():
            # Найти последний файл по времени
            latest_file = max(files, key=lambda x: x[1])
            # Удалить все, кроме самого последнего
            for file, _ in files:
                if file != latest_file[0]:
                    try:
                        os.unlink(file)
                        print(f"Удалён старый файл: {file} для вложения {filename} от {sender}")
                    except Exception as e:
                        print(f"Ошибка при удалении файла {file}: {e}")

def find_repo_folder(start_path, folder_structure):
    """Рекурсивный поиск папки с указанной структурой."""
    for root, dirs, files in os.walk(start_path):
        # Создаём полный путь для проверки
        full_path = os.path.join(root, folder_structure)
        if os.path.isdir(full_path):
            return full_path
    return None

def mail_ru_utile():
    senders = ["prices_export@shate-m.com", "post@mx.forum-auto.ru", "prices@favorit-parts.ru", "krsk_price@mail2.tpm.ru", "noreply@berg.ru"]
    
    # Задаём структуру папок для поиска
    target_folder_structure = "repo\\ugkorea\\Output"
    
    # Начинаем поиск с диска C:
    save_folder = find_repo_folder("C:\\", target_folder_structure)
    
    if not save_folder:
        print(f"Папка {target_folder_structure} не найдена.")
        return
    
    print(f"Папка найдена: {save_folder}")
    clean_output_folder(save_folder)
    
    one_day_ago = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=1)

    # Пытаемся работать с Mail.ru
    try:
        email_user = address
        email_pass = password
        filtered_messages = get_filtered_messages_mail_ru(senders, email_user, email_pass, one_day_ago)
        print(f"Найдено сообщений в Mail.ru: {len(filtered_messages)}")
        if filtered_messages:
            save_attachments_mail_ru(filtered_messages, save_folder)
            print("Операция завершена успешно.")
        else:
            print("Сообщений не найдено в Mail.ru")
    except Exception as e:
        print(f"Ошибка при работе с Mail.ru: {e}")

if __name__ == "__main__":
    mail_ru_utile()
