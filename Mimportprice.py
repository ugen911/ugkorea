import os
import datetime
import win32com.client
import pytz

# Список отправителей
senders = ["prices_export@shate-m.com", "post@mx.forum-auto.ru", "PRICES@FAVORIT-PARTS.RU", "krsk_price@mail2.tpm.ru", "noreply@berg.ru"]

# Папка для сохранения вложений
save_folder = r"C:\Users\evgen\repo\ugkorea\Output"

# Очистка папки от содержимого
for file in os.listdir(save_folder):
    file_path = os.path.join(save_folder, file)
    try:
        if os.path.isfile(file_path):
            os.unlink(file_path)
    except Exception as e:
        print(f"Не удалось удалить {file_path}: {e}")

# Создаем объект для работы с Outlook
outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")

# Получаем текущую дату и время
current_date = datetime.datetime.now()

# Получаем дату и время минус 1 день с учетом часового пояса
one_day_ago = current_date - datetime.timedelta(days=1)
one_day_ago = pytz.utc.localize(one_day_ago)  # Приводим one_day_ago к UTC

# Формируем запрос для поиска писем за последние 24 часа
filter_str = f"[ReceivedTime] >= '{one_day_ago.strftime('%m/%d/%Y %H:%M:%S')}'"

# Собираем запрос для каждого отправителя
for sender in senders:
    filter_str += f" OR [SenderEmailAddress] = '{sender}'"

# Получаем письма, соответствующие запросу
messages = outlook.GetDefaultFolder(6).Items.Restrict(filter_str)

# Фильтруем письма по времени и отправителям
filtered_messages = []
for message in messages:
    # Преобразуем время получения письма в UTC
    received_time = message.ReceivedTime.astimezone(pytz.utc)
    if message.SenderEmailAddress.lower() in senders and received_time >= one_day_ago:
        filtered_messages.append(message)

# Сохраняем вложения из отобранных писем
for message in filtered_messages:
    sender_email = message.SenderEmailAddress.lower()
    if sender_email == "noreply@berg.ru" and "прайс" not in message.Subject.lower():
        continue  # Пропускаем письма от Berg.ru без слова "прайс" в теме
    for attachment in message.Attachments:
        if attachment.FileName:
            # Создаем имя файла с добавлением даты и времени письма
            attachment_date = message.ReceivedTime.strftime("%Y-%m-%d_%H-%M-%S")
            attachment_name, attachment_ext = os.path.splitext(attachment.FileName)
            new_filename = f"{attachment_date}_{attachment_name}{attachment_ext}"
            # Сохраняем вложение в папку
            try:
                attachment.SaveAsFile(os.path.join(save_folder, new_filename))
                print(f"Сохранено вложение от {message.SenderName} ({message.SenderEmailAddress}): {new_filename}")
            except Exception as e:
                print(f"Ошибка при сохранении вложения от {message.SenderName} ({message.SenderEmailAddress}): {e}")

print("Вложения успешно сохранены.")

inbox = outlook.GetDefaultFolder(6)

# Получаем все сообщения от указанного отправителя
sender = "PRICES@FAVORIT-PARTS.RU"
messages = inbox.Items
messages.Sort("[ReceivedTime]", True)
filtered_messages = messages.Restrict(f"[SenderEmailAddress] = '{sender}'")

# Получаем последнее сообщение
latest_message = filtered_messages.GetFirst()

# Получаем последнее вложение
if latest_message:
    attachments = latest_message.Attachments
    if attachments.Count > 0:
        # Получаем последнее вложение
        last_attachment = attachments.Item(attachments.Count)
        # Создаем имя файла с добавлением даты и времени письма
        attachment_date = latest_message.ReceivedTime.strftime("%Y-%m-%d_%H-%M-%S")
        file_path = os.path.join(save_folder, f"{attachment_date}_{last_attachment.FileName}")
        # Сохраняем вложение
        last_attachment.SaveAsFile(file_path)
        print("Последнее вложение сохранено в:", file_path)
    else:
        print("Нет вложений в последнем сообщении от", sender)
else:
    print("Нет сообщений от", sender)


def keep_latest_file_with_phrase(folder_path, phrase):
    files = os.listdir(folder_path)
    filtered_files = {}

    for file_name in files:
        # Пропускаем файлы, которые не содержат нужную фразу
        if phrase.lower() in file_name.lower():
            try:
                # Парсим дату и время из имени файла
                datetime_str = '_'.join(file_name.split('_')[:2])  # Получаем 'ГГГГ-ММ-ДД_чч-мм-сс'
                datetime_obj = datetime.datetime.strptime(datetime_str, '%Y-%m-%d_%H-%M-%S')
                filtered_files[file_name] = datetime_obj
            except ValueError as e:
                print(f"Ошибка в имени файла {file_name}: {e}")

    if filtered_files:
        # Находим файл с самой поздней датой
        latest_file = max(filtered_files, key=filtered_files.get)
        latest_datetime = filtered_files[latest_file]

        for file_name, datetime_obj in filtered_files.items():
            if datetime_obj != latest_datetime:
                # Если файл не последний, удаляем его
                file_path = os.path.join(folder_path, file_name)
                os.remove(file_path)
                print(f"Deleted '{file_name}'")

# Пути к папке с файлами
folder_path = r"C:\Users\evgen\repo\ugkorea\Output"

# Список фраз для проверки в именах файлов
phrases = ["FORUM_AUTO_PRICE_CENTER", "FORUM_AUTO_PRICE_NVS", "export_Ekaterinburg", "export_Podolsk", "FAVORIT", "Прайс", "BERG"]

# Применяем функцию для каждой фразы
for phrase in phrases:
    print(f"Processing for phrase '{phrase}':")
    keep_latest_file_with_phrase(folder_path, phrase)