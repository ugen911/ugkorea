import os
import datetime
import win32com.client
import pytz

def clean_output_folder(folder_path):
    """Очистка папки от файлов."""
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f"Не удалось удалить {file_path}: {e}")

def get_filtered_messages(senders, one_day_ago):
    """Извлечение сообщений из всех подключенных почтовых аккаунтов."""
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    filtered_messages = []

    def process_folder(folder):
        if folder.Name.lower() in ["inbox", "входящие"]:
            messages = folder.Items
            messages.Sort("[ReceivedTime]", True)
            for message in messages:
                if message.ReceivedTime > one_day_ago:
                    sender_email = message.SenderEmailAddress.lower()
                    if sender_email in senders or any(sender_email.endswith(s) for s in senders):
                        filtered_messages.append(message)

        if folder.Folders.Count > 0:
            for subfolder in folder.Folders:
                process_folder(subfolder)

    for account in outlook.Folders:
        for folder in account.Folders:
            process_folder(folder)

    return filtered_messages

def save_attachments_from_messages(messages, save_folder):
    """Сохранение вложений из сообщений, со специальными условиями для некоторых отправителей."""
    attachments_saved = {}
    for message in messages:
        sender_email = message.SenderEmailAddress.lower()
        if "noreply@berg.ru" == sender_email and "прайс" not in message.Subject.lower():
            continue
        for attachment in message.Attachments:
            filename = f"{message.ReceivedTime.strftime('%Y-%m-%d_%H-%M-%S')}_{attachment.FileName}"
            full_path = os.path.join(save_folder, filename)
            attachment.SaveAsFile(full_path)
            attachments_saved.setdefault(sender_email, []).append((full_path, message.ReceivedTime))
            print(f"Сохранено вложение от {sender_email}: {filename}")
    keep_only_latest_files(attachments_saved)

def keep_only_latest_files(attachments_saved):
    """Оставляем только последние файлы по специфическим условиям."""
    special_criteria = {
        "prices_export@shate-m.com": ["ekaterinburg", "podolsk"],
        "post@mx.forum-auto.ru": ["center", "nvs"]
    }

    for sender, files in attachments_saved.items():
        if sender in special_criteria:
            for criterion in special_criteria[sender]:
                relevant_files = [file for file in files if criterion in file[0].lower()]
                if relevant_files:
                    latest_file = max(relevant_files, key=lambda x: x[1])
                    for file, _ in relevant_files:
                        if file != latest_file[0]:
                            os.unlink(file)
                            print(f"Удалён старый файл: {file}")
        else:
            if len(files) > 1:
                latest_file = max(files, key=lambda x: x[1])
                for file, _ in files:
                    if file != latest_file[0]:
                        os.unlink(file)
                        print(f"Удалён старый файл: {file}")

def find_repo_folder(start_path, folder_structure):
    """Рекурсивный поиск папки с указанной структурой."""
    for root, dirs, files in os.walk(start_path):
        # Создаём полный путь для проверки
        full_path = os.path.join(root, folder_structure)
        if os.path.isdir(full_path):
            return full_path
    return None

def outlook_utile():
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
    filtered_messages = get_filtered_messages(senders, one_day_ago)
    print(f"Найдено сообщений: {len(filtered_messages)}")
    save_attachments_from_messages(filtered_messages, save_folder)

if __name__ == "__main__":
    outlook_utile()
