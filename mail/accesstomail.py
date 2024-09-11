import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from ugkorea.mail.config import email, password
import mimetypes
import os
import urllib.parse  # Для кодирования имени файла в формате RFC 2231

def send_email_via_mailru(to_email, subject, body, attachment_path=None):
    from_email = email  # Ваш Mail.ru email
    from_password = password  # Пароль от Mail.ru

    # Создаем объект MIMEMultipart
    msg = MIMEMultipart()
    msg['From'] = formataddr(('Your Name', from_email))  # Можно указать имя отправителя
    msg['To'] = to_email
    msg['Subject'] = subject
    msg['Cc'] = from_email  # Добавляем копию самому себе

    # Добавляем тело письма
    msg.attach(MIMEText(body, 'plain'))

    # Добавляем вложение, если указано
    if attachment_path:
        # Автоматически берем имя файла вместе с его расширением из пути
        attachment_name = os.path.basename(attachment_path)
        
        # Принудительное перекодирование имени файла в UTF-8 с использованием urllib.parse.quote
        encoded_attachment_name = urllib.parse.quote(attachment_name)

        # Определяем MIME-тип файла
        mime_type, _ = mimetypes.guess_type(attachment_path)
        if mime_type is None:
            mime_type = 'application/octet-stream'

        with open(attachment_path, "rb") as attachment:
            part = MIMEBase(*mime_type.split('/'))
            part.set_payload(attachment.read())
            encoders.encode_base64(part)

            # Устанавливаем заголовок для вложения с корректным кодированием имени файла (RFC 2231)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename*=UTF-8\'\'{encoded_attachment_name}'
            )
            msg.attach(part)

    # Устанавливаем соединение с сервером и отправляем письмо
    try:
        server = smtplib.SMTP_SSL('smtp.mail.ru', 465)
        server.login(from_email, from_password)
        text = msg.as_string()
        server.sendmail(from_email, [to_email, from_email], text)  # Отправляем письмо и получателю, и себе
        print(f"Email sent to {to_email} with attachment {attachment_name}, and cc to {from_email}")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")
    finally:
        server.quit()

    print(f"Email отправлен на {to_email} с вложением {attachment_name}, и копия на {from_email}")
