import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from ugkorea.mail.config import email, password
from email.utils import formataddr

def send_email_via_mailru(to_email, subject, body, attachment_path=None, attachment_name=None):
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
        with open(attachment_path, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            if not attachment_name:
                attachment_name = attachment_path.split('/')[-1]

            # Добавляем правильные заголовки для имени файла и типа контента
            part.add_header(
                'Content-Disposition',
                f'attachment; filename*="UTF-8\'\'{attachment_name}"'
            )
            part.add_header(
                'Content-Type',
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                name=attachment_name
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


    print(f"Email sent to {to_email} with attachment {attachment_name}, and cc to {from_email}")
