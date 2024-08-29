import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from ugkorea.mail.config import email, password

def send_email_via_mailru(to_email, subject, body, attachment_path=None):
    from_email = email  # Ваш Mail.ru email
    from_password = password  # Пароль от Mail.ru

    # Создаем объект MIMEMultipart
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    # Добавляем тело письма
    msg.attach(MIMEText(body, 'plain'))

    # Добавляем вложение, если указано
    if attachment_path:
        attachment = open(attachment_path, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename= {attachment_path.split('/')[-1]}")
        msg.attach(part)

    # Устанавливаем соединение с сервером и отправляем письмо
    server = smtplib.SMTP_SSL('smtp.mail.ru', 465)
    server.login(from_email, from_password)
    text = msg.as_string()
    server.sendmail(from_email, to_email, text)
    server.quit()

    print(f"Email sent to {to_email}")
