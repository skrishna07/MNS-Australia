import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os


def send_email(config_dict, subject, body, to_emails, attachment_paths=None):
    try:
        # Email configuration
        sender_email = config_dict['sender_email']
        sender_password = config_dict['sender_password']
        smtp_server = config_dict['smtp_server']
        smtp_port = config_dict['smtp_port']
        to_email_string = ', '.join(to_emails)
        # Create the MIME object
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = to_email_string
        msg['Subject'] = subject

        # Attach body text
        msg.attach(MIMEText(body, 'html'))
        # Attach file if specified
        if attachment_paths:
            for attachment_path in attachment_paths:
                attachment = open(attachment_path, 'rb')
                part = MIMEBase('application', 'octet-stream')
                part.set_payload((attachment).read())
                encoders.encode_base64(part)
                file_name_with_extension = os.path.basename(attachment_path)
                file_name, _ = os.path.splitext(file_name_with_extension)
                if '.xlsx' in attachment_path:
                    file_name = file_name + '.xlsx'
                else:
                    file_name = file_name + '.json'
                part.add_header('Content-Disposition', "attachment; filename= " + file_name)
                msg.attach(part)

        # Connect to the SMTP server
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, to_emails, msg.as_string())

        print("Email sent successfully!")

    except smtplib.SMTPAuthenticationError:
        print("Authentication error. Check your email and password.")
    except smtplib.SMTPException as e:
        print(f"SMTP error: {e}")
    except Exception as e:
        print(f"Error: {e}")
