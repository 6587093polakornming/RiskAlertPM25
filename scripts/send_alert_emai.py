import smtplib
from email.message import EmailMessage
from pathlib import Path
import configparser

def email_alert(subject, body, to):
    msg = EmailMessage()
    msg.set_content(body)
    msg["subject"] = subject
    msg["to"] = to

    user = "supakorn.ming@gmail.com"

    BASE_DIR = Path(__file__).resolve().parent.parent
    CONFIG_PATH = BASE_DIR / "config" / "config.conf"
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    print("File Config Path:", CONFIG_PATH)
    password = config.get("email", "password")

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(user, password)
    server.send_message(msg)

    server.quit()

if __name__ == "__main__":
    print("Start Send Alert Email...")
    email_alert("Test Send Email", "Hello World", "polakorn.ana@student.mahidol.edu")
    print("Complete Send Alert Email")