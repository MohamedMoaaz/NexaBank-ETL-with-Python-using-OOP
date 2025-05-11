import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from dotenv import load_dotenv

load_dotenv()


class EmailClient:
    def __init__(self):
        self._sender_email = os.getenv("EMAIL_ADDRESS")
        self._server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        self._connected: bool = False
        self._login()

    def _login(self):
        try:
            self._server.login(self._sender_email, os.getenv("EMAIL_PASSWORD"))
            self._connected = True
        except (smtplib.SMTPAuthenticationError, smtplib.SMTPSenderRefused):
            print("[FAIL] Email client is not working")

    def send(self, receiver_email: str, subject: str, body: str) -> bool:
        if not self._connected:
            print("[FAIL] Server is not connected!")
            return False

        message = MIMEMultipart()
        message["From"] = self._sender_email
        message["To"] = receiver_email
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))

        try:
            self._server.send_message(message)
        except smtplib.SMTPSenderRefused:
            print("[FAIL] Cannot send this email!")
            return False

        return True


if __name__ == "__main__":
    receiver_email = "mohamedmoaaz646@gmail.com"
    subject = "Another Test again"
    body = "Another test <3"
    email_client = EmailClient()
    email_client.send(receiver_email, subject, body)
