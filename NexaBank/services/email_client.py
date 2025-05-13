"""
email_client.py

This module defines the `EmailClient` class for sending emails via Gmail SMTP
using credentials stored in environment variables.

Environment variables required:
- EMAIL_ADDRESS: Sender's Gmail address
- EMAIL_PASSWORD: App password or Gmail account password (if allowed)

Features:
- Establishes secure connection with Gmail SMTP server
- Sends plain-text email messages
- Provides connection and error handling
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from dotenv import load_dotenv

load_dotenv()


class EmailClient:
    """
    A class to send plain text emails using Gmail's SMTP over SSL.

    Attributes:
        _sender_email (str): Sender's email address from environment variable.
        _server (smtplib.SMTP_SSL): SMTP client for secure communication.
        _connected (bool): Indicates if the SMTP connection is authenticated.
    """

    def __init__(self):
        """
        Initialize the EmailClient instance and attempt login.
        """
        self._sender_email = os.getenv("EMAIL_ADDRESS")
        self._server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        self._connected: bool = False
        self._login()

    def _login(self):
        """
        Attempt to log in to the SMTP server using environment credentials.

        Sets the `_connected` flag to True if successful.
        """
        try:
            self._server.login(self._sender_email, os.getenv("EMAIL_PASSWORD"))
            self._connected = True
        except (smtplib.SMTPAuthenticationError, smtplib.SMTPSenderRefused):
            print("[FAIL] Email client is not working")

    def send(self, receiver_email: str, subject: str, body: str) -> bool:
        """
        Send an email to the specified recipient.

        Args:
            receiver_email (str): Recipient's email address.
            subject (str): Subject line of the email.
            body (str): Plain text content of the email.

        Returns:
            bool: True if sent successfully, False otherwise.
        """
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
    # Example usage for testing
    receiver_email = "mohamedmoaaz646@gmail.com"
    subject = "Another Test again"
    body = "Another test <3"
    email_client = EmailClient()
    email_client.send(receiver_email, subject, body)