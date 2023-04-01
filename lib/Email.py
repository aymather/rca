from .env import SMTP_KEY, SMTP_SECRET
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import smtplib

SMTP_HOST = 'clops-smtp-ep.smecloudops.com'
SMTP_PORT = 587
SMTP_USER = 'ccshr-bus-ses2'
SMTP_SENDER = 'graphitti@sonymusic.com'


class Email:

    def send(self, receivers, subject, body, files = []):

        """
            Send an email to any number of people, optionally with file attachments.

            @param files
                {
                    "path": <fullfile path to the file>,
                    "filename": <whatever you want to call the file inside the email, include the extension>
                }
        """
        
        try:
            
            # Init SMTP
            smtpObj = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
            smtpObj.starttls()
            smtpObj.login(SMTP_KEY, SMTP_SECRET)
            
            # Create 'to' addresses, if receivers is an array then convert
            to = receivers
            if isinstance(receivers, list):
                to = ','.join(receivers)
            
            # Build message
            body = f'{body}\n\nSincerely,\nGraphitti Team'
            
            # Build multipart message
            msg = MIMEMultipart()
            body = MIMEText(body, 'plain')
            msg['From'] = SMTP_SENDER
            msg['Subject'] = subject
            msg['To'] = to
            msg.attach(body)
            
            # Attach files
            for file_obj in files:
                
                # Open file
                with open(file_obj['path'],'rb') as file:
                    
                    # Attach the file with filename to the email
                    msg.attach(MIMEApplication(file.read(), Name=file_obj['filename']))
            
            # Send
            smtpObj.send_message(msg)
            
            # Close SMTP
            smtpObj.quit()
            
            print('Email successfully sent to:')
            print(receivers)
            
        except smtplib.SMTPException as e:
            
            print('Unable to send email')
            print(str(e))