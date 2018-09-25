REMOTE_DATABASE = "**************************"
LOCAL_DATABASE = "**************************"

SMTP_HOST = 'smtp.mailtrap.io'
SMTP_PORT = 2525

SMTP_SENDER = 'noreply@rhinoda.com'
SMTP_USERNAME = "f23b91b182122a"
SMTP_PASSWORD = "25463637b5f48b"
SMTP_CONNECTIONS = 1
SMTP_SENDING_ATTEMPTS = 5

try:
    from config_production import *
except Exception:
    pass
