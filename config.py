REMOTE_DATABASE = "**************************"
LOCAL_DATABASE = "**************************"

SECRET_KEY = "This is a dev key!"

try:
    from config_production import *
except Exception:
    pass
