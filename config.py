REMOTE_DATABASE = "**************************"
LOCAL_DATABASE = "**************************"

# Ensure that table api_keys is created!
# CREATE TABLE api_keys (
#     id SERIAL PRIMARY KEY,
#     comment TEXT,
#     disabled BOOLEAN,
#     key VARCHAR(256),
#     name VARCHAR(128),
#     remote_name VARCHAR(128),
#     UNIQUE (key, name, remote_name)
# );

# <!> Ensure that table request_log is created!
# CREATE TABLE request_log (
#     id SERIAL PRIMARY KEY,
#     datetime TIMESTAMP,
#     name VARCHAR(128)
# );

# noinspection PyBroadException
try:
    from config_production import *
except Exception:
    pass

