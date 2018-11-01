# use python server.py --config config
# <!> without .py
# or set environment variables:
# export OPENMETRICS_DSN=postgres://postgres:password@localhost:5432/openmetrics
OPENMETRICS_DSN = "postgres://postgres:12345@localhost:5432/openmetrics"
LOCAL_STORAGE_DSN = "postgres://postgres:12345@localhost:5432/api_keys"
SECRET_KEY = "This is a dev key!"
