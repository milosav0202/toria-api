# How to deploy the application

##1. Install Python3 and virtualenv
```commandline
apt install python3
apt install python3-pip
pip3 install virtualenv
```

##2. Create virtual environment
```commandline
mkdir /root/venvs
virtualenv /root/venvs/small_api
source /root/venvs/small_api/bin/activate
```

##3. Clone the application
```commandline
mkdir /root/apps/
cd /root/apps/
git clone https://.../small_api.git
cd small_api/
```

##4. Install requirements
```commandline
pip install -r requirements.txt
```

##5. Ensure that api_keys and request_log tables created
```postgresql
CREATE TABLE api_keys (
    id SERIAL PRIMARY KEY,
    comment TEXT,
    disabled BOOLEAN,
    key VARCHAR(256),
    name VARCHAR(128),
    remote_name VARCHAR(128),
    UNIQUE (key, name, remote_name)
);
```
```postgresql
CREATE TABLE request_log (
    id SERIAL PRIMARY KEY,
    datetime TIMESTAMP,
    name VARCHAR(128)
);
```

##6. Create a production config
```commandline
cd /root/apps/small_api/
touch config_production.py
```

Copy following lines and fill with your settings.
```python
REMOTE_DATABASE = "postgres://........."
LOCAL_DATABASE = "postgres://........."
SECRET_KEY = "PRODUCTION SECRET KEY STRING (KEEP IT IN SECRET)"
```

##7. Install and configure supervisor
```commandline
apt-get install supervisor
cd /etc/supervisor/conf.d/
touch smallapi.conf
```

Fill smallapi.conf with lines:

```text
[program:smallapi]
command=/root/venvs/small_api/bin/gunicorn app:api_app -c /root/apps/small_api/gunicorn.conf.py
directory=/root/apps/small_api
autorestart=true
redirect_stderr=true
```

##8. Update supervisor
```commandline
supervisorctl reread
supervisorctl update
supervisorctl status smallapi
```

##9. Configure nginx
```commandline
cd /etc/nginx/sites-available/
nano default
```

Fill with following lines:
```text
server {
    listen 80;
    server_name ***.***.***.***; # your host IP 
    access_log  /var/log/nginx/example.log;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $server_name;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```
