import csv
import random
from io import StringIO

import aiosmtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

import asyncio
import datetime

import aiopg
from aiohttp import web

import config

routes = web.RouteTableDef()


@routes.post("/")
async def index(request):
    # noinspection SqlResolve
    select_query = """
        SELECT
        m.id,
        m.name,
        r.date,
        r.import_total_wh,
        r.import_total
        FROM meters_meter AS m
        INNER JOIN users_profile_meters AS u ON (m.id=u.meter_id)
        INNER JOIN auth_user AS a ON (u.profile_id=a.id)
        INNER JOIN readings_reading AS r ON (m.id=r.meter_id)
        WHERE
        a.username = %(username)s
        AND
        r.date = %(date)s
        AND
        m.name IN %(meters)s
    """

    post_body = await request.post()

    if 'date' not in post_body:
        return web.json_response({
            "error": "Body must contains 'date' (DATE) field"
        })

    if 'meters' not in post_body:
        return web.json_response({
            "error": "Body must contains at least one 'meters' (STRING) field"
        })

    parameters = {
        'username': request["username"],
        'date': post_body['date'],
        'meters': tuple(post_body.getall('meters'))
    }

    response = []
    async with request.app["remote_db"].acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(select_query, parameters)
            async for meter_id, name, date, import_total_wh, import_total in cursor:
                response.append({
                    "id": meter_id,
                    "name": name,
                    "date": date.strftime("%Y-%m-%d"),
                    "import_total_wh": import_total_wh,
                    "import_total": import_total
                })

    await access_logging(request)
    return web.json_response({
        "data": response
    })


@routes.get("/send_email")
async def readings_get(request):
    send_email = request.app['send_email']

    message = MIMEMultipart()
    message['From'] = 'root@localhost'
    message['To'] = 'somebody@example.com'
    message['Subject'] = 'Hello World!'

    message.attach(MIMEText('Sent via aiosmtplib'))
    part = MIMEApplication(
        b'12345 hello',
        Name='hello.txt',
    )
    part['Content-Disposition'] = f'attachment; filename="hello.txt"'
    message.attach(part)

    await send_email(message)
    return web.json_response({
        "data": "Success"
    })


@routes.post("/readings/get")
async def readings_get(request):
    meter_alias = 'm'
    reading_alias = 'r'

    # always included in each request
    both_names = [
        f'{meter_alias}.name',
        f'{meter_alias}.mpan',
        f'{meter_alias}.location',
        f'{reading_alias}.date'
    ]

    # IMPORT READINGS
    import_names = [reading_alias + '.' + name for name in (
        'import_total_wh', 'import_total',
        'import0030', 'import0100', 'import0130', 'import0200', 'import0230', 'import0300', 'import0330', 'import0400',
        'import0430', 'import0500', 'import0530', 'import0600', 'import0630', 'import0700', 'import0730', 'import0800',
        'import0830', 'import0900', 'import0930', 'import1000', 'import1030', 'import1100', 'import1130', 'import1200',
        'import1230', 'import1300', 'import1330', 'import1400', 'import1430', 'import1500', 'import1530', 'import1600',
        'import1630', 'import1700', 'import1730', 'import1800', 'import1830', 'import1900', 'import1930', 'import2000',
        'import2030', 'import2100', 'import2130', 'import2200', 'import2230', 'import2300', 'import2330', 'import0000',
    )]

    # EXPORT READINGS
    export_names = [reading_alias + '.' + name for name in (
        'export_total_wh', 'export_total',
        'export0030', 'export0100', 'export0130', 'export0200', 'export0230', 'export0300', 'export0330', 'export0400',
        'export0430', 'export0500', 'export0530', 'export0600', 'export0630', 'export0700', 'export0730', 'export0800',
        'export0830', 'export0900', 'export0930', 'export1000', 'export1030', 'export1100', 'export1130', 'export1200',
        'export1230', 'export1300', 'export1330', 'export1400', 'export1430', 'export1500', 'export1530', 'export1600',
        'export1630', 'export1700', 'export1730', 'export1800', 'export1830', 'export1900', 'export1930', 'export2000',
        'export2030', 'export2100', 'export2130', 'export2200', 'export2230', 'export2300', 'export2330', 'export0000'
    )]

    # Rename table column name here
    rename_dict = {
        f'{meter_alias}.name': 'name',
        f'{meter_alias}.mpan': 'reference',
        f'{meter_alias}.location': 'description',
        f'{reading_alias}.import_total': 'day_total_wh',
        f'{reading_alias}.export_total': 'export_day_total_wh'
    }

    post_body = await request.post()
    include_imports = bool(post_body.get('import_reads', False))
    include_exports = bool(post_body.get('export_reads', False))

    if not include_imports and not include_exports:
        return web.json_response({
            "error": "Both import_reads and export_reads are false"
        })

    select_names = []
    select_names.extend(both_names)

    if include_imports:
        select_names.extend(import_names)

    if include_exports:
        select_names.extend(export_names)

    select_query = (f"""
        SELECT { ','.join(select_names) }
        FROM readings_reading AS {reading_alias}
        INNER JOIN meters_meter AS {meter_alias} ON {meter_alias}.id = {reading_alias}.meter_id
        WHERE {reading_alias}.meter_id IN
        (SELECT meter_id FROM users_profile_meters WHERE profile_id =
        (SELECT id FROM auth_user WHERE username = %(username)s)) 
        AND date >= %(fromdate)s AND date <= %(todate)s; 
    """)

    if "fromdate" not in post_body:
        return web.json_response({
            "error": "Body must contains 'fromdate' (DATE) field"
        })

    if "todate" not in post_body:
        return web.json_response({
            "error": "Body must contains 'todate' (DATE) field"
        })

    if "receiver_email" not in post_body:
        return web.json_response({
            "error": "Body must contains 'receiver_email' (STRING) field"
        })

    parameters = {
        'fromdate': post_body["fromdate"],
        'todate': post_body['todate'],
        'username': request['username']
    }

    csv_response = StringIO()
    writer = csv.DictWriter(csv_response, select_names)
    writer.writerow({
        selected_column: (rename_dict.get(selected_column) or selected_column.replace(f'{reading_alias}.', ''))
        for selected_column in select_names
    })

    async with request.app["remote_db"].acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(select_query, parameters)
            async for selected_row in cursor:
                response_row = {}
                for selected_column, value in zip(select_names, selected_row):
                    if isinstance(value, (datetime.date, datetime.datetime)):
                        value = value.strftime("%Y-%m-%d")
                    response_row[selected_column] = value
                writer.writerow(response_row)

    csv_response.seek(0)

    send_email = request.app['send_email']

    message = MIMEMultipart()
    message['From'] = post_body.get('sender_email', config.SMTP_SENDER)
    message['To'] = post_body['receiver_email']
    message['Subject'] = post_body.get('message_subject', 'readings')

    if 'message_content' in post_body:
        message.attach(MIMEText(post_body['message_content']))

    filename = post_body.get('filename', 'readings.csv')
    part = MIMEApplication(
        csv_response.read().encode(),
        Name=filename
    )
    part['Content-Disposition'] = f'attachment; filename="{filename}"'
    message.attach(part)

    await send_email(message)
    return web.json_response({
        "data": "success"
    })


async def access_logging(request):
    async with request.app["local_db"].acquire() as conn:
        async with conn.cursor() as cursor:
            request_log = """
                 INSERT INTO request_log (datetime, name) 
                     VALUES (%(datetime)s, %(name)s)
             """
            await cursor.execute(request_log, {
                "datetime": datetime.datetime.now(),
                "name": request.headers["username"]
            })


@web.middleware
async def api_key_middleware(request, handler):
    if "username" not in request.headers or "api-key" not in request.headers:
        return web.json_response({
            "error": "You must provide 'username' and 'api-key' http headers"
        })

    async with request.app["local_db"].acquire() as conn:
        async with conn.cursor() as cursor:
            select_username = """
                SELECT keys.remote_name
                FROM api_keys as keys
                WHERE 
                  NOT keys.disabled
                AND
                  keys.name = %(name)s 
                AND 
                  keys.key = %(key)s
            """
            await cursor.execute(select_username, {
                "name": str(request.headers["username"]),
                "key": str(request.headers["api-key"])
            })
            async for remote_name, in cursor:
                request["username"] = remote_name

    if "username" not in request:
        return web.json_response({
            "error": "You provide wrong 'username' or 'api-key' headers"
        })

    return await handler(request)


async def pg_pool(app):
    async with aiopg.create_pool(config.LOCAL_DATABASE) as local_pool:
        app["local_db"] = local_pool
        async with aiopg.create_pool(config.REMOTE_DATABASE) as remote_pool:
            app["remote_db"] = remote_pool
            yield  # <!> Do not remove this yield.


async def smtp_pool(app):
    semaphore = asyncio.Semaphore(value=1)
    connection_pool = []

    async def connect():
        smtp = aiosmtplib.SMTP(hostname=config.SMTP_HOST, port=config.SMTP_PORT)
        await smtp.connect()
        await smtp.login(username=config.SMTP_USERNAME, password=config.SMTP_PASSWORD)
        connection_pool.append(smtp)

    async def send_message(message, **kwargs):
        for _ in range(config.SMTP_SENDING_ATTEMPTS):
            async with semaphore:
                while len(connection_pool) < config.SMTP_CONNECTIONS:
                    await connect()

            smtp = connection_pool.pop(random.randint(0, config.SMTP_CONNECTIONS - 1))
            try:
                await smtp.send_message(message, config.SMTP_SENDER, **kwargs)
                connection_pool.append(smtp)
                return
            except aiosmtplib.SMTPException as ex:
                print(type(ex), ex)

    app['send_email'] = send_message

    yield  # <!> Do not remove this yield.

    for connection in connection_pool:
        connection.close()


async def api_app():
    app = web.Application(
        middlewares=[
            api_key_middleware
        ]
    )
    app.cleanup_ctx.append(pg_pool)
    app.cleanup_ctx.append(smtp_pool)
    app.add_routes(routes)
    return app


if __name__ == '__main__':
    web.run_app(asyncio.get_event_loop().run_until_complete(api_app()))
