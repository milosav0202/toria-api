import csv
import json
import urllib.parse
from io import StringIO

import asyncio
import datetime

import aiopg
from aiohttp import web

import config
import pyaes
import hashlib
import base64

routes = web.RouteTableDef()


async def get_remote_name(local_db, username, api_key):
    async with local_db.acquire() as conn:
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
                "name": str(username),
                "key": str(api_key)
            })
            async for remote_name, in cursor:
                return remote_name

        raise PermissionError("Wrong 'username' or 'api-key'")


def api_key_headers(async_handler):
    async def async_wrapper(request):
        if "username" not in request.headers or "api-key" not in request.headers:
            return web.json_response({
                "error": "You must provide 'username' and 'api-key' http headers"
            })

        try:
            request["username"] = await get_remote_name(
                local_db=request.app['local_db'],
                username=str(request.headers["username"]),
                api_key=str(request.headers["api-key"])
            )

        except PermissionError:
            return web.json_response({
                "error": "You provide wrong 'username' or 'api-key' headers"
            })

        return await async_handler(request)
    return async_wrapper


def access_logging(async_handler):
    async def async_wrapper(request):
        async with request.app["local_db"].acquire() as conn:
            async with conn.cursor() as cursor:
                response = await async_handler()

                request_log = """
                     INSERT INTO request_log (datetime, name) 
                         VALUES (%(datetime)s, %(name)s)
                 """
                await cursor.execute(request_log, {
                    "datetime": datetime.datetime.now(),
                    "name": request.headers["username"]
                })

                return response
    return async_wrapper


@routes.post("/")
@api_key_headers
@access_logging
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

    return web.json_response({
        "data": response
    })


def encode_arguments(**arguments):
    aes = pyaes.AESModeOfOperationCTR(hashlib.sha256(config.SECRET_KEY.encode()).digest())
    raw_encoded = aes.encrypt(json.dumps(arguments).encode())
    return urllib.parse.quote(base64.b64encode(raw_encoded).decode())


def decode_arguments(encoded):
    # noinspection PyBroadException
    try:
        raw_encoded = base64.b64decode(urllib.parse.unquote(encoded).encode())
        aes = pyaes.AESModeOfOperationCTR(hashlib.sha256(config.SECRET_KEY.encode()).digest())
        return json.loads(aes.decrypt(raw_encoded).decode())
    except Exception:
        raise ValueError("Failed to decode arguments")


@routes.post("/readings/get")
@api_key_headers
async def readings_get(request):
    post_body = await request.post()
    include_imports = bool(post_body.get('import_reads', False))
    include_exports = bool(post_body.get('export_reads', False))

    if not include_imports and not include_exports:
        return web.json_response({
            "error": "Both import_reads and export_reads are false"
        })

    if "fromdate" not in post_body:
        return web.json_response({
            "error": "Body must contains 'fromdate' (DATE) field"
        })

    if "todate" not in post_body:
        return web.json_response({
            "error": "Body must contains 'todate' (DATE) field"
        })

    return web.json_response({
        'token': encode_arguments(
            import_reads=include_imports,
            export_reads=include_exports,
            fromdate=post_body['fromdate'],
            todate=post_body['todate'],
            username=request.headers["username"],
            api_key=request.headers["api-key"],
        )
    })


@routes.get("/download")
async def download(request):
    if 'token' not in request.query:
        return web.json_response({
            'error': "You must provide 'token' field"
        })

    try:
        body = decode_arguments(request.query['token'])
    except (ValueError, KeyError):
        return web.json_response({
            'error': "File it not available anymore"
        })

    remote_name = await get_remote_name(
        local_db=request.app['local_db'],
        username=body['username'],
        api_key=body['api_key']
    )

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

    include_imports = body['import_reads']
    include_exports = body['export_reads']

    select_names = []
    select_names.extend(both_names)

    if include_imports:
        select_names.extend(import_names)

    if include_exports:
        select_names.extend(export_names)

    select_query = "SELECT " + f"""
        { ','.join(select_names) }
        FROM readings_reading AS {reading_alias}
        INNER JOIN meters_meter AS {meter_alias} ON {meter_alias}.id = {reading_alias}.meter_id
        WHERE {reading_alias}.meter_id IN
        (SELECT meter_id FROM users_profile_meters WHERE profile_id =
        (SELECT id FROM auth_user WHERE username = %(username)s)) 
        AND date >= %(fromdate)s AND date <= %(todate)s; 
    """

    parameters = {
        'fromdate': body["fromdate"],
        'todate': body['todate'],
        'username': remote_name
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

    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M')
    filename = f"{body['username']}_{current_time}_csvexport.csv"

    response = web.StreamResponse()
    response.headers['CONTENT-DISPOSITION'] = f'attachment; filename="{filename}"'
    await response.prepare(request)
    try:
        csv_response.seek(0)
        while True:
            file_part = csv_response.read(4096*4096)
            if len(file_part) <= 0:
                break
            await response.write(file_part.encode())
    finally:
        await response.write_eof()
    return response


@routes.post("/emc1sp/export_query")
@api_key_headers
async def emc1sp_export_query(request):
    body = await request.post()

    if "fromdate" not in body:
        return web.json_response({
            "error": "Body must contains 'fromdate' (DATE) field"
        })

    if "todate" not in body:
        return web.json_response({
            "error": "Body must contains 'todate' (DATE) field"
        })

    select_query = """-- noinspection SqlResolveForFile
        SELECT m.name, m.mpan, m.location, r.date,
            r.export_total_wh, -- Domestic Load kWh
            spc.grid_energy_wh, -- Grid Energy Utilised kWh
            r.import_total_wh, -- Grid Export kWh
            r.export_total_wh_b, -- Solar Storage Utilised kWh
            spc.generation_wh, -- Generation kWh
            spc.charge_wh, -- Battery Charge kWh
            g.import_total_wh -- Gas Total m3
        
        FROM readings_reading AS r
        INNER JOIN readings_gasreading AS g ON (r.name=g.name)
        INNER JOIN readings_spcreading AS spc ON (r.name=spc.name)
        INNER JOIN meters_meter AS m ON (m.id=r.meter_id)
        WHERE r.meter_id IN
        (SELECT meter_id FROM users_profile_meters WHERE profile_id =
        (SELECT id FROM auth_user WHERE username = %(username)s)) --username taken from remote_name of APIKeys
        AND r.date >= %(fromdate)s AND r.date <= %(todate)s; -- from and to dates from body of request
    """

    parameters = {
        'fromdate': body["fromdate"],
        'todate': body['todate'],
        'username': request["username"],
    }

    to_kwh_fields = (
        'domestic_load_kwh',
        'grid_energy_utilised_kwh',
        'grid_export_kwh',
        'solar_storage_utilised_kwh',
        'generation_kwh',
        'battery_charge_kwh',
        'gas_total_m3'
    )

    query_fields = (
        'name', 'reference', 'description', 'date',
        *to_kwh_fields
    )

    response = []
    async with request.app["remote_db"].acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(select_query, parameters)
            async for selected_row in cursor:
                item = dict(zip(query_fields, selected_row))

                for wh_field in to_kwh_fields:
                    item[wh_field] /= 1000

                item['date'] = item['date'].strftime("%Y-%m-%d")
                item['solar_generation_kwh'] = item['generation_kwh'] - item['battery_charge_kwh']
                response.append(item)

    return web.json_response({
        'data': response
    })


async def pg_pool(app):
    async with aiopg.create_pool(config.LOCAL_DATABASE) as local_pool:
        app["local_db"] = local_pool
        async with aiopg.create_pool(config.REMOTE_DATABASE) as remote_pool:
            app["remote_db"] = remote_pool
            yield  # <!> Do not remove this yield.


async def api_app():
    app = web.Application()
    app.cleanup_ctx.append(pg_pool)
    app.add_routes(routes)
    return app


if __name__ == '__main__':
    web.run_app(asyncio.get_event_loop().run_until_complete(api_app()))
