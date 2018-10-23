import csv
import datetime
import io

from aiohttp import web
from server.utility import user_keys, tokens, database, config

endpoints = web.RouteTableDef()


async def emc1sp_query_iter(openmetrics, remote_name, from_date, to_date):
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
        'fromdate': from_date,
        'todate': to_date,
        'username': remote_name,
    }

    to_kwh_fields = (
        'domestic_load_kwh',
        'grid_energy_utilised_kwh',
        'grid_export_kwh',
        'solar_storage_utilised_kwh',
        'generation_kwh',
        'battery_charge_kwh',
    )

    query_fields = (
        'name', 'reference', 'description', 'date',
        *to_kwh_fields,
        'gas_total_m3'
    )

    async with openmetrics as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(select_query, parameters)
            async for selected_row in cursor:
                item = dict(zip(query_fields, selected_row))

                for wh_field in to_kwh_fields:
                    item[wh_field] /= 1000

                item['date'] = item['date'].strftime("%Y-%m-%d")
                item['solar_generation_kwh'] = item['generation_kwh'] - item['battery_charge_kwh']
                yield item


@tokens.register_token_handler('emc1sp/exp')
async def emc1sp_csv(request, request_args):
    field_names = (
        'name', 'reference', 'description', 'date',
        'domestic_load_kwh',
        'grid_energy_utilised_kwh',
        'grid_export_kwh',
        'solar_storage_utilised_kwh',
        'generation_kwh',
        'battery_charge_kwh',
        'solar_generation_kwh',
        'gas_total_m3',
    )

    csv_response = io.StringIO()
    writer = csv.DictWriter(csv_response, field_names)
    writer.writeheader()

    from_date = request_args['fd']
    to_date = request_args['td']
    remote_name = await user_keys.get_remote_username(
        local_db=database.local_storage(request.app),
        username=request_args['usr'],
        api_key=request_args['key']
    )

    async for response_row in emc1sp_query_iter(database.openmetrics(request.app), remote_name, from_date, to_date):
        writer.writerow(response_row)

    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M')
    filename = f"{request_args['usr']}_{current_time}_csvexport.csv"

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


@endpoints.post("/emc1sp/csv_token")
@user_keys.access_headers
async def emc1sp_csv_token(request):
    body = await request.post()

    if "fromdate" not in body:
        return web.json_response({
            "error": "Body must contains 'fromdate' (DATE) field"
        })
    from_date = body['fromdate']

    if "todate" not in body:
        return web.json_response({
            "error": "Body must contains 'todate' (DATE) field"
        })
    to_date = body['todate']

    return web.json_response({
        'token': tokens.create_request_token(config(request.app, 'SECRET_KEY'), emc1sp_csv, **{
            'usr': request.headers["username"],
            'key': request.headers["api-key"],
            'fd': from_date,
            'td': to_date
        })
    })


@endpoints.post("/emc1sp/json")
@user_keys.access_headers
async def emc1sp_json(request):
    body = await request.post()

    if "fromdate" not in body:
        return web.json_response({
            "error": "Body must contains 'fromdate' (DATE) field"
        })
    from_date = body['fromdate']

    if "todate" not in body:
        return web.json_response({
            "error": "Body must contains 'todate' (DATE) field"
        })
    to_date = body['todate']

    response = []
    async for item in emc1sp_query_iter(database.openmetrics(request.app), request['username'], from_date, to_date):
        response.append(item)

    return web.json_response({
        'data': response
    })
