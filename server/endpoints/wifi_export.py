import csv
import datetime
import io

from aiohttp import web
from server.utility import database, tokens, config

endpoints = web.RouteTableDef()


def wifi_field_names():
    return {
        'serial': 'meter.name',
        'mpan': 'meter.mpan',
        'location': 'meter.location',
        'datetime': 'wifi_reading.datetime',
        'status': 'wifi_reading.status',
        'power_output': 'wifi_reading.power_output',
        'solar_generation': 'wifi_reading.solar_generation',
        'solar_export': 'wifi_reading.solar_export',
        'power_import': 'wifi_reading.power_import'
    }


@endpoints.post('/spc/wifi_token')
async def wifi_csv_token(request):
    request_data = await request.post()

    valid_fields = wifi_field_names()
    for field in request_data.getall('fields'):
        if field not in valid_fields:
            return web.json_response({'error': f"invalid name '{field}' in 'fields'"})

    token_data = {
        'date_from': request_data['date_from'],
        'date_to': request_data['date_to'],
        'fields': request_data.getall('fields'),
    }

    return web.json_response({
        'token': tokens.create_request_token(config(request.app, 'SECRET_KEY'), wifi_csv, **token_data)
    })


async def wifi_iter(app, username, slugs, meter_type, fields, date_from, date_to):
    # Collect field names like in DB
    field_names = wifi_field_names()
    select_names = [field_names[field] for field in fields]

    select_query = f"""
      SELECT /*<select_names>*/*/*</select_names>*/
      FROM readings_wifireading as wifi_reading
        INNER JOIN meters_meter as meter
          ON wifi_reading.meter_id = meter.id
      WHERE meter.id IN (
        SELECT profile_meters.meter_id
        FROM users_profile_meters as profile_meters
          INNER JOIN meters_meter as meter
          ON meter.id = profile_meters.meter_id
        WHERE meter.type = %(meter_type)s
          AND (
            %(empty_slugs)s OR -- TRUE if no slugs 
            meter.name IN %(slugs)s
          )
          AND (
            %(all_users)s OR -- True if superuser
            profile_meters.profile_id = (SELECT auth_user.id FROM auth_user WHERE auth_user.username = %(username)s)
          )
      )
      AND %(date_from)s <= wifi_reading.datetime AND wifi_reading.datetime <= %(date_to)s
      ORDER BY wifi_reading.datetime
    ;
    """.replace('/*<select_names>*/*/*</select_names>*/', ','.join(select_names))

    async with database.openmetrics(app) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(select_query, {
                'meter_type': meter_type,
                'empty_slugs': not slugs,
                # avoid sql syntax error: 'meter.name IN ()'
                #                                     ^^^^^
                'slugs': slugs if slugs else ('',),
                'all_users': username is None,
                'username': '' if username is None else username,
                'date_from': date_from,
                'date_to': date_to,
            })
            async for row in cursor:
                response_item = dict(zip(fields, row))
                if 'date' in response_item:
                    response_item['date'] = response_item['date'].strftime("%Y-%m-%d")
                yield response_item


async def wifi_csv_iter(app, username, slugs, meter_type, fields, date_from, date_to):
    csv_response = io.StringIO()
    writer = csv.DictWriter(csv_response, fields)
    writer.writeheader()

    wifi_export_iterator = wifi_iter(
        app=app,
        username=username,
        slugs=slugs,
        meter_type=meter_type,
        fields=fields,
        date_from=date_from,
        date_to=date_to,
    )
    async for response_row in wifi_export_iterator:
        writer.writerow(response_row)
        if csv_response.tell() > 4096*4096:
            csv_response.seek(0)
            yield csv_response.read()
            csv_response.truncate()

    if csv_response.tell() > 0:
        csv_response.seek(0)
        yield csv_response.read()


@tokens.register_token_handler('wifi/export')
async def wifi_csv(request, request_args):
    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M')
    filename = f"{request_args.get('username', '_SUPERUSER')}_{current_time}_csvexport.csv"

    response = web.StreamResponse()
    response.headers['CONTENT-DISPOSITION'] = f'attachment; filename="{filename}"'
    await response.prepare(request)

    csv_iterator = wifi_csv_iter(
        app=request.app,
        username=request_args.get('username'),
        slugs=request_args.get('slugs', []),
        meter_type='EM',
        fields=request_args['fields'],
        date_from=request_args['date_from'],
        date_to=request_args['date_to'],
    )

    try:
        async for file_part in csv_iterator:
            await response.write(file_part.encode())
    finally:
        await response.write_eof()
    return response
