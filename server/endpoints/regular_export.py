import csv
import datetime
import io

from aiohttp import web
from server.utility import database, tokens, config

endpoints = web.RouteTableDef()


def regular_field_names():
    return {
        'reading_id': 'reading.id',
        'serial': 'meter.name',
        'mpan': 'meter.mpan',
        'location': 'meter.location',
        'date': 'reading.date',
        'import_total': '(reading.import_total_wh * 0.001) as import_total',
        'import_daily': 'reading.import_total',
        **{
            f'import{number}': f'reading.import{number}'
            for number in [
                '0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330', '0400', '0430',
                '0500', '0530', '0600', '0630', '0700', '0730', '0800', '0830', '0900', '0930',
                '1000', '1030', '1100', '1130', '1200', '1230', '1300', '1330', '1400', '1430',
                '1500', '1530', '1600', '1630', '1700', '1730', '1800', '1830', '1900', '1930',
                '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330'
            ]
        },
        'export_total': '(reading.export_total_wh * 0.001) as export_total',
        'export_daily': 'reading.export_total',
        **{
            f'export{number}': f'reading.export{number}'
            for number in [
                '0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330', '0400', '0430',
                '0500', '0530', '0600', '0630', '0700', '0730', '0800', '0830', '0900', '0930',
                '1000', '1030', '1100', '1130', '1200', '1230', '1300', '1330', '1400', '1430',
                '1500', '1530', '1600', '1630', '1700', '1730', '1800', '1830', '1900', '1930',
                '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330'
            ]
        },
        'extra_total': '(reading.export_total_wh_b * 0.001) as extra_total',
        'extra_daily': 'reading.export_total_b',
        **{
            f'extra{number}': f'reading.export{number}_b'
            for number in [
                '0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330', '0400', '0430',
                '0500', '0530', '0600', '0630', '0700', '0730', '0800', '0830', '0900', '0930',
                '1000', '1030', '1100', '1130', '1200', '1230', '1300', '1330', '1400', '1430',
                '1500', '1530', '1600', '1630', '1700', '1730', '1800', '1830', '1900', '1930',
                '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330'
            ]
        },
        'utilisation_total': '((reading.export_total_wh - reading.export_total_wh_b) * 0.001) as utilisation_total',
        'utilisation_daily': '(reading.export_total - reading.export_total_b) as utilisation_daily',
        **{
            f'utilisation{number}': f'(reading.export{number} - reading.export{number}_b) as utilisation{number}'
            for number in [
                '0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330', '0400', '0430',
                '0500', '0530', '0600', '0630', '0700', '0730', '0800', '0830', '0900', '0930',
                '1000', '1030', '1100', '1130', '1200', '1230', '1300', '1330', '1400', '1430',
                '1500', '1530', '1600', '1630', '1700', '1730', '1800', '1830', '1900', '1930',
                '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330'
            ]
        },
    }


@endpoints.post('/regular/csv_token')
async def regular_csv_token(request):
    request_data = await request.post()
    is_superuser = request_data.get('is_superuser', '0') in ('true', '1')

    if 'date_from' not in request_data:
        return web.json_response({'error': "missing 'date_from'"})
    if 'date_to' not in request_data:
        return web.json_response({'error': "missing 'date_to'"})
    if 'fields' not in request_data:
        return web.json_response({'error': "missing 'fields'"})
    if 'username' not in request_data:
        if not is_superuser:
            return web.json_response({'error': "missing 'username'"})

    valid_fields = regular_field_names()
    for field in request_data.getall('fields'):
        if field not in valid_fields:
            return web.json_response({'error': f"invalid name '{field}' in 'fields'"})

    token_data = {
        'date_from': request_data['date_from'],
        'date_to': request_data['date_to'],
        'fields': request_data.getall('fields'),
    }
    if 'username' in request_data and not is_superuser:
        token_data['username'] = request_data['username']
    if 'slugs' in request_data:
        token_data['slugs'] = request_data.getall('slugs')

    return web.json_response({
        'token': tokens.create_request_token(config(request.app, 'SECRET_KEY'), regular_csv, **token_data)
    })


async def regular_iter(app, username, slugs, fields, date_from, date_to):
    # Collect field names like in DB
    field_names = regular_field_names()
    select_names = [field_names[field] for field in fields]

    select_query = f"""
      SELECT /*<select_names>*/*/*</select_names>*/
      FROM readings_reading as reading
        INNER JOIN meters_meter as meter
          ON reading.meter_id = meter.id
      WHERE meter.id IN (
        SELECT profile_meters.meter_id
        FROM users_profile_meters as profile_meters
          INNER JOIN meters_meter as meter
          ON meter.id = profile_meters.meter_id
        WHERE (
            %(empty_slugs)s OR -- TRUE if no slugs 
            meter.name IN %(slugs)s
          )
          AND (
            %(all_users)s OR -- True if superuser
            profile_meters.profile_id = (SELECT auth_user.id FROM auth_user WHERE auth_user.username = %(username)s)
          )
      )
      AND %(date_from)s <= reading.date AND reading.date <= %(date_to)s
      ORDER BY reading.date
    ;
    """.replace('/*<select_names>*/*/*</select_names>*/', ','.join(select_names))

    async with database.openmetrics(app) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(select_query, {
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


async def regular_csv_iter(app, username, slugs, fields, date_from, date_to):
    csv_response = io.StringIO()
    writer = csv.DictWriter(csv_response, fields)
    writer.writeheader()

    regular_export_iterator = regular_iter(
        app=app,
        username=username,
        slugs=slugs,
        fields=fields,
        date_from=date_from,
        date_to=date_to,
    )
    async for response_row in regular_export_iterator:
        writer.writerow(response_row)
        if csv_response.tell() > 4096*4096:
            csv_response.seek(0)
            yield csv_response.read()
            csv_response.truncate()

    if csv_response.tell() > 0:
        csv_response.seek(0)
        yield csv_response.read()


@tokens.register_token_handler('regular/export')
async def regular_csv(request, request_args):
    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M')
    filename = f"{request_args.get('username', '_SUPERUSER')}_{current_time}_csvexport.csv"

    response = web.StreamResponse()
    response.headers['CONTENT-DISPOSITION'] = f'attachment; filename="{filename}"'
    await response.prepare(request)

    csv_iterator = regular_csv_iter(
        app=request.app,
        username=request_args.get('username'),
        slugs=request_args.get('slugs', []),
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
