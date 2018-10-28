import csv
import datetime
import io

from aiohttp import web
from server.utility import database, tokens, config

endpoints = web.RouteTableDef()


def spc_field_names():
    return {
        'serial': 'meter.name',
        'reading_id': 'reading.id',
        'mpan': 'meter.mpan',
        'location': 'meter.location',
        'date': 'reading.date',
        'domestic_load_total': 'reading.export_total_wh',
        'domestic_load_daily': 'reading.export_total',
        **{
            f'domestic_load{number}': f'reading.export{number}'
            for number in [
                '0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330', '0400', '0430',
                '0500', '0530', '0600', '0630', '0700', '0730', '0800', '0830', '0900', '0930',
                '1000', '1030', '1100', '1130', '1200', '1230', '1300', '1330', '1400', '1430',
                '1500', '1530', '1600', '1630', '1700', '1730', '1800', '1830', '1900', '1930',
                '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330'
            ]
        },
        **{
            f'grid_energy{number}': f'reading.export{number}_b'
            for number in [
                '0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330', '0400', '0430',
                '0500', '0530', '0600', '0630', '0700', '0730', '0800', '0830', '0900', '0930',
                '1000', '1030', '1100', '1130', '1200', '1230', '1300', '1330', '1400', '1430',
                '1500', '1530', '1600', '1630', '1700', '1730', '1800', '1830', '1900', '1930',
                '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330'
            ]
        },
        'utilised_total': 'reading.export_total_wh_b',
        'utilised_daily': 'reading.import_total',
        **{
            f'utilised{number}': f'reading.import{number}'
            for number in [
                '0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330', '0400', '0430',
                '0500', '0530', '0600', '0630', '0700', '0730', '0800', '0830', '0900', '0930',
                '1000', '1030', '1100', '1130', '1200', '1230', '1300', '1330', '1400', '1430',
                '1500', '1530', '1600', '1630', '1700', '1730', '1800', '1830', '1900', '1930',
                '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330'
            ]
        },
        'grid_export_total': 'reading.import_total_wh',

        # ------------------------------------------------------------------------------------
        # spc_readings
        'grid_energy_total': 'spc_reading.grid_energy_wh',
        'grid_energy_daily': 'spc_reading.grid_energy',
        'charge_total': 'spc_reading.charge_wh',
        'charge_daily': 'spc_reading.export_total',
        **{
            f'charge{number}': f'spc_reading.export{number}'
            for number in [
                '0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330', '0400', '0430',
                '0500', '0530', '0600', '0630', '0700', '0730', '0800', '0830', '0900', '0930',
                '1000', '1030', '1100', '1130', '1200', '1230', '1300', '1330', '1400', '1430',
                '1500', '1530', '1600', '1630', '1700', '1730', '1800', '1830', '1900', '1930',
                '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330'
            ]
        },
        'generation_total': 'spc_reading.generation_wh',
        'generation_daily': 'spc_reading.export_total_b',
        **{
            f'generation{number}': f'spc_reading.export{number}_b'
            for number in [
                '0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330', '0400', '0430',
                '0500', '0530', '0600', '0630', '0700', '0730', '0800', '0830', '0900', '0930',
                '1000', '1030', '1100', '1130', '1200', '1230', '1300', '1330', '1400', '1430',
                '1500', '1530', '1600', '1630', '1700', '1730', '1800', '1830', '1900', '1930',
                '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330'
            ]
        },
        'grid_export_daily': 'spc_reading.export_total',
        **{
            f'grid_export{number}': f'spc_reading.import{number}'
            for number in [
                '0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330', '0400', '0430',
                '0500', '0530', '0600', '0630', '0700', '0730', '0800', '0830', '0900', '0930',
                '1000', '1030', '1100', '1130', '1200', '1230', '1300', '1330', '1400', '1430',
                '1500', '1530', '1600', '1630', '1700', '1730', '1800', '1830', '1900', '1930',
                '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330'
            ]
        },
    }


@endpoints.post('/spc/csv_token')
async def spc_csv_token(request):
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

    valid_fields = spc_field_names()
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
        'token': tokens.create_request_token(config(request.app, 'SECRET_KEY'), spc_csv, **token_data)
    })


async def spc_iter(app, username, slugs, meter_type, fields, date_from, date_to):
    # Collect field names like in DB
    field_names = spc_field_names()
    select_names = [field_names[field] for field in fields]

    select_query = f"""
      SELECT /*<select_names>*/*/*</select_names>*/
      FROM readings_reading as reading
        INNER JOIN readings_spcreading as spc_reading
          ON reading.meter_id = spc_reading.meter_id AND reading.date = spc_reading.date
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


async def spc_csv_iter(app, username, slugs, meter_type, fields, date_from, date_to):
    csv_response = io.StringIO()
    writer = csv.DictWriter(csv_response, fields)
    writer.writeheader()

    spc_export_iterator = spc_iter(
        app=app,
        username=username,
        slugs=slugs,
        meter_type=meter_type,
        fields=fields,
        date_from=date_from,
        date_to=date_to,
    )
    async for response_row in spc_export_iterator:
        writer.writerow(response_row)
        if csv_response.tell() > 4096*4096:
            csv_response.seek(0)
            yield csv_response.read()
            csv_response.truncate()

    if csv_response.tell() > 0:
        csv_response.seek(0)
        yield csv_response.read()


@tokens.register_token_handler('spc/export')
async def spc_csv(request, request_args):
    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M')
    filename = f"{request_args.get('username', '_SUPERUSER')}_{current_time}_csvexport.csv"

    response = web.StreamResponse()
    response.headers['CONTENT-DISPOSITION'] = f'attachment; filename="{filename}"'
    await response.prepare(request)

    csv_iterator = spc_csv_iter(
        app=request.app,
        username=request_args.get('username'),
        slugs=request_args.get('slugs', []),
        meter_type='SP',
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
