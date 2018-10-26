import csv
import datetime
import io

from aiohttp import web
from server.utility import user_keys, tokens, database, config

endpoints = web.RouteTableDef()


def rename_args(request_args, name_table):
    for old_name, new_name in name_table.items():
        if old_name not in request_args:
            continue
        request_args[new_name] = request_args[old_name]
        del request_args[old_name]


@tokens.register_token_handler('readings/get')
async def readings_csv(request, request_args):
    rename_args(request_args, {
        'ir': 'import_reads',
        'er': 'export_reads',
        'fd': 'fromdate',
        'td': 'todate',
        'usr': 'username',
        'key': 'api_key'
    })

    remote_name = await user_keys.get_remote_username(
        local_db=database.local_storage(request.app),
        username=request_args['username'],
        api_key=request_args['api_key']
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

    select_names = []
    select_names.extend(both_names)

    if request_args['import_reads']:
        select_names.extend(import_names)

    if request_args['export_reads']:
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
        'fromdate': request_args["fromdate"],
        'todate': request_args['todate'],
        'username': remote_name
    }

    csv_response = io.StringIO()
    writer = csv.DictWriter(csv_response, select_names)
    writer.writerow({
        selected_column: (rename_dict.get(selected_column) or selected_column.replace(f'{reading_alias}.', ''))
        for selected_column in select_names
    })

    async with database.openmetrics(request.app) as conn:
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
    filename = f"{request_args['username']}_{current_time}_csvexport.csv"

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


@endpoints.post("/readings/csv_token")
@user_keys.access_headers
async def readings_csv_token(request):
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
        'token': tokens.create_request_token(config(request.app, 'SECRET_KEY'), readings_csv, **{
            'ir': include_imports,
            'er': include_exports,
            'fd': post_body['fromdate'],
            'td': post_body['todate'],
            'usr': request.headers["username"],
            'key': request.headers["api-key"],
        })
    })

