import datetime, io, csv
from aiohttp import web

from server.utility import database, tokens, config


def current_time():
    return datetime.datetime.now().strftime('%Y%m%d%H%M')


class DefaultExporter:
    def __init__(self):
        self.current_time = current_time()

    def meter_type(self):
        raise NotImplementedError

    async def process_meter(self, meter):
        raise NotImplementedError

    # get request username
    def get_username(self):
        return self.request_args.get('username', '_SUPERUSER')

    # get request app
    def get_app(self):
        return self.request.app

    # get request slugs
    def get_slugs(self):
        return self.request_args.get('slugs', [])

    # get request fields
    def get_fields(self):
        return self.request_args.get('fields')

    # get request date_from
    def get_date_from(self):
        return self.request_args.get('date_from')

    # get request date_to
    def get_date_to(self):
        return self.request_args.get('date_to')

    # list of fields
    def fields(self):
        raise NotImplementedError

    async def export(self, request, request_args):
        self.request = request
        self.request_args = request_args

        if self.get_username() == "_SUPERUSER":
            meters = await self.get_meters(is_superuser=True)
        else:
            meters = await self.get_meters()

        rows = list()
        async for meter_elem in meters:
            meter = Meter(meter_elem)
            elements = await self.process_meter(meter)
            for element in elements:
                rows.append(element)

        return self.make_csv(rows)

    def make_csv(self, rows):
        csv_response = io.StringIO()
        writer = csv.DictWriter(csv_response, self.get_fields())
        writer.writeheader()

        for row in rows:
            writer.writerow(row)
            if csv_response.tell() > 4096 * 4096:
                csv_response.seek(0)
                yield csv_response.read()
                csv_response.truncate()

        if csv_response.tell() > 0:
            csv_response.seek(0)
            yield csv_response.read()

    async def get_meters(self, is_superuser=False):
        if not is_superuser:
            profile_id = await self.get_profile_id(self.get_username())
        else:
            profile_id = 0
        meter_type = self.meter_type()
        return self.get_meters_list(profile_id, meter_type=meter_type, is_superuser=is_superuser)

    async def get_meters_list(self, profile_id, meter_type, is_superuser=False):
        slugs = self.get_slugs()

        field_names = self.meter_fields().values()

        query = """SELECT /*<select_names>*/*/*</select_names>*/ FROM meters_meter as meter
         INNER JOIN users_profile_meters as profile_meters 
         ON meter.id = profile_meters.meter_id
         WHERE (%(empty_slugs)s OR meter.name IN (%(slugs)s)) 
         AND %(all_users)s OR (profile_id = %(profile_id)s)
         AND (meter.type = %(type)s);""".replace('/*<select_names>*/*/*</select_names>*/', ','.join(field_names))

        async with database.openmetrics(self.request.app) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(query, {
                    'profile_id': profile_id,
                    'all_users': is_superuser,
                    'empty_slugs': not slugs,
                    'slugs': ",".join(slugs) if slugs else ('',),
                    'type': meter_type
                })
                async for row in cursor:
                    response_item = dict(zip(self.meter_fields(), row))
                    yield response_item

    def meter_fields(self):
        return {
            'id': 'meter.id',
            'name': 'meter.name',
            'mpan': 'meter.mpan',
            'location': 'meter.location',
            'type': 'meter.type',
            'billable': 'meter.billable',
            'paid_until': 'meter.paid_until'
        }

    async def get_profile_id(self, username):
        query = """SELECT auth_user.id FROM auth_user WHERE auth_user.username = %(username)s;"""

        async with database.openmetrics(self.request.app) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(query, {
                    'username': username,
                })
                profile_id = await cursor.fetchone()
                return profile_id[0]

    # get token
    async def token(self, request, request_function):
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
        valid_fields = self.fields()
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
            'token': tokens.create_request_token(config(request.app, 'SECRET_KEY'), request_function, **token_data)
        })


class Meter:
    def __init__(self, meter):
        self.meter = meter