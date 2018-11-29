from server.utility.exporter import *

endpoints = web.RouteTableDef()


class RegularExport(DefaultExporter):
    def __init__(self, request, request_args=None):
        super().__init__()
        self.request = request
        self.request_args = request_args

    times = [
        '0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330',
        '0400', '0430', '0500', '0530', '0600', '0630', '0700', '0730',
        '0800', '0830', '0900', '0930', '1000', '1030', '1100', '1130',
        '1200', '1230', '1300', '1330', '1400', '1430', '1500', '1530',
        '1600', '1630', '1700', '1730', '1800', '1830', '1900', '1930',
        '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330'
    ]

    times_utilisation = [
        '0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330',
        '0400', '0430', '0500', '0530', '0600', '0630', '0700', '0730',
        '0800', '0830', '0900', '0930', '1000', '1030', '1100', '1130',
        '1200', '1230', '1300', '1330', '1400', '1430', '1500', '1530',
        '1600', '1630', '1700', '1730', '1800', '1830', '1900', '1930',
        '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330',
    ]

    def fields(self):
        fields = [
            'serial',
            'reading_id',
            'mpan',
            'location',
            'date',
            'import_total',
            'import_daily',
            'extra_total',
            'extra_daily',
            'utilisation_total',
            'utilisation_daily',
            'export_total',
            'export_daily',
        ]

        for time in self.times:
            fields.append('export'+time+"_b")
            fields.append('import' + time)
            fields.append('export' + time)

        for time in self.times_utilisation:
            fields.append('utilisation' + time)

        return fields

    def meter_type(self):
        return "SP"

    async def process_meter(self, meter):
        elements = list()
        readings = self.get_readings(meter=meter)
        async for reading_elem in readings:
            element = dict()
            reading = Reading(reading_elem, meter)
            for field_name in self.get_fields():
                element[field_name] = await self.field_handler(field_name, meter, reading)
            elements.append(element)
        return elements

    async def field_handler(self, field, meter, reading):
        fields = dict()

        fields['serial'] = meter.meter['name']
        fields['reading_id'] = reading.reading['reading_id']
        fields['mpan'] = meter.meter['mpan'] #TODO incorrect mpan
        fields['location'] = meter.meter['location']
        fields['date'] = reading.reading['date']
        fields['import_total'] = ((reading.reading['import_total'] or 0) * 0.001)[0],
        fields['import_daily'] = reading.reading['import_daily']
        fields['export_total'] = ((reading.reading['export_total'] or 0) * 0.001)[0],
        fields['export_daily'] = reading.reading['export_daily'],
        fields['extra_total'] = ((reading.reading['extra_total'] or 0) * 0.001)[0],
        fields['extra_daily'] = reading.reading['extra_daily'],
        fields['utilisation_total'] = abs(((reading.reading['extra_total'] - reading.reading['import_total']) * 0.001)[0]),
        fields['utilisation_daily'] = (reading.reading['extra_daily'] - reading.reading['import_daily']),

        for number in self.times:
            fields[f'import{number}'] = reading.reading[f'import{number}']
            fields[f'export{number}'] = reading.reading[f'export{number}']
            fields[f'export{number}_b'] = reading.reading[f'export{number}_b']

        for number in self.times_utilisation:
            fields[f'utilisation{number}'] = (reading.reading[f'export{number}_b'] - reading.reading[f'export{number}'])

        return fields[field]

    async def get_readings(self, meter):
        field_names = self.reading_fields().values()

        query = """SELECT /*<select_names>*/*/*</select_names>*/ FROM readings_reading as reading
                    WHERE reading.meter_id=%(meter_id)s 
                    AND %(date_from)s <= reading.date AND reading.date <= %(date_to)s
                    ORDER BY reading.date
              ;""".replace('/*<select_names>*/*/*</select_names>*/', ','.join(field_names))

        async with database.openmetrics(self.request.app) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(query, {
                    'meter_id': meter.meter['id'],
                    'date_from': self.get_date_from(),
                    'date_to': self.get_date_to()
                })
                async for row in cursor:
                    response_item = dict(zip(self.reading_fields(), row))
                    if 'date' in response_item:
                        response_item['date'] = response_item['date'].strftime("%Y-%m-%d")
                    yield response_item

    def reading_fields(self):
        return {
            'reading_id': 'reading.id',
            'date': 'reading.date',
            'import_total': 'reading.import_total_wh',
            'import_daily': 'reading.import_total',
            'extra_total': 'reading.export_total_wh_b',
            'extra_daily': 'reading.export_total_b',
            'export_total': 'reading.export_total_wh',
            'export_daily': 'reading.export_total',
            **{
                f'import{number}': f'reading.import{number}'
                for number in self.times
            },
            **{
                f'export{number}_b': f'reading.export{number}_b'
                for number in self.times
            },
            **{
                f'export{number}': f'reading.export{number}'
                for number in self.times
            },
        }


class Reading:
    def __init__(self, reading, meter):
        self.reading = reading
        self.meter = meter


@endpoints.post('/regular/csv_token')
async def regular_csv_token(request):
    regular = RegularExport(request)
    return await regular.token(request, regular_csv)


@tokens.register_token_handler('spc/export')
async def regular_csv(request, request_args):
    regular = RegularExport(request, request_args)
    filename = f"{regular.get_username()}_{regular.current_time}_csvexport.csv"

    response = web.StreamResponse()
    response.headers['CONTENT-DISPOSITION'] = f'attachment; filename="{filename}"'
    await response.prepare(request)

    try:
        for file_part in await regular.export(request, request_args):
            await response.write(file_part.encode())
    finally:
        await response.write_eof()
    return response

