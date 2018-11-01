from server.utility.exporter import *

endpoints = web.RouteTableDef()


class SPCExport(DefaultExporter):
    def __init__(self, request, request_args):
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

    def fields(self):
        fields = [
            'serial',
            'reading_id',
            'mpan',
            'location',
            'date',
            'domestic_load_total',
            'domestic_load_daily',
            'grid_energy_total',
            'grid_energy_daily',
            'utilised_total',
            'utilised_daily',
            'charge_total',
            'charge_daily',
            'generation_total',
            'generation_daily',
            'grid_export_total',
            'grid_export_daily'
        ]

        for time in self.times:
            fields.append('domestic_load'+time)
            fields.append('grid_energy' + time)
            fields.append('utilised' + time)
            fields.append('charge' + time)
            fields.append('generation' + time)
            fields.append('grid_export' + time)
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
        fields['mpan'] = meter.meter['mpan']
        fields['location'] = meter.meter['location']
        fields['date'] = reading.reading['date']
        fields['domestic_load_total'] = reading.reading['domestic_load_total'],
        fields['domestic_load_daily'] = reading.reading['domestic_load_daily']
        fields['utilised_total'] = reading.reading['utilised_total'],
        fields['utilised_daily'] = reading.reading['utilised_daily'],
        fields['grid_export_total'] = reading.reading['grid_export_total'],
        fields['grid_export_daily'] = reading.reading['grid_export_daily'],
        fields['grid_energy_total'] = reading.reading['grid_energy_total'],
        fields['charge_total'] = reading.reading['charge_total'],
        fields['charge_daily'] = reading.reading['charge_daily'],
        fields['generation_total'] = reading.reading['generation_total'],
        fields['generation_daily'] = reading.reading['generation_daily']

        for number in self.times:
            fields[f'domestic_load{number}'] = reading.reading[f'domestic_load{number}']
            fields[f'grid_energy{number}'] = reading.reading[f'grid_energy{number}']
            fields[f'utilised{number}'] = reading.reading[f'utilised{number}']
            fields[f'charge{number}'] = reading.reading[f'charge{number}']
            fields[f'generation{number}'] = reading.reading[f'generation{number}']
            fields[f'grid_export{number}'] = reading.reading[f'grid_export{number}']

        return fields[field]

    async def get_readings(self, meter):
        field_names = self.reading_fields().values()

        query = """SELECT /*<select_names>*/*/*</select_names>*/ FROM readings_reading as reading
                    INNER JOIN readings_spcreading as spc_reading
                      ON reading.meter_id = spc_reading.meter_id AND reading.date = spc_reading.date
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
            'domestic_load_total': 'reading.export_total_wh',
            'domestic_load_daily': 'reading.export_total',
            **{
                f'domestic_load{number}': f'reading.export{number}'
                for number in self.times
            },
            **{
                f'grid_energy{number}': f'reading.export{number}_b'
                for number in self.times
            },
            'utilised_total': 'reading.export_total_wh_b',
            'utilised_daily': 'reading.import_total',
            **{
                f'utilised{number}': f'reading.import{number}'
                for number in self.times
            },
            'grid_export_total': 'reading.import_total_wh',
            'grid_energy_total': 'spc_reading.grid_energy_wh',
            'grid_energy_daily': 'spc_reading.grid_energy',
            'charge_total': 'spc_reading.charge_wh',
            'charge_daily': 'spc_reading.export_total',
            **{
                f'charge{number}': f'spc_reading.export{number}'
                for number in self.times
            },
            'generation_total': 'spc_reading.generation_wh',
            'generation_daily': 'spc_reading.export_total_b',
            **{
                f'generation{number}': f'spc_reading.export{number}_b'
                for number in self.times
            },
            'grid_export_daily': 'spc_reading.export_total',
            **{
                f'grid_export{number}': f'spc_reading.import{number}'
                for number in self.times
            },
        }


class Reading:
    def __init__(self, reading, meter):
        self.reading = reading
        self.meter = meter


@endpoints.post('/spc/csv_token')
async def spc_csv_token(request, request_args):
    spc = SPCExport(request, request_args)
    return await spc.token(request, spc_csv)


@tokens.register_token_handler('spc/export')
async def spc_csv(request, request_args):
    spc = SPCExport(request, request_args)
    filename = f"{spc.get_username()}_{spc.current_time}_csvexport.csv"

    response = web.StreamResponse()
    response.headers['CONTENT-DISPOSITION'] = f'attachment; filename="{filename}"'
    await response.prepare(request)

    try:
        for file_part in await spc.export(request, request_args):
            await response.write(file_part.encode())
    finally:
        await response.write_eof()
    return response
