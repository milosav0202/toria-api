from aiohttp import web
from server.utility import user_keys, database

endpoints = web.RouteTableDef()


@endpoints.post("/total_readings/json")
@user_keys.access_headers
@user_keys.access_logging
async def total_readings_json(request):
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
    async with database.openmetrics(request.app) as conn:
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
