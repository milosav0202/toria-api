import asyncio
import datetime
import aiopg
from aiohttp import web

import config

routes = web.RouteTableDef()


@routes.post("/")
async def index(request):
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
    parameters = {
        'username': request["username"],
        'date': post_body['date'],
        'meters': tuple(post_body.getall('meters'))
    }

    response = []
    async with request.app["remote_db"].acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(select_query, parameters)
            async for row in cursor:
                response.append({
                    "id": row[0],
                    "name": row[1],
                    "date": row[2].strftime("%Y-%m-%d"),
                    "import_total_wh": row[3],
                    "import_total": row[4]
                })

    return web.json_response({
        "data": response
    })


@web.middleware
async def api_key_middleware(request, handler):
    if "username" not in request.headers or "api-key" not in request.headers:
        return web.json_response({
            "error": "You must provide 'username' and 'api-key' http headers"
        })

    async with request.app["local_db"].acquire() as conn:
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
                "name": str(request.headers["username"]),
                "key": str(request.headers["api-key"])
            })
            for remote_name, in cursor:
                request["username"] = remote_name

    if "username" not in request:
        return web.json_response({
            "error": "You provide wrong 'username' or 'api-key' headers"
        })

    async with request.app["local_db"].acquire() as conn:
        async with conn.cursor() as cursor:
            request_log = """
                INSERT INTO request_log (datetime, name) 
                    VALUES (%(datetime)s, %(name)s)
            """
            await cursor.execute(request_log, {
                "datetime": datetime.datetime.now(),
                "name": request.headers["username"]
            })

    return await handler(request)


async def pg_pool(app):
    async with aiopg.create_pool(config.LOCAL_DATABASE) as local_pool:
        app["local_db"] = local_pool
        async with aiopg.create_pool(config.REMOTE_DATABASE) as remote_pool:
            app["remote_db"] = remote_pool
            yield  # <!> Do not remove this yield.


async def api_app():
    app = web.Application(
        middlewares=[
            api_key_middleware
        ]
    )
    app.cleanup_ctx.append(pg_pool)
    app.add_routes(routes)
    return app


if __name__ == '__main__':
    web.run_app(asyncio.get_event_loop().run_until_complete(api_app()))
