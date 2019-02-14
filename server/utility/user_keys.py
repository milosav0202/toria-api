import datetime

from aiohttp import web


async def get_remote_username(local_db, username, api_key):
    async with local_db.acquire() as conn:
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
                "name": str(username),
                "key": str(api_key)
            })
            async for remote_name, in cursor:
                return remote_name

        raise PermissionError("Wrong 'username' or 'api-key'")


def access_headers(async_handler):
    async def async_wrapper(request):
        if "username" not in request.headers or "api-key" not in request.headers:
            return web.json_response({
                "error": "You must provide 'username' and 'api-key' http headers"
            })

        try:
            request["username"] = await get_remote_username(
                local_db=request.app['local_db'],
                username=str(request.headers["username"]),
                api_key=str(request.headers["api-key"])
            )

        except PermissionError:
            return web.json_response({
                "error": "You provide wrong 'username' or 'api-key' headers"
            })

        return await async_handler(request)
    return async_wrapper


def access_logging(async_handler):
    async def async_wrapper(request):
        async with request.app["local_db"].acquire() as conn:
            async with conn.cursor() as cursor:
                response = await async_handler(request)

                request_log = """
                     INSERT INTO request_log (datetime, name) 
                         VALUES (%(datetime)s, %(name)s)
                 """
                await cursor.execute(request_log, {
                    "datetime": datetime.datetime.now(),
                    "name": request.headers["username"]
                })

                return response
    return async_wrapper
