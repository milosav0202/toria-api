from aiohttp import web
from server.utility import tokens, config

endpoints = web.RouteTableDef()


@endpoints.get("/download")
async def download(request):
    if 'token' not in request.query:
        return web.json_response({
            'error': "You must provide 'token' field"
        })

    try:
        secret_key = config(request.app, 'SECRET_KEY')
        target_function, request_args = tokens.parse_request_token(secret_key, request.query['token'])
        return await target_function(request, request_args)
    except (ValueError, KeyError):
        return web.json_response({
            'error': "Invalid token"
        })
