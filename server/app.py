import asyncio

from aiohttp import web

from server.utility import database
from server import endpoints


async def configure_app(config):
    app = web.Application()
    app['config'] = config
    app.cleanup_ctx.append(database.openmetrics_ctx)
    app.cleanup_ctx.append(database.local_storage_ctx)
    endpoints.add_to(app)
    return app


def start(host=None, port=None, *, config=None):
    web.run_app(
        host=host, port=port,
        app=asyncio.get_event_loop().run_until_complete(configure_app(config))
    )

