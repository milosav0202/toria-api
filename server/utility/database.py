import aiopg
import aiohttp.web
from server.utility import config


def __create_database_context(resource_name, config_key):
    async def cleanup_context(app: aiohttp.web.Application):
        # Create postgres connection pool and append it to application
        async with aiopg.create_pool(config(app, config_key)) as connection_pool:
            app[resource_name] = connection_pool
            yield  # <!> Do not remove this yield

    return cleanup_context


__OPENMETRICS_DB_POOL = 'OPENMETRICS_DB_POOL'
openmetrics_ctx = __create_database_context(__OPENMETRICS_DB_POOL, 'OPENMETRICS_DSN')


def openmetrics(app: aiohttp.web.Application):
    assert __OPENMETRICS_DB_POOL in app
    connection_pool: aiopg.Pool = app[__OPENMETRICS_DB_POOL]
    return connection_pool.acquire()


__LOCAL_STORAGE_DB_POOL = 'LOCAL_STORAGE_DB_POOL'
local_storage_ctx = __create_database_context(__LOCAL_STORAGE_DB_POOL, 'LOCAL_STORAGE_DSN')


def local_storage(app: aiohttp.web.Application) -> aiopg.Connection:
    assert __LOCAL_STORAGE_DB_POOL in app
    connection_pool: aiopg.Pool = app[__LOCAL_STORAGE_DB_POOL]
    return connection_pool.acquire()
