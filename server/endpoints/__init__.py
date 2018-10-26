from aiohttp import web


def add_to(app: web.Application):
    from .download import endpoints as download
    app.add_routes(download)

    from .emc1sp import endpoints as emc1sp
    app.add_routes(emc1sp)

    from .readings import endpoints as readings
    app.add_routes(readings)

    from .spc_export import endpoints as spc_export
    app.add_routes(spc_export)

    from .wifi_export import endpoints as wifi_export
    app.add_routes(wifi_export)

    from .total_readings import endpoints as total_readings
    app.add_routes(total_readings)
