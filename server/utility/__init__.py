def config(app, name):
    if hasattr(app['config'], name):
        return getattr(app['config'], name)
    try:
        return app['config'][name]
    except (TypeError, KeyError):
        raise AttributeError(f'"{name}" is not configured')
