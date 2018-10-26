import argparse
import importlib
import os

from server import app


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--address', default='127.0.0.1:8080', help='host:port (e.g. "127.0.0.1:8080")')
    parser.add_argument('--config', help='config file (*.py)')
    args = parser.parse_args()
    host, port = args.address.split(':')
    if args.config is not None:
        return app.start(host, int(port), config=importlib.import_module(args.config))
    else:
        return app.start(host, int(port), config=os.environ)


if __name__ == '__main__':
    main()
