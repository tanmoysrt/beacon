import argparse

import uvicorn

from beacon.app import app, configure


def main() -> None:
    """Parse arguments, configure the application, and start the server."""
    parser = argparse.ArgumentParser(description="Beacon coordination service")
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Address to bind to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to listen on (default: 8000)",
    )
    parser.add_argument(
        "--database",
        type=str,
        default="beacon.db",
        help="Path to SQLite database (default: beacon.db)",
    )
    arguments = parser.parse_args()

    configure(arguments.database)

    uvicorn.run(
        app,
        host=arguments.host,
        port=arguments.port,
        workers=1,
    )


if __name__ == "__main__":
    main()
