"""Application entry point: read command-line options and start the server.

Networking and event-loop behavior live in server.py.
"""

import argparse
from pathlib import Path

from mini_redis.config import DEFAULT_CONFIG, AOFConfig, ServerConfig


def parse_args(argv: list[str] | None = None) -> ServerConfig:
    """Turn command-line options into a ServerConfig.

    argv=None reads sys.argv. Defaults come from DEFAULT_CONFIG so config.py
    stays the only place they are defined.
    """
    parser = argparse.ArgumentParser(
        prog="mini_redis",
        description="A Redis-compatible server built from scratch.",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_CONFIG.host,
        help=f"address to listen on (default: {DEFAULT_CONFIG.host})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_CONFIG.port,
        help=f"port to listen on (default: {DEFAULT_CONFIG.port})",
    )
    parser.add_argument(
        "--aof",
        action="store_true",
        help="turn on the append-only file",
    )
    parser.add_argument(
        "--aof-path",
        type=Path,
        default=DEFAULT_CONFIG.aof.path,
        help=f"append-only file location (default: {DEFAULT_CONFIG.aof.path})",
    )
    args = parser.parse_args(argv)

    # The config classes are frozen and validate themselves, so build new ones
    # and turn a validation error into a short usage message (exit code 2).
    try:
        return ServerConfig(
            host=args.host,
            port=args.port,
            aof=AOFConfig(enabled=args.aof, path=args.aof_path),
        )
    except ValueError as err:
        parser.error(str(err))


def main(argv: list[str] | None = None) -> None:
    """Start the server."""
    config = parse_args(argv)

    from mini_redis.server import run
    run(config)


if __name__ == "__main__":
    main()
