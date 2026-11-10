"""Application entry point: read command-line options and start the server.

Networking and event-loop behavior live in server.py.
"""
import sys
import argparse
from pathlib import Path

from mini_redis.config import DEFAULT_CONFIG, FSYNC_POLICIES, AOFConfig, RDBConfig, ServerConfig

def parse_save_points(text: str) -> tuple[tuple[int, int], ...]:
    """'60 1000 300 10' -> ((60, 1000), (300, 10))"""
    parts = text.split()
    if len(parts) % 2:
        raise argparse.ArgumentTypeError("needs pairs of <seconds> <changes>")
    numbers = []
    for part in parts:
        try:
            numbers.append(int(part))
        except ValueError:
            raise argparse.ArgumentTypeError(f"not an integer: {part!r}") from None
    return tuple(zip(numbers[::2], numbers[1::2]))

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
    parser.add_argument(
        "--appendfsync",
        choices=FSYNC_POLICIES,
        default=DEFAULT_CONFIG.aof.appendfsync,
        help=f"when to fsync the AOF (default: {DEFAULT_CONFIG.aof.appendfsync})",
    )
    parser.add_argument(
            "--rdb-path",
            type=Path,
            default=DEFAULT_CONFIG.rdb.path,
            help=f"rdb file location (default: {DEFAULT_CONFIG.rdb.path})",
        )
    parser.add_argument(
        "--save",
        type=parse_save_points,
        default=DEFAULT_CONFIG.rdb.save_points,
        metavar='"SECONDS CHANGES ..."',
        help="save automatically after SECONDS if at least CHANGES writes happened (default: off)",
    )
    args = parser.parse_args(argv)

    # The config classes are frozen and validate themselves, so build new ones
    # and turn a validation error into a short usage message (exit code 2).
    try:
        return ServerConfig(
            host=args.host,
            port=args.port,
            aof=AOFConfig(enabled=args.aof, path=args.aof_path, appendfsync=args.appendfsync),
            rdb=RDBConfig(path=args.rdb_path, save_points=args.save),
        )
    except ValueError as err:
        parser.error(str(err))


def main(argv: list[str] | None = None) -> None:
    """Start the server."""
    config = parse_args(argv)

    from mini_redis.persistence.rdb import RDBError
    from mini_redis.server import run
    try:
        run(config)
    except RDBError as err:
        print(f"Can't load the RDB file {config.rdb.path}: {err}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
