"""Application entry point.

TODO: Load configuration, create the server, and start it.
Keep networking and event-loop behavior in server.py.
"""

def main() -> None:
    """Start the server."""
    from mini_redis.server import run
    run()


if __name__ == "__main__":
    main()