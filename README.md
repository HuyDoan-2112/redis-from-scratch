# Mini Redis

A Redis-compatible server built from scratch in Python as a learning project.

This is a folder scaffold. The Python files contain responsibility notes only;
there is no server implementation yet. No third-party runtime packages are required.

## Project structure

```text
mini-redis/
|-- README.md
|-- pyproject.toml
|-- .gitignore
|-- mini_redis/
|   |-- __init__.py
|   |-- __main__.py
|   |-- config.py
|   |-- server.py
|   `-- connection.py
`-- tests/
    |-- __init__.py
    `-- test_server.py
```

## File responsibilities

| File | Responsibility |
| --- | --- |
| `mini_redis/__main__.py` | Load configuration, create the server, and start it. |
| `mini_redis/config.py` | Define the address, port, and connection or buffer limits. |
| `mini_redis/server.py` | Own the listener, selector, event loop, client registry, and shutdown. |
| `mini_redis/connection.py` | Track one client's socket, input buffer, output buffer, and state. |
| `tests/test_server.py` | Check observable server behavior using real TCP connections. |
| `pyproject.toml` | Record project metadata and the supported Python version. |

## First milestone

1. Define the bind address as `127.0.0.1` and the default port as `6379`.
2. Create a TCP listener and handle startup errors.
3. Add a selector and accept non-blocking client connections.
4. Keep a separate connection record for each client.
5. Handle disconnects and clean shutdown.
6. Check that two clients can connect without blocking each other.

Work through `config.py`, `server.py`, `__main__.py`, then `connection.py`.

The intended entry command, run from this project folder, is `python -m mini_redis`.
At present it exits immediately because the entry point is a placeholder.

Use Python's built-in `unittest` for initial tests. Run them with
`python -m unittest discover -s tests -v`. There are no implemented tests yet.

## Add later

- `protocol.py`: parse RESP requests and encode replies.
- `commands.py`: validate and execute supported commands.
- `storage.py`: own keys, values, and expiration metadata.

Add these when you reach RESP and command handling. The first supported command
can be `PING`, followed by `SET` and `GET`.
