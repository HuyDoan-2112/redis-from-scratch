# Mini Redis

A Redis-compatible server written from scratch in Python, with no third-party
packages. It speaks RESP2, so `redis-cli` and other Redis clients can connect to
it.

I am building it to learn how Redis works inside: the protocol, persistence,
the event loop, expiry, and data structures. Later phases add vector search
(HNSW), probabilistic data structures, replication, and a C++ parser. See the
[architecture and roadmap](docs/architecture.md).

## Status

Working now:

- RESP2 parser and encoder (arrays, bulk strings, simple strings, integers,
  errors, null)
- Binary-safe keys and values: any bytes, not only UTF-8 text
- Pipelining: several commands in one packet get their replies in order
- Single-threaded event loop (`selectors`, epoll on Linux): one thread serves
  every client, so commands need no locks
- Partial input and output: a command split across several packets waits in
  the client's input buffer, and a reply the socket cannot take at once waits
  in its output buffer, so a slow client never blocks the others
- Command table with arity checks and a `write` flag
- Append-only file (AOF): every write command is logged and replayed on
  startup, with an fsync once per second
- Crash recovery: a half-written last command is cut from the AOF on startup,
  so the server can always restart

Not yet:

- Inline commands (`PING` typed in telnet)
- Expiry, `DEL`, `EXISTS`, `INCR`, and the other commands in the roadmap

## Supported commands

| Command | Reply |
| --- | --- |
| `PING [message]` | `PONG`, or the message |
| `SET key value` | `OK` |
| `GET key` | The value, or nil if the key does not exist |
| `HSET key field value` | 1 if the field is new, 0 if it was updated |
| `HGET key field` | The value, or nil |

Using a key with the wrong type (for example `GET` on a hash) returns an error.

## Quick start

Requires Python 3.11 or newer. Run from the project folder.

```bash
# Start the server on port 6379
python3 -m mini_redis

# Start with the append-only file on
python3 -m mini_redis --aof

# Other options
python3 -m mini_redis --port 6380 --aof --aof-path data/appendonly.aof
```

In another terminal (install `redis-cli` with `sudo apt install redis-tools`):

```console
$ redis-cli PING
PONG
$ redis-cli SET name Alice
OK
$ redis-cli GET name
"Alice"
$ redis-cli HSET user:1 city Hanoi
(integer) 1
$ redis-cli HGET user:1 city
"Hanoi"
```

With `--aof`, stop the server with Ctrl+C and start it again. `GET name` still
returns `"Alice"`.

## Tests

```bash
python3 -m unittest discover -s tests -v
```

`tests/test_aof.py` checks AOF crash recovery. It starts the real server as a
separate process on a free port, writes keys, cuts bytes off the AOF to fake a
crash, and checks that the server restarts with the right data.

## Project layout

```text
mini_redis/
  __main__.py     command-line options, starts the server
  config.py       server and AOF settings
  server.py       event loop: accept, read, run commands, write replies
  client.py       per-client state: socket, input and output buffers
  resp.py         RESP parser and encoder
  commands/
    registry.py   command table: name, handler, arity, write flag
    connection.py PING
    strings.py    SET, GET
    hashes.py     HSET, HGET
  storage/
    keyspace.py   the in-memory store
  persistence/
    aof.py        append-only file: write, fsync thread, replay, crash recovery
tests/
  helpers.py      build RESP commands and replay an AOF in tests
  test_aof.py     AOF crash recovery tests
  test_server.py  server tests (planned)
docs/
  architecture.md design and roadmap
```

## References

- [RESP protocol spec](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [memkv](https://github.com/quangh33/memkv) and
  [EP_redis_course](https://github.com/vietnakid/EP_redis_course), Go projects
  I used as references
