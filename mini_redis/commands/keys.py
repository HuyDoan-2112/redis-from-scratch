import time
from typing import List

from mini_redis.resp import RESPType, Value
from mini_redis.storage.keyspace import expires, store
from mini_redis.commands.args import (
    parse_expire_options, 
    parse_int, 
    INT64_MAX, 
    INT64_MIN, 
    expire_allowed
)
from mini_redis.commands.errors import CommandError


def del_cmd(args: List[Value]) -> Value:
    count = 0 
    for arg in args:
        key = arg.bulk
        if key in store:
            del store[key]
            expires.pop(key, None)
            count += 1

    return Value(typ=RESPType.INTEGER, num=count)

def exists_cmd(args: List[Value]) -> Value:
    count = 0
    for arg in args:
        key = arg.bulk
        if key in store:
            count += 1

    return Value(typ=RESPType.INTEGER, num=count)

EXPIRE_OPTIONS = {b"NX", b"XX", b"GT", b"LT"}

def expire_cmd(args: List[Value]) -> int:
    """EXPIRE key seconds [NX | XX | GT | LT]

    Returns 1 if the timeout was set, 0 if the key is missing or an
    option's condition was not met.
    """
    key = args[0].bulk
    options = parse_expire_options(args[2:])

    # turn seconds into an absolute deadline in milliseconds.
    seconds = parse_int(args[1].bulk)
    now = now_ms()
    deadline = now + seconds*1000
    if not INT64_MIN <= deadline <= INT64_MAX:
        raise CommandError("ERR invalid expire time in 'expire' command")

    if key not in store:
        return Value(typ=RESPType.INTEGER, num=0)
    if not expire_allowed(options, expires.get(key), deadline):
        return Value(typ=RESPType.INTEGER, num=0)

    # All checks passed: now change the data.
    if deadline <= now:
        # Already in the past: the key expires right away.
        del store[key]
        expires.pop(key, None)
    else:
        expires[key] = deadline
    return Value(typ=RESPType.INTEGER, num=1)

def now_ms() -> int:
    """Current time in milliseconds since the epoch."""
    return time.time_ns() // 1_000_000