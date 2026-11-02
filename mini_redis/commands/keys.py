from typing import List

from mini_redis.resp import RESPType, Value
from mini_redis.storage.keyspace import expires, store, now_ms, lookup_key
from mini_redis.commands.args import (
    parse_expire_options, 
    parse_int, 
    INT64_MAX, 
    INT64_MIN, 
    expire_allowed
)
from mini_redis.commands.errors import CommandError


def del_cmd(args: List[Value]) -> Value:
    """DEL key [key ...]: delete keys of any type, and their TTLs.

    Returns how many of the keys existed and were deleted.
    """
    count = 0
    for arg in args:
        key = arg.bulk
        if lookup_key(key) is not None:
            del store[key]
            expires.pop(key, None)
            count += 1

    return Value(typ=RESPType.INTEGER, num=count)

def exists_cmd(args: List[Value]) -> Value:
    """EXISTS key [key ...]: how many of the keys exist.

    A key named twice is counted twice, like Redis.
    """
    count = 0
    for arg in args:
        key = arg.bulk
        if lookup_key(key) is not None:
            count += 1

    return Value(typ=RESPType.INTEGER, num=count)

def expire_cmd(args: List[Value]) -> Value:
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

    if lookup_key(key) is None:
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

def ttl_cmd(args: List[Value]) -> Value:
    """TTL key: seconds left before the key expires.

    -2 if the key does not exist, -1 if it has no TTL. Works on any type.
    """
    key = args[0].bulk
    if lookup_key(key) is None:
        return Value(typ=RESPType.INTEGER, num=-2)

    deadline = expires.get(key)
    if deadline is None:
        return Value(typ=RESPType.INTEGER, num=-1)

    # Round to the nearest second, like Redis: (ms + 500) // 1000.
    # lookup_key() already removed expired keys, so ms_left is above 0.
    ms_left = deadline - now_ms()
    return Value(typ=RESPType.INTEGER, num=(ms_left + 500) // 1000)
