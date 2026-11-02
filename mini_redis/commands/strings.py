from typing import List

from mini_redis.resp import Value, RESPType
from mini_redis.storage.keyspace import (
    lookup_key,
    remove_ttl,
    set_ttl,
    store,
    now_ms,
    delete_key
)
from mini_redis.commands.args import INT64_MAX, SetOptions, parse_int, parse_set_options
from mini_redis.commands.errors import CommandError
from mini_redis.propagation import propagate

def set_cmd(args: List[Value]) -> Value:
    """SET key value [NX | XX] [GET] [EX s | PX ms | EXAT t | PXAT t | KEEPTTL]"""
    key = args[0].bulk
    value = args[1].bulk
    opts = parse_set_options(args[2:])
    item  = lookup_key(key)
    old = None
    if opts.get and item is not None:
        typ, old = item
        if typ != "string":
            raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")

    exists = item is not None
    if (opts.condition == b"NX" and exists) or (opts.condition == b"XX" and not exists):
        return reply_set(opts, old, wrote=False)

    store[key] = ("string", value)
    if opts.expire_at_ms is not None:
        if opts.expire_at_ms <= now_ms():
            delete_key(key)
        else:
            set_ttl(key, opts.expire_at_ms)
            propagate(b"SET", key, value, b"PXAT", str(opts.expire_at_ms).encode())
    elif opts.keepttl:
        propagate(b"SET", key, value, b"KEEPTTL")
    else:
        remove_ttl(key)
        propagate(b"SET", key, value)

    return reply_set(opts, old, wrote=True)

def reply_set(opts: SetOptions, value: bytes | None, wrote: bool) -> Value:

    if opts.get:
        if value is None:
            return Value(typ=RESPType.NULL)
        return Value(typ=RESPType.BULK_STRING, bulk=value)
    if not wrote:
        return Value(typ=RESPType.NULL)

    return Value(typ=RESPType.SIMPLE_STRING, string="OK")

def get(args: List[Value]) -> Value:
    """GET key: the string value, nil if missing, WRONGTYPE if not a string."""
    key = args[0].bulk
    item = lookup_key(key)

    if item is None:
        return Value(typ=RESPType.NULL)  # Return a RESP null value if the key does not exist.

    typ, value = item
    if typ != "string":
        return Value(typ=RESPType.ERROR, string="WRONGTYPE Operation against a key holding the wrong kind of value")  # Return an error if the key is not a string.

    return Value(typ=RESPType.BULK_STRING, bulk=value)  # Return the value as a RESP bulk string if the key exists.

def incr_cmd(args: List[Value]) -> Value:
    """INCR key: add 1 to the integer stored as a string, and return it.

    A missing key counts as 0. The value is parsed as a 64-bit signed
    integer and written back as decimal digits; the TTL is kept.
    """
    key = args[0].bulk
    item = lookup_key(key)
    if item is None:
        curr_value = 0  # a missing key counts as 0
    else:
        typ, raw = item
        if typ != "string":
            raise CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")
        curr_value = parse_int(raw)

    curr_value  += 1
    if curr_value > INT64_MAX:
        raise CommandError("ERR increment or decrement would overflow")

    store[key] = ("string", str(curr_value).encode())
    propagate(b"INCR", key)
    return Value(typ=RESPType.INTEGER, num=curr_value)
