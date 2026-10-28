from typing import List

from mini_redis.resp import Value, RESPType
from mini_redis.storage.keyspace import store

# Named set_cmd, not set, so it does not hide Python's built-in set().
def set_cmd(args: List[Value]) -> Value:
    """SET key value: store a string, replacing any old value of any type."""
    key = args[0].bulk
    value = args[1].bulk
    store[key] = ("string", value)

    return Value(typ=RESPType.SIMPLE_STRING, string="OK")

def get(args: List[Value]) -> Value:
    """GET key: the string value, nil if missing, WRONGTYPE if not a string."""
    key = args[0].bulk
    item = store.get(key)

    if item is None:
        return Value(typ=RESPType.NULL)  # Return a RESP null value if the key does not exist.
    
    typ, value = item
    if typ != "string":
        return Value(typ=RESPType.ERROR, string="WRONGTYPE Operation against a key holding the wrong kind of value")  # Return an error if the key is not a string.
    
    return Value(typ=RESPType.BULK_STRING, bulk=value)  # Return the value as a RESP bulk string if the key exists.

