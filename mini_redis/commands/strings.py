from typing import List

from mini_redis.resp import Value, RESPType
from mini_redis.storage.keyspace import store_lock, store

def set_cmd(args: List[Value]) -> Value:
    if len(args) != 2:
        return Value(typ=RESPType.ERROR, string="ERR wrong number of arguments for 'set' command")
    
    key = args[0].bulk
    value = args[1].bulk

    with store_lock:
        store[key] = ("string", value) 

    return Value(typ=RESPType.SIMPLE_STRING, string="OK")

def get(args: List[Value]) -> Value:
    if len(args) != 1:
        return Value(typ=RESPType.ERROR, string="ERR wrong number of arguments for 'get' command")

    key = args[0].bulk

    with store_lock:
        item = store.get(key)

    if item is None:
        return Value(typ=RESPType.NULL)  # Return a RESP null value if the key does not exist.
    
    typ, value = item
    if typ != "string":
        return Value(typ=RESPType.ERROR, string=f"ERR wrong type for key")  # Return an error if the key is not a string.
    
    return Value(typ=RESPType.BULK_STRING, bulk=value)  # Return the value as a RESP bulk string if the key exists.

