from typing import List

from mini_redis.resp import Value, RESPType
from mini_redis.storage.keyspace import store, store_lock


def hset(args: List[Value]) -> Value:
    if len(args) != 3:
        return Value(typ=RESPType.ERROR, string="ERR wrong number of arguments for 'hset' command")

    hash_key = args[0].bulk
    key = args[1].bulk
    value = args[2].bulk

    with store_lock:
        item = store.get(hash_key)
        if item is None:
           hash_map = {}
           store[hash_key] = ("hash", hash_map)
        else:
            typ, hash_map = item
            if typ != "hash":
                return Value(typ=RESPType.ERROR, string=f"WRONGTYPE Operation against a key holding the wrong kind of value")  # Return an error if the key is not a hash.

        is_new_key = key not in hash_map
        hash_map[key] = value  # Set the field in the hash map.

    return Value(typ=RESPType.INTEGER, num=1 if is_new_key else 0)

def hget(args: List[Value]) -> Value:
    if len(args) != 2:
        return Value(typ=RESPType.ERROR, string="ERR wrong number of arguments for 'hget' command")

    hash_key = args[0].bulk
    key = args[1].bulk

    with store_lock:
        item = store.get(hash_key)
        if item is None:
            return Value(typ=RESPType.NULL)  # Return a RESP null value if the hash does not exist.
        typ, hash_map = item
        if typ != "hash":
            return Value(typ=RESPType.ERROR, string=f"WRONGTYPE Operation against a key holding the wrong kind of value")  # Return an error if the key is not a hash.

        value = hash_map.get(key)

    if value is None:
        return Value(typ=RESPType.NULL)  # Return a RESP null value if the key does not exist in the hash.
    
    return Value(typ=RESPType.BULK_STRING, bulk=value)  # Return the value as a RESP bulk string if the key exists in the hash.