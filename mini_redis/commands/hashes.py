from typing import List

from mini_redis.resp import Value, RESPType
from mini_redis.storage.keyspace import store

def hset(args: List[Value]) -> Value:
    """HSET key field value: set one field. Returns 1 if the field is new, 0 if updated."""
    hash_key = args[0].bulk
    key = args[1].bulk  # the field name inside the hash
    value = args[2].bulk
    item = store.get(hash_key)

    if item is None:
        # First field: create the hash. The dict is stored by reference, so
        # writing to hash_map below updates the stored hash too.
        hash_map = {}
        store[hash_key] = ("hash", hash_map)
    else:
        typ, hash_map = item
        if typ != "hash":
            return Value(typ=RESPType.ERROR, string="WRONGTYPE Operation against a key holding the wrong kind of value")  # Return an error if the key is not a hash.

    is_new_key = key not in hash_map
    hash_map[key] = value  # Set the field in the hash map.

    return Value(typ=RESPType.INTEGER, num=1 if is_new_key else 0)

def hget(args: List[Value]) -> Value:
    """HGET key field: the field's value, or nil if the key or field is missing."""
    hash_key = args[0].bulk
    key = args[1].bulk  # the field name inside the hash
    item = store.get(hash_key)

    if item is None:
        return Value(typ=RESPType.NULL)  # Return a RESP null value if the hash does not exist.
    typ, hash_map = item
    if typ != "hash":
        return Value(typ=RESPType.ERROR, string="WRONGTYPE Operation against a key holding the wrong kind of value")  # Return an error if the key is not a hash.

    value = hash_map.get(key)

    if value is None:
        return Value(typ=RESPType.NULL)  # Return a RESP null value if the key does not exist in the hash.
    
    return Value(typ=RESPType.BULK_STRING, bulk=value)  # Return the value as a RESP bulk string if the key exists in the hash.