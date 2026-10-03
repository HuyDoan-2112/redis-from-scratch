# Import List for type hints like List[Value], and Callable for function type hints.
import threading
from typing import List, Callable

# Import the RESP Value class used for command arguments and responses.
from mini_redis.resp import Value, RESPType

store_lock = threading.Lock()  # A lock to ensure thread-safe access to the store.
store: dict[str, tuple[str, object]] = {} # A global dictionary to store key-value pairs, where the value is a tuple of (value, expiration_time).
# Define a handler function for the PING command.
# It receives a list of RESP Values and returns one RESP Value.
def ping(args: List[Value]) -> Value:
    """Handle the PING command."""
    # Return a RESP simple string response containing "PONG".
    if len(args) > 1:
        return Value(typ=RESPType.ERROR, string="ERR wrong number of arguments for 'ping' command")
    
    if len(args) == 0:
        return Value(typ=RESPType.SIMPLE_STRING, string="PONG")
    
    return Value(typ=RESPType.SIMPLE_STRING, string=args[0].bulk)  # Echo back the first argument if provided.
 
def set(args: List[Value]) -> Value:
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
        return Value(typ=RESPType.ERROR, string=f"ERR wrong type for key '{key}'")  # Return an error if the key is not a string.
    
    return Value(typ=RESPType.BULK_STRING, bulk=value)  # Return the value as a RESP bulk string if the key exists.

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

# Create a dictionary that maps command names to handler functions.
# The key is a string command name, and the value is a callable handler.
# Callable [arguments, return type]
handlers: dict[str, Callable[[List[Value]], Value]] = {
    # When the command is "PING", call the ping function.
    "PING": ping,
    "SET": set,
    "GET": get,
    "HSET": hset,
    "HGET": hget,
}