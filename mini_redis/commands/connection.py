"""Connection commands: PING."""

from typing import List

from mini_redis.resp import Value, RESPType

# Define a handler function for the PING command.
# It receives a list of RESP Values and returns one RESP Value.
def ping(args: List[Value]) -> Value:
    """Handle the PING command."""
    # Return a RESP simple string response containing "PONG".
    if len(args) > 1:
        return Value(typ=RESPType.ERROR, string="ERR wrong number of arguments for 'ping' command")
    
    if len(args) == 0:
        return Value(typ=RESPType.SIMPLE_STRING, string="PONG")
    
    return Value(typ=RESPType.BULK_STRING, bulk=args[0].bulk)  # Echo back the first argument if provided.
 