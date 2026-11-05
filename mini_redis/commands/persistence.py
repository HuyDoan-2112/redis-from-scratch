""" Persistence commands """
from typing import List

from mini_redis.persistence import aof
from mini_redis.resp import RESPType, Value

def bgrewriteaof_cmd(args: List[Value]) -> Value:
    """BGREWRITEAOF: replace the AOF with the shortest commands that rebuild the data."""
    if aof.current is None:
        return Value(typ=RESPType.ERROR, string="ERR AOF is not enabled")
    aof.current.rewrite()
    return Value(typ=RESPType.SIMPLE_STRING, string="Background append only file rewriting started")
