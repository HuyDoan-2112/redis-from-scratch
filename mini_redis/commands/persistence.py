""" Persistence commands """
from typing import List

from mini_redis.persistence import aof, child, rdb
from mini_redis.resp import RESPType, Value

def bgrewriteaof_cmd(args: List[Value]) -> Value:
    """BGREWRITEAOF: replace the AOF with the shortest commands that rebuild the data."""
    if aof.current is None:
        return Value(typ=RESPType.ERROR, string="ERR AOF is not enabled")
    if child.kind == "aof":
        return Value(typ=RESPType.ERROR, string="ERR Background append only file rewriting already in progress")
    if child.kind is not None:
        # Another child (an RDB save) is running. cron starts the rewrite once it is done.
        aof.current.rewrite_scheduled = True
        return Value(typ=RESPType.SIMPLE_STRING, string="Background append only file rewriting scheduled")
    aof.current.start_rewrite()
    return Value(typ=RESPType.SIMPLE_STRING, string="Background append only file rewriting started")

def bgsave_cmd(args: List[Value]) -> Value:
    """BGSAVE: write a snapshot of the data to the RDB file, in a child process."""
    if child.kind == "rdb":
        return Value(typ=RESPType.ERROR, string="ERR Background save already in progress")
    if child.kind is not None:
        return Value(typ=RESPType.ERROR, string=(
            "ERR Another child process is active (AOF?): can't BGSAVE right now. "
            "Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenever possible."
        ))
    rdb.current.start_save()
    return Value(typ=RESPType.SIMPLE_STRING, string="Background saving started")

def lastsave_cmd(args: List[Value]) -> Value:
    """LASTSAVE: Unix time, in seconds, of the last successful save."""
    return Value(typ=RESPType.INTEGER, num=rdb.current.lastsave)
