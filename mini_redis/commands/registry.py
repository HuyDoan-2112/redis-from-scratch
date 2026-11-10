from typing import Callable, List
from dataclasses import dataclass

from mini_redis.commands.persistence import bgrewriteaof_cmd, bgsave_cmd, lastsave_cmd
from mini_redis.resp import RESPType, Value
from mini_redis.commands.connection import ping, echo
from mini_redis.commands.strings import set_cmd, get, incr_cmd
from mini_redis.commands.hashes import hset, hget
from mini_redis.commands.keys import (
    dbsize_cmd,
    del_cmd,
    exists_cmd,
    expire_cmd,
    pexpireat_cmd,
    ttl_cmd,
)
from mini_redis.commands.errors import CommandError

@dataclass(frozen=True)
class Command:
    """One entry in the command table, the same idea as Redis's redisCommand."""

    name: str
    # Gets the arguments without the command name, returns the reply.
    handler: Callable[[List[Value]], Value]
    # Number of items including the command name: GET key is 2.
    # Negative means "at least": -1 is 1 or more (PING, PING msg).
    arity: int
    write: bool = False # True if the command modifies the store, False if it only reads

    def arity_ok(self, n: int) -> bool:
        """Check the request length `n` (command name included)."""
        return n == self.arity if self.arity > 0 else n >= -self.arity


def lookup_command(request: Value) -> Command:
    """Find the command for a request, or raise CommandError"""
    if request.typ != RESPType.ARRAY or not request.array:
        raise CommandError("ERR empty command")
    # Command names are case-insensitive: "get", "GET" and "Get" all work.
    name = request.array[0].bulk.decode(errors="replace").upper()
    cmd = COMMANDS.get(name)
    if cmd is None:
        raise CommandError(f"ERR unknown command '{name}'")
    if not cmd.arity_ok(len(request.array)):
        raise CommandError(f"ERR wrong number of arguments for '{name.lower()}' command")
    return cmd

# To add a command: write its handler, then add one line here.
# lookup_command() checks arity first, so handlers can index args without checking.
cmds = [
    Command("PING", ping, -1),
    Command("ECHO", echo, 2),
    Command("SET", set_cmd, -3, write=True),
    Command("GET", get, 2),
    Command("HSET", hset, 4, write=True),
    Command("HGET", hget, 3),
    Command("DEL", del_cmd, -2, write=True),
    Command("EXISTS", exists_cmd, -2),
    Command("EXPIRE", expire_cmd, -3, write=True),
    Command("PEXPIREAT", pexpireat_cmd, -3, write=True),
    Command("TTL", ttl_cmd, 2),
    Command("INCR", incr_cmd, 2, write=True),
    Command("DBSIZE", dbsize_cmd, 1),
    Command("BGREWRITEAOF", bgrewriteaof_cmd, 1),
    Command("BGSAVE", bgsave_cmd, 1),
    Command("LASTSAVE", lastsave_cmd, 1),
]
COMMANDS: dict[str, Command] = {c.name: c for c in cmds}
