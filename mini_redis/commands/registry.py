from typing import Callable, List
from dataclasses import dataclass

from mini_redis.resp import RESPType, Value
from mini_redis.commands.connection import ping, echo
from mini_redis.commands.strings import set_cmd, get
from mini_redis.commands.hashes import hset, hget


@dataclass(frozen=True)
class Command:
    name: str
    handler: Callable[[List[Value]], Value]
    arity: int # counts the command name, negative means "at least" -1
    write: bool = False # True if the command modifies the store, False if it only reads

    def arity_ok(self, n: int) -> bool:
        return n == self.arity if self.arity > 0 else n >= -self.arity

class CommandError(Exception):
    pass

def lookup(request: Value) -> Command:
    """Find the command for a request, or raise CommandError"""
    if request.typ != RESPType.ARRAY or not request.array:
        raise CommandError("ERR empty command")
    name = request.array[0].bulk.decode(errors="replace").upper()
    cmd = COMMANDS.get(name)
    if cmd is None:
        raise CommandError(f"ERR unknown command '{name}'")
    if not cmd.arity_ok(len(request.array)):
        raise CommandError(f"ERR wrong number of arguments for '{name.lower()}' command")
    return cmd

cmds = [
    Command("PING", ping, -1),
    Command("ECHO", echo, 2),
    Command("SET", set_cmd, 3, write=True),
    Command("GET", get, 2),
    Command("HSET", hset, 4, write=True),
    Command("HGET", hget, 3),
]
COMMANDS: dict[str, Command] = {c.name: c for c in cmds}