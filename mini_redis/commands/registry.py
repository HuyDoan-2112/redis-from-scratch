from typing import Callable, List
from dataclasses import dataclass

from mini_redis.resp import Value
from mini_redis.commands.connection import ping
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
    
COMMANDS: dict[str, Command] = {c.name: c for c in [
    Command("PING", ping, -1),
    Command("SET", set_cmd, 3, write=True),
    Command("GET", get, 2),
    Command("HSET", hset, 4, write=True),
    Command("HGET", hget, 3),
]}