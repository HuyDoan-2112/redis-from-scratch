"""Replication commands."""
from typing import List

from mini_redis.client import Client
from mini_redis.commands.args import SYNTAX_ERROR, parse_int
from mini_redis.commands.errors import CommandError
from mini_redis.replication import master
from mini_redis.resp import RESPType, Value

def replconf_cmd(client: Client, args: List[Value]) -> Value | None:
    """REPLCONF <option> <value> [<option> <value> ...]: a replica describes
    itself during the handshake, and later reports its offset."""
    if len(args) % 2 != 0:
        raise CommandError(SYNTAX_ERROR)                     # options come in pairs
    for i in range(0, len(args), 2):
        option, value = args[i].bulk, args[i + 1].bulk
        match option.lower():
            case b"listening-port":
                client.listening_port = parse_int(value)    # Redis does no range check
            case b"capa":
                client.capa.add(value.decode(errors="replace").lower())
            case b"ack" | b"getack":
                return None                                 # never a reply
            case _:
                raise CommandError(f"ERR Unrecognized REPLCONF option: {option.decode(errors='replace')}")
    return Value(typ=RESPType.SIMPLE_STRING, string="OK")

def psync_cmd(client: Client, args: List[Value]) -> None:
    """PSYNC <replid> <offset>: the replica asks for the stream. The master
    writes its own reply (+FULLRESYNC, then the RDB), so there is none here."""
    master.current.psync(client)
    return None
