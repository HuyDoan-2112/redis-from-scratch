import socket

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from mini_redis.resp import CommandParser

if TYPE_CHECKING:
    # Only for type checkers: importing it at run time would be circular,
    # because master.py imports this module.
    from mini_redis.replication.master import Replica

@dataclass(eq=False)
class Client:
    """per-connection state"""

    sock:  socket.socket
    # Turns received bytes into commands. Holds the bytes not parsed yet,
    # which can end with half a command.
    parser: CommandParser = field(default_factory=CommandParser)
    # Replies not yet sent. send() may take only part of it at a time.
    outbuf: bytearray = field(default_factory=bytearray)

    # True after a protocol error: send what is left in outbuf, then close.
    closing: bool = False

    # Set by REPLCONF during a replica's handshake, before PSYNC.
    listening_port: int = 0                                 # 0 = not given
    capa: set[str] = field(default_factory=set)             # e.g. {"eof", "psync2"}

    # Set by PSYNC: this connection is a replica of this server.
    replica: "Replica | None" = None
