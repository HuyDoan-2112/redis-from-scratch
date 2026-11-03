import socket

from dataclasses import dataclass, field

from mini_redis.resp import CommandParser

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