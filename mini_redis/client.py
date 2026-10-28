import socket

from dataclasses import dataclass, field

@dataclass
class Client:
    """per-connection state"""

    sock:  socket.socket
    # Bytes received but not yet parsed. Can end with half a command.
    inbuf: bytearray = field(default_factory=bytearray)
    # Replies not yet sent. send() may take only part of it at a time.
    outbuf: bytearray = field(default_factory=bytearray)

    # True after a protocol error: send what is left in outbuf, then close.
    closing: bool = False