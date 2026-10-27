import socket

from dataclasses import dataclass, field

@dataclass
class Client:
    """per-connection state"""

    sock:  socket.socket
    # buffered input and output
    inbuf: bytearray = field(default_factory=bytearray)
    outbuf: bytearray = field(default_factory=bytearray)

    closing: bool = False