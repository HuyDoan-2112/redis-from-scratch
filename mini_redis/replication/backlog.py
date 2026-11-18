"""
The replication backlog: the last `size` bytes of the replication stream.

A replica that reconnects asks for the stream from the next byte it needs.
If that byte is still here, the master sends the rest from here (a partial
resync). Otherwise the replica needs a full sync.

Offsets count bytes from 1, like Redis: after feeding 10 bytes, `offset` is 10
and the next replica that is up to date asks for byte 11.
"""

class Backlog:
    """A fixed-size ring buffer of the most recent replication stream bytes."""

    def __init__(self, size: int, offset: int = 0):
        if size < 1:
            raise ValueError("backlog size must be at least 1")
        self.size = size            # seats in buf
        self.buf = bytearray(size)
        self.offset = offset        # byte number of the last byte fed
        self.histlen = 0            # bytes held, at most size
        self.idx = 0                # where the next byte goes in buf

    @property
    def first_byte_offset(self) -> int:
        """Byte number of the oldest byte held."""
        return self.offset - self.histlen + 1

    def feed(self, data: bytes) -> None:
        """Add stream bytes. Once full, each new byte replaces the oldest."""
        n = len(data)
        self.offset += n
        view = memoryview(data)
        if n >= self.size:
            # Only the last `size` bytes survive.
            self.buf[:] = view[n - self.size:]
            self.idx = 0
            self.histlen = self.size
            return
        first = min(n, self.size - self.idx)     # up to the end of buf
        self.buf[self.idx:self.idx + first] = view[:first]
        self.buf[:n-first] = view[first:]
        self.idx = (self.idx + n) % self.size
        self.histlen = min(self.size, self.histlen + n)

    def read_from(self, offset: int) -> bytes | None:
        """Bytes from byte number `offset` to the end. b"" if the caller
        already has everything, None if `offset` is no longer held."""
        if not self.first_byte_offset <= offset <= self.offset + 1:
            return None
        length = self.offset - offset + 1
        start = (self.idx - length) % self.size
        if start + length <= self.size:
            return bytes(self.buf[start:start + length])
        return bytes(self.buf[start:]) + bytes(self.buf[:start + length - self.size])
