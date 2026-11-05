"""
    AOF (Append-Only File): Redis records each write command in a file, in RESP format.
    On restart, the server replays the commands in the file to rebuild the data.
"""
import os
import threading

from pathlib import Path
from typing import Callable

from mini_redis.config import AOFConfig
from mini_redis.resp import RESPType, Value, RESPReader
from mini_redis.storage import keyspace


def _command(*parts: bytes) -> bytes:
    """RESP bytes for one command, e.g. _command(b"SET", b"k", b"v")."""
    return Value(
        typ=RESPType.ARRAY,
        array=[Value(typ=RESPType.BULK_STRING, bulk=p) for p in parts]
    ).marshal()

def keyspace_to_commands() -> bytes:
    """The shortest list of commands that rebuilds the current data."""
    out = bytearray()
    now = keyspace.now_ms()
    for key, (typ, data) in keyspace.store.items():
        deadline = keyspace.expires.get(key)
        if deadline is not None and deadline <= now:
            continue # Already expired: leave it out, or the rewrite brings it back.

        if typ == "string":
            out += _command(b"SET", key, data)
        elif typ == "hash":
            for field, val in data.items():
                out += _command(b"HSET", key, field, val)
        else:
            raise ValueError(f"rewrite does not know type {typ!r}")

        if deadline is not None:
            out += _command(b"PEXPIREAT", key, str(deadline).encode())

    return bytes(out)

def _fsync_dir(path: Path) -> None:
    """fsync a directory, so a rename inside it survives power loss."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

def _write_all( fd: int, data: bytearray) -> None:
    """Write all of data to the file. os.write() may write only part of it."""
    view = memoryview(data)
    while view:
        n = os.write(fd, view)
        view = view[n:]


class AOF:
    """
    The append-only file.

    Commands collect in memory, and flush() writes them to the file once per
    event loop turn, before replies go out. When they are fsynced depends on
    the appendfsync policy: in flush() (always), once per second in a
    background thread (everysec), or when the OS decides (no).
    """

    def __init__(self, config: AOFConfig):
        self.config = config
        self.stop_event = threading.Event()  # Set by close() to stop the sync thread.
        self.fd_lock = threading.Lock()

        Path(config.path).parent.mkdir(parents=True, exist_ok=True)

        # O_APPEND: the kernel puts every write at the end of the file.
        self.fd = os.open(config.path, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o644)
        self.buf = bytearray()
        self.sync_thread = None
        if config.appendfsync == "everysec":
            self.sync_thread = threading.Thread(
                target=self._sync_loop,
                daemon=True
            )
            self.sync_thread.start()

    def _sync_loop(self) -> None:
        """Background thread for everysec: fsync the file once per interval."""
        # wait() returns False on timeout (time to sync) and True once
        # close() sets the event, which ends the loop.
        while not self.stop_event.wait(self.config.sync_interval_secs):
            with self.fd_lock:
                os.fsync(self.fd)

    def flush(self) -> None:
        """Send the buffer to the kernel, and fsync it if the policy is always."""
        if not self.buf:
            return
        _write_all(self.fd, self.buf)
        self.buf.clear()
        if self.config.appendfsync == "always":
            os.fsync(self.fd)

    def close(self) -> None:
        """Stop the sync thread, then flush, fsync and close the file."""
        self.stop_event.set()
        if self.sync_thread:
            self.sync_thread.join()

        self.flush()
        os.fsync(self.fd)
        os.close(self.fd)

    def rewrite(self) -> None:
        """Replace the AOF with the shortest commands that rebuild the current data."""
        # Buffered commands are already in memory, so they are in the snapshot.
        # Flush them to the old file first: if the rewrite fails, they are still saved.
        self.flush()

        path = Path(self.config.path)
        temp = path.with_name("temp-rewrite.aof") # Same directory, so os.replace is atomic.

        fd = os.open(temp, os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            _write_all(fd, keyspace_to_commands())
            os.fsync(fd)
        except BaseException:
            os.close(fd)
            os.unlink(temp)
            raise
        os.replace(temp, path)
        _fsync_dir(path.parent)

        with self.fd_lock:
            old_fd, self.fd = self.fd, fd
            os.close(old_fd)

    def write(self, value: Value) -> None:
        """Add one write command to the buffer. flush() writes it to the file."""
        self.buf += value.marshal()

    def read(self, callback: Callable[[Value], None]) -> None:
        """
        Replay every command in the file through `callback`.

        A crash in the middle of a write can leave half a command at the
        end of the file. Cut that tail off, so new writes are not appended
        after broken bytes.
        """
        # RESPReader needs a file object, so read through a separate handle.
        with open(self.config.path, "rb") as f:
            reader = RESPReader(f)
            good_offset = 0
            while True:
                try:
                    # Same rules as client commands, so the AOF can never
                    # replay something a client could not have sent.
                    value = reader.read_command()
                except EOFError:
                    break

                callback(value)
                good_offset = f.tell()

            size = f.seek(0, os.SEEK_END)

        if good_offset < size:
            print(f"truncating AOF file from {size} to {good_offset}")
            # O_APPEND makes the next write land at the new end, so no seek.
            os.ftruncate(self.fd, good_offset)

# The server's AOF, set by server.run(). Command handlers only get args,
# so BGREWRITEAOF finds the AOF here. None when AOF is off.
current: AOF | None = None
