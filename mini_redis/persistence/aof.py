"""
    AOF (Append-Only File): Redis records each write command in a file, in RESP format.
    On restart, the server replays the commands in the file to rebuild the data.
"""
import os
import threading

from pathlib import Path
from typing import Callable

from mini_redis.config import AOFConfig
from mini_redis.resp import Value, RESPReader

class AOF:
    """The append-only file, fsynced once per interval by a background thread."""

    def __init__(self, config: AOFConfig):
        self.config = config
        # The event loop writes and the sync thread flushes the same file
        # object, so both must hold this lock.
        self.lock = threading.Lock()
        self.stop_event = threading.Event() # A flag to indicate whether the AOF file is closed.

        Path(config.path).parent.mkdir(parents=True, exist_ok=True) # Ensure the directory for the AOF file exists.

        # Python file objects are already buffered, so no seperat bufio.Reader is needed.
        # buffer means a temp holding area in computer memory (RAM) used to store data
        # while it moves from one place to another.
        # "a+b": append mode, so every write goes to the end of the file, plus
        # read access for replaying it at startup. Binary, because keys and
        # values can be any bytes.
        self.file = open(config.path, "a+b")
        self.sync_thread = threading.Thread(
            target=self._sync_loop, 
            daemon=True
        )
        self.sync_thread.start()
    def _sync_loop(self) -> None:
        """
        A background thread that periodically flushes the AOF file to disk.
        This is done to ensure that the data is persisted even if the server crashes.
        """
        # wait() returns False on timeout (time to sync) and True once
        # close() sets the event, which ends the loop.
        while not self.stop_event.wait(self.config.sync_interval_secs):
            with self.lock:
                self.file.flush() # Python's buffer -> OS page cache (not on disk yet).
                os.fsync(self.file.fileno()) # OS page cache -> disk.
        

    def close(self) -> None:
        """
           Close the AOF file and stop the sync thread.
        """
        self.stop_event.set() # Signal the sync thread to stop.
        self.sync_thread.join() # Wait for the sync thread to finish.
        with self.lock:
            self.file.flush()
            os.fsync(self.file.fileno())
            try:
                self.file.close()
            except OSError as err:
                print(f"failed to close AOF file: {err}")
                raise

    def write(self, value: Value) -> None:
        """
            Append one write command to the AOF.

            This only fills Python's buffer, so it is fast. The data reaches
            disk at the next fsync in _sync_loop(), so a crash can lose up to
            about one sync interval of writes.
        """
        with self.lock:
            self.file.write(value.marshal())

    def read(self, callback: Callable[[Value], None]) -> None:
        """
            Replay every command in the file through `callback`.

            A crash in the middle of a write can leave half a command at the
            end of the file. Cut that tail off, so new writes are not appended
            after broken bytes.
        """
        with self.lock:
            self.file.flush()
            self.file.seek(0)

            reader = RESPReader(self.file)
            good_offset = 0
            while True:
                try:
                    # Same rules as client commands, so the AOF can never
                    # replay something a client could not have sent.
                    value = reader.read_command()
                except EOFError:
                    break

                callback(value)
                good_offset = self.file.tell() # Update the good offset after successfully reading a value.

            size = self.file.seek(0, os.SEEK_END)
            if good_offset < size:
                print(f"truncating AOF file from {size} to {good_offset}")
                self.file.truncate(good_offset) # Truncate the file to the last good offset to remove any corrupted data.
                self.file.seek(good_offset) # Move the file pointer to the end of the truncated file.


        


        
