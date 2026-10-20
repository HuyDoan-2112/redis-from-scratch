"""
    AOF (Append-Only File) is a method that Redis record each command in the file as RESP.
    When a restart, the server can replay the commands in the AOF file to restore the database state.
"""
import os
import threading

from pathlib import Path
from typing import Callable

from mini_redis.config import AOFConfig
from mini_redis.resp import Value, RESPReader

class AOF:
    def __init__(self, config: AOFConfig):
        self.config = config
        self.lock = threading.Lock() # A lock to ensure thread-safe access to the AOF file.
        self.stop_event = threading.Event() # A flag to indicate whether the AOF file is closed.

        Path(config.path).parent.mkdir(parents=True, exist_ok=True) # Ensure the directory for the AOF file exists.

        # Python file objects are already buffered, so no seperat bufio.Reader is needed.
        # buffer means a temp holding area in computer memory (RAM) used to store data
        # while it moves from one place to another. 
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
        while not self.stop_event.wait(self.config.sync_interval_secs):
            with self.lock:
                self.file.flush() # Flush the file buffer to disk.
                os.fsync(self.file.fileno()) # Ensure the data is written to disk.
        

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
            Write the command to AOF file whenever we receive a request from the client.
        """
        with self.lock:
            self.file.write(value.marshal())

    def read(self, callback: Callable[[Value], None]) -> None:
        with self.lock:
            self.file.flush()
            self.file.seek(0)

            reader = RESPReader(self.file)
            good_offset = 0
            while True:
                try:
                    value = reader.read()
                except EOFError:
                    break

                callback(value)
                good_offset = self.file.tell() # Update the good offset after successfully reading a value.

            size = self.file.seek(0, os.SEEK_END)
            if good_offset < size:
                print(f"truncating AOF file from {size} to {good_offset}")
                self.file.truncate(good_offset) # Truncate the file to the last good offset to remove any corrupted data.
                self.file.seek(good_offset) # Move the file pointer to the end of the truncated file.


        


        
