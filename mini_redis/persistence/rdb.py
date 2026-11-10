import os
from pathlib import Path
import struct
import time
import zlib

from typing import BinaryIO

from mini_redis.config import RDBConfig
from mini_redis.persistence import child
from mini_redis.persistence.files import fsync_dir
from mini_redis.storage import keyspace

MAGIC = b"MINIRDB"
VERSION = 1
TYPE_STRING = 0
TYPE_HASH = 1
EOF = 0xFF
NO_EXPIRE = -1 

_U32 = struct.Struct("<I")
_I64 = struct.Struct("<q")
_RECORD = struct.Struct("<Bq")

# dump() collects this many bytes before writing them, and load() reads this
# many at a time. Memory stays small however big the file is.
_CHUNK = 256*1024

class RDBError(Exception):
    """The file is not a valid snapshot."""

class _Writer:
    """Collects bytes in a buffer, writes it out in chunks, and keeps a CRC of everything written."""

    def __init__(self, out: BinaryIO):
        self._out = out
        self._buf = bytearray()
        self.crc = 0 

    def write(self, data: bytes) -> None:
        self._buf += data
        if len(self._buf) >= _CHUNK:
            self.flush()

    def blob(self, data: bytes) -> None:
        """Write a lenght-prefixed byte string."""
        self.write(_U32.pack(len(data)))
        self.write(data)

    def flush(self) -> None:
        self.crc = zlib.crc32(self._buf, self.crc)
        self._out.write(self._buf)
        self._buf.clear()

def dump(out: BinaryIO, 
         store: dict[bytes, tuple[str, object]], 
         expires: dict[bytes, int], 
         now_ms: int
) -> None:
    """Write a snapshot of store and expires to out."""

    w = _Writer(out)
    w.write(MAGIC)
    w.write(bytes([VERSION]))

    for key, (typ, data) in store.items():
        deadline = expires.get(key)
        if deadline is None:
            expire = NO_EXPIRE
        elif deadline <= now_ms:
            continue
        else:
            expire = deadline

        if typ == "string":
            w.write(_RECORD.pack(TYPE_STRING, expire))
            w.blob(key)
            w.blob(data)

        elif typ == "hash":
            w.write(_RECORD.pack(TYPE_HASH, expire))
            w.blob(key)
            w.write(_U32.pack(len(data)))
            for field, val in data.items():
                w.blob(field)
                w.blob(val)

        else:
            raise ValueError (f"RDB does not know type {typ!r}")

    w.write(bytes([EOF]))
    w.flush()
    out.write(_U32.pack(w.crc))
    
class _Reader:

    def __init__(self, inp: BinaryIO):
        self._inp = inp
        self._buf = b""
        self._pos = 0
        self._crc = 0

    def read(self, n: int) -> bytes:
        """Give exactly n bytes to read"""

        if self._pos + n > len(self._buf):
            self._refill(n)
        data = self._buf[self._pos:self._pos + n]
        self._pos += n
        return data

    def _refill(self, n: int) -> None:
        """get more bytes of the file"""
        self._crc = zlib.crc32(memoryview(self._buf)[:self._pos], self._crc) # memory lets crc32 look at part of _buf without copying it.
        parts = [self._buf[self._pos:]]
        have = len(parts[0])
        while have < n:
            chunk = self._inp.read(_CHUNK)
            if not chunk:
                raise RDBError("unexpected end of file")
            parts.append(chunk)
            have += len(chunk)
        self._buf = b"".join(parts)
        self._pos = 0

    def u32(self) -> int:
        return _U32.unpack(self.read(4))[0]

    def blob(self) -> bytes:
        return self.read(self.u32())

    def crc(self) -> int:
        return zlib.crc32(memoryview(self._buf)[:self._pos], self._crc)

    def at_end(self) -> bool:
        return self._pos == len(self._buf) and not self._inp.read(1)
    

def load(inp: BinaryIO, now_ms: int) -> tuple[dict[bytes, tuple[str, object]], dict[bytes, int]]:
    """
    Read a snapshot. 
    
    Returns new (store, expires) dicts. 
    Raises RDBError for a bad file
    """
    r = _Reader(inp)
    if r.read(len(MAGIC)) != MAGIC:
        raise RDBError("Not an RDB file (bad magic)")
    version = r.read(1)[0]
    if version != VERSION:
        raise RDBError(f"unsupported RDB version {version}")

    store = {}
    expires = {}
    while True:
        typ = r.read(1)[0]
        if typ == EOF:
            break
        if typ not in (TYPE_STRING, TYPE_HASH):
            raise RDBError(f"unknown record type {typ}")
        
        expire = _I64.unpack(r.read(8))[0]
        key = r.blob()
        if typ == TYPE_STRING:
            value = ("string", r.blob())
        elif typ == TYPE_HASH:
            fields = {}
            for _ in range(r.u32()):
                field = r.blob()
                fields[field] = r.blob() # Python runs the right side first in assignment

            value = ("hash", fields)

        if expire != NO_EXPIRE and expire <= now_ms:
            continue
        store[key] = value
        if expire != NO_EXPIRE:
            expires[key] = expire

    expected = r.crc()
    if r.u32() != expected:
        raise RDBError("checksum mismatch")
    if not r.at_end():
        raise RDBError("extra bytes after checksum")

    return store, expires

class RDB:
    """The snapshot file. BGSAVE writes it in a child process."""

    def __init__(self, config: RDBConfig):
        self.config = config

        config.path.parent.mkdir(parents=True, exist_ok=True)

        self.lastsave = int(time.time())
        self.last_save_ok = True
        self.dirty = 0
        self._dirty_at_fork = 0
        self._last_try = 0.0
    
    def load_file(self) -> int | None:
        """Load the snapshot file into the keyspace. Returns the number of keys,
           or None if there is no file. Raises RDBError or OSError for a file that can't be used."""
        started = time.perf_counter()
        try:
            with open(self.config.path, "rb") as f:
                new_store, new_expires = load(f, keyspace.now_ms())
        except FileNotFoundError:
            return None
        except OSError as err:
            raise RDBError(err.strerror) from err
        keyspace.replace_all(new_store, new_expires)
        print(f"DB loaded from disk: {len(new_store)} keys in {time.perf_counter() - started:.3f} seconds")
        return len(new_store)   
    
    def start_save(self) -> None:
        """Start a save in a child process. The caller makes sure no child is running."""
        self._dirty_at_fork = self.dirty   # what this save will cover
        self._last_try = time.time()       # for the 5-second retry delay
        try:
            child.start("rdb", self._write_file, self._save_done)
        except OSError:
            self.last_save_ok = False      # fork failed: the retry delay applies too
            raise

    def _write_file(self) -> None:
        temp = self._temp_path()
        with open(temp, "wb") as f:
            dump(f, keyspace.store, keyspace.expires, keyspace.now_ms()) 
            f.flush() # Py buffer -> kernel
            os.fsync(f.fileno())   # kernel -> disk
        os.replace(temp, self.config.path)
        fsync_dir(self.config.path.parent)
        
    def save(self) -> None:
        """Save in this process, at shutdown. Raises OSError if it fails."""
        try:
            self._write_file()
        except OSError:
            self.last_save_ok = False
            try:
                self._temp_path().unlink(missing_ok=True)
            except OSError:
                pass    # keep the first error, which explains what went wrong
            raise
        self.dirty = 0
        self.lastsave = int(time.time())
        self.last_save_ok = True
        print("DB saved on disk")

    def save_point_due(self, now: float) -> tuple[int, int] | None:
        """The first save point that is due, or None. Waits 5 seconds after a failed save."""
        if not self.last_save_ok and now - self._last_try < 5:
            return None
        for seconds, changes in self.config.save_points:
            if self.dirty >= changes and now - self.lastsave > seconds:
                return (seconds, changes)
        return None
    
    def _save_done(self, ok: bool) -> None:
        self.last_save_ok = ok
        if ok:
            # The child already renamed the temp file to dump.rdb
            self.lastsave = int(time.time())
            self.dirty -= self._dirty_at_fork
            print("Background saving terminated with success")
            return
        # Failed or killed. child.check() already printed why, so don't print it again.
        try:
            self._temp_path().unlink(missing_ok=True)
        except OSError as err:
            print(f"Could not remove {self._temp_path()}: {err}")    
    
    def _temp_path(self) -> Path:
        """Temp file next to dump.rdb, so os.replace is atomic."""
        return self.config.path.with_name("temp-bgsave.rdb")

current: RDB | None = None
