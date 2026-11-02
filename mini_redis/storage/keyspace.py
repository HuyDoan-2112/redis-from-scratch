
# Every key in the database. Each value is a (type, data) tuple:
#   ("string", bytes) or ("hash", dict[bytes, bytes]).
# The type tag lets commands return WRONGTYPE for the wrong kind of key.
# No lock is needed: only the event loop thread touches this dict.
import time

from mini_redis.propagation import propagate

store: dict[bytes, tuple[str, object]] = {}

# Keys that have a TTL, mapped to their deadline in ms since the epoch.
# A key that is not in here never expires.
expires: dict[bytes, int] = {}

def now_ms() -> int:
    """Current time in milliseconds since the epoch."""
    return time.time_ns() // 1_000_000

def lookup_key(key: bytes) -> tuple[str, object] | None:
    """The (type, data) for key, or None if missing or expired.
    An expired key is deleted here (lazy expiry)."""
    deadline = expires.get(key)
    if deadline is not None and deadline <= now_ms():
        del store[key]
        del expires[key]
        propagate(b"DEL", key) 
        return None

    return store.get(key)
