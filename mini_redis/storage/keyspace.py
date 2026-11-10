import time
import random

from mini_redis.propagation import propagate

# Every key in the database. Each value is a (type, data) tuple:
#   ("string", bytes) or ("hash", dict[bytes, bytes]).
# The type tag lets commands return WRONGTYPE for the wrong kind of key.
# No lock is needed: only the event loop thread touches this dict.
store: dict[bytes, tuple[str, object]] = {}

# Keys that have a TTL, mapped to their deadline in ms since the epoch.
# A key that is not in here never expires.
expires: dict[bytes, int] = {}
ttl_keys: list[bytes] = []  # keys that have a TTL, in any order
ttl_pos: dict[bytes, int] = {}  # key -> index in ttl_keys

def now_ms() -> int:
    """Current time in milliseconds since the epoch."""
    return time.time_ns() // 1_000_000

def lookup_key(key: bytes) -> tuple[str, object] | None:
    """The (type, data) for key, or None if missing or expired.
    An expired key is deleted here (lazy expiry)."""
    deadline = expires.get(key)
    if deadline is not None and deadline <= now_ms():
        delete_key(key)
        return None

    return store.get(key)

def delete_key(key: bytes) -> None:
    """Delete key and its TTL."""
    del store[key]
    remove_ttl(key)
    propagate(b"DEL", key)

def set_ttl(key: bytes, deadline: int) -> None:
    """Set or change key's deadline (ms since the epoch). The caller makes sure key is in store."""
    expires[key] = deadline
    if key not in ttl_pos:
        ttl_pos[key] = len(ttl_keys)
        ttl_keys.append(key)

def remove_ttl(key: bytes) -> None:
    """Drop key's TTL. Does nothing if key has none."""
    pos = ttl_pos.pop(key, None)
    if pos is None:
        return
    del expires[key]

    last_key = ttl_keys.pop()
    if pos < len(ttl_keys):
        ttl_keys[pos] = last_key
        ttl_pos[last_key] = pos

def sample_ttl_keys(n: int) -> list[bytes]:
    """Return up to n keys that have a TTL."""
    if len(ttl_keys) <= n:
        return ttl_keys.copy()
    return [ttl_keys[i] for i in random.sample(range(len(ttl_keys)), n)]

def replace_all(new_store: dict, new_expires: dict) -> None:
    """Replace all data, e.g. with a loaded snapshot. Not logged to the AOF."""
    store.clear()
    store.update(new_store)
    expires.clear()
    ttl_keys.clear()
    ttl_pos.clear()
    for key, deadline in new_expires.items():
        set_ttl(key, deadline)
