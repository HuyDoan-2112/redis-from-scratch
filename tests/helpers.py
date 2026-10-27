
from pathlib import Path

from mini_redis.persistence.aof import AOF
from mini_redis.config import AOFConfig
from mini_redis.resp import Value, RESPType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_cmd(*parts: str) -> Value:
    """Build a RESP array command, e.g. make_cmd("SET", "a", "1")."""
    return Value(
        typ=RESPType.ARRAY,
        array=[Value(typ=RESPType.BULK_STRING, bulk=p.encode()) for p in parts],
    )


def cmd_bytes(*parts: str) -> bytes:
    """RESP bytes of a command, exactly as the AOF stores it."""
    return make_cmd(*parts).marshal()


def replay(path: Path) -> list[list[str]]:
    """Open the AOF, replay it, and return the commands as lists of strings.

    We collect commands in a list instead of using the global store,
    so tests do not affect each other.
    """
    seen: list[list[str]] = []
    aof = AOF(AOFConfig(path=path, enabled=True))
    try:
        aof.read(lambda v: seen.append([item.bulk.decode() for item in v.array]))
    finally:
        aof.close()
    return seen
