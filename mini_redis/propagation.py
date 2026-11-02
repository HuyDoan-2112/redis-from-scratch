"""
Changes to the data, waiting to be saved.

Handlers call propagate() at the moment they change the data. After each
command, server.py calls take() and writes the changes to the AOF (and, in
step 3e, sends them to replicas).

A change is saved as a command that gives the same result whenever it is
replayed. For example, EXPIRE k 10 is saved as PEXPIREAT k <deadline in ms>.
"""

from mini_redis.resp import RESPType, Value

# Changes made by the command that is running now. Only the event loop thread
# uses this list, and take() empties it after every command.
_pending: list[Value] = []

def propagate(*parts: bytes) -> None:
    """Record one change as a command, e.g. propagate(b"DEL", key)."""
    _pending.append(Value(
        typ=RESPType.ARRAY,
        array=[Value(typ=RESPType.BULK_STRING, bulk=part) for part in parts],
    ))

def take() -> list[Value]:
    """Return the changes made by the command that just finished, and clear them."""
    global _pending
    changes = _pending
    _pending = []

    return changes

