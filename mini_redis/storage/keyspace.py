
# Every key in the database. Each value is a (type, data) tuple:
#   ("string", bytes) or ("hash", dict[bytes, bytes]).
# The type tag lets commands return WRONGTYPE for the wrong kind of key.
# No lock is needed: only the event loop thread touches this dict.
store: dict[bytes, tuple[str, object]] = {}

# Keys that have a TTL, mapped to their deadline in ms since the epoch.
# A key that is not in here never expires.
expires: dict[bytes, int] = {}