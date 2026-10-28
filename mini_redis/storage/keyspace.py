
# Every key in the database. Each value is a (type, data) tuple:
#   ("string", bytes) or ("hash", dict[bytes, bytes]).
# The type tag lets commands return WRONGTYPE for the wrong kind of key.
# No lock is needed: only the event loop thread touches this dict.
store: dict[bytes, tuple[str, object]] = {}