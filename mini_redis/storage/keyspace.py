import threading

store_lock = threading.Lock()  # A lock to ensure thread-safe access to the store.
store: dict[bytes, tuple[str, object]] = {} # A global dictionary to store key-value pairs, where the value is a tuple of (value, expiration_time).