import time

from mini_redis.storage.keyspace import delete_key, expires, sample_ttl_keys

def active_expire_cycle(now: int, time_limit: float) -> int:
    """Delete expired keys, up to time_limit seconds. Returns how many were deleted."""
    start = time.monotonic()
    iteration = 0
    total = 0
    while True:
        expired = 0
        keys = sample_ttl_keys(20)
        if not keys:
            break
        for key in keys:
            if expires[key] <= now:
                delete_key(key)
                expired += 1
        total += expired
        iteration += 1
        if iteration % 16 == 0 and (time.monotonic() - start) > time_limit:
            break
        if expired <= 5:
            break
        
    return total

