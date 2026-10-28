"""Helpers that turn raw command arguments (bytes) into Python values."""
from typing import List

from mini_redis.commands.errors import CommandError
from mini_redis.resp import Value

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
INT64_MAX_DIGITS = 19

NOT_AN_INTEGER = "ERR value is not an integer or out of range"


def parse_int(raw: bytes) -> int:
    """Parse an argument as a 64-bit signed integer, as strictly as Redis.

    Accepts an optional "-" and then digits, with no leading zeros.
    Python's int() alone is too loose: it also accepts " 10", "+10", "010",
    "1_0" and numbers of any size.
    """
    # 1. Remember the sign, then look at the digits only.
    negative = raw.startswith(b"-")
    digits = raw[1:] if negative else raw

    # 2. Only ASCII digits, at least one. This also rejects b"" and b"-".
    if not digits.isdigit():
        raise CommandError(NOT_AN_INTEGER)

    # 3. No leading zeros ("010"), and no "-0". A plain "0" is fine.
    if digits.startswith(b"0") and (len(digits) > 1 or negative):
        raise CommandError(NOT_AN_INTEGER)

    # 4. Too many digits to fit. Checked before int(), which raises
    #    ValueError (not CommandError) on strings over 4,300 digits.
    if len(digits) > INT64_MAX_DIGITS:
        raise CommandError(NOT_AN_INTEGER)

    # 5. Safe to convert now: every check above passed.
    value = int(digits)
    if negative:
        value = -value

    # 6. Must fit in a 64-bit signed integer, like a C long long in Redis.
    if not INT64_MIN <= value <= INT64_MAX:
        raise CommandError(NOT_AN_INTEGER)

    return value


EXPIRE_OPTIONS = {b"NX", b"XX", b"GT", b"LT"}

# Helpers
def parse_expire_options(args: List[Value]) -> set[bytes]:
    """Read NX / XX / GT / LT, rejecting unknown or conflicting options."""
    options = set()
    for arg in args:
        option = arg.bulk.upper()
        if option not in EXPIRE_OPTIONS:
            name = arg.bulk.decode(errors="replace")
            raise CommandError(f"ERR Unsupported option {name}")
        options.add(option)

    if b"NX" in options and len(options) > 1:
        raise CommandError(
            "ERR NX and XX, GT or LT options at the same time are not compatible"
        )
    if b"GT" in options and b"LT" in options:
        raise CommandError("ERR GT and LT options at the same time are not compatible")
    return options

def expire_allowed(options: set[bytes], current: int | None, deadline: int) -> bool:
    """Do the options allow replacing the `current` deadline with `deadline`?

    `current` is None when the key has no TTL. Such a key lives forever, so
    it counts as "infinite": GT can never beat it, LT always does.
    """
    if b"NX" in options:
        return current is None
    if b"XX" in options and current is None:
        return False
    if b"GT" in options:
        return current is not None and deadline > current
    if b"LT" in options:
        return current is None or deadline < current
    return True