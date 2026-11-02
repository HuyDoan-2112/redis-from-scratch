"""Helpers that turn raw command arguments (bytes) into Python values."""
from dataclasses import dataclass
from typing import List

from mini_redis.commands.errors import CommandError
from mini_redis.resp import Value
from mini_redis.storage.keyspace import now_ms

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


SYNTAX_ERROR = "ERR syntax error"
INVALID_SET_EXPIRE = "ERR invalid expire time in 'set' command"

# SET's expiration options that take a number, and its unit.
SET_TIME_OPTIONS = {b"EX", b"PX", b"EXAT", b"PXAT"}
SET_TIME_IN_SECONDS = {b"EX", b"EXAT"}
SET_TIME_RELATIVE = {b"EX", b"PX"}


@dataclass
class SetOptions:
    """What SET's options asked for, after parse_set_options checked them."""
    condition: bytes | None = None   # b"NX", b"XX", or None for "always set"
    get: bool = False                # reply with the old value instead of OK
    expire_at_ms: int | None = None  # absolute deadline in ms, from EX/PX/EXAT/PXAT
    keepttl: bool = False            # leave the key's current TTL alone


def parse_set_options(args: List[Value]) -> SetOptions:
    """Read SET's options: [NX | XX] [GET] [EX | PX | EXAT | PXAT | KEEPTTL].

    Behaves like Redis 8:
    - Two different options from one group (NX XX, EX 10 KEEPTTL) are a
      syntax error, but the same option twice is fine and the last one wins.
    - Every syntax error is reported before a bad number, so the number
      is only checked after the loop has read all the words.
    """
    opts = SetOptions()
    expire = None    # the expiration option seen: EX, PX, EXAT, PXAT or KEEPTTL
    raw_time = b""   # its number, still unparsed

    i = 0
    while i < len(args):
        word = args[i].bulk.upper()

        if word in (b"NX", b"XX"):
            if opts.condition not in (None, word):
                raise CommandError(SYNTAX_ERROR)
            opts.condition = word

        elif word == b"GET":
            opts.get = True

        elif word == b"KEEPTTL" or word in SET_TIME_OPTIONS:
            if expire not in (None, word):
                raise CommandError(SYNTAX_ERROR)
            expire = word
            if word in SET_TIME_OPTIONS:
                if i + 1 >= len(args):  # "SET k v EX" with no number
                    raise CommandError(SYNTAX_ERROR)
                raw_time = args[i + 1].bulk
                i += 1  # skip the number: it is not an option

        else:
            raise CommandError(SYNTAX_ERROR)
        i += 1

    opts.keepttl = expire == b"KEEPTTL"
    if expire in SET_TIME_OPTIONS:
        opts.expire_at_ms = set_deadline_ms(expire, parse_int(raw_time))
    return opts


def set_deadline_ms(option: bytes, n: int) -> int:
    """Turn EX / PX / EXAT / PXAT n into an absolute deadline in ms.

    The deadline may already be in the past (EXAT 1). That is valid:
    SET still replies OK, and the key expires right away.
    """
    if n <= 0:
        raise CommandError(INVALID_SET_EXPIRE)

    ms = n * 1000 if option in SET_TIME_IN_SECONDS else n
    if option in SET_TIME_RELATIVE:
        ms += now_ms()

    # Python ints never overflow, so check the int64 limit by hand.
    if ms > INT64_MAX:
        raise CommandError(INVALID_SET_EXPIRE)
    return ms
