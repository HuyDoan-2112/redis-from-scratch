"""INFO: server state as "field:value" lines, grouped in sections."""
from typing import Callable, List

from mini_redis.replication import master
from mini_redis.resp import RESPType, Value

# One function per section, in the order INFO prints them. Each returns the
# section's "field:value" lines. Step 4 adds "stats".
SECTIONS: dict[str, Callable[[], list[str]]] = {
    "replication": lambda: master.current.info() if master.current is not None else [],
}

# Names that ask for every section.
ALL_SECTIONS = ("all", "default", "everything")

def info_cmd(args: List[Value]) -> Value:
    """INFO [section ...]: server state as "field:value" lines.

    No argument, or all/default/everything, gives every section. Otherwise the
    named sections, case-insensitive and each once, in the usual order. Unknown
    names are skipped, so INFO foo is an empty string, as in Redis.
    """
    names = {arg.bulk.decode(errors="replace").lower() for arg in args}
    if not names or names & set(ALL_SECTIONS):
        wanted = list(SECTIONS)
    else:
        wanted = [name for name in SECTIONS if name in names]

    # Every line ends with CRLF, and a blank line separates sections.
    blocks = [
        "".join(f"{line}\r\n" for line in [f"# {name.capitalize()}", *SECTIONS[name]()])
        for name in wanted
    ]
    return Value(typ=RESPType.BULK_STRING, bulk="\r\n".join(blocks).encode())
