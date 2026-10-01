"""State for one connected client.

TODO: Track its socket, input buffer, output buffer, and connection state.
Preserve incomplete requests and unsent reply bytes between events.
Keep every client's buffers separate.
"""
from typing import Any, List, Optional
from dataclasses import dataclass, field
from enum import StrEnum

# Define constants that represent each type of RESP data.
class RESPType(StrEnum):
    """RESP data types."""

    SIMPLE_STRING = "+"
    ERROR = "-"
    INTEGER = ":"
    BULK_STRING = "$"
    ARRAY = "*"

# Define a dataclass to represent a value in the RESP protocol.
@dataclass
class Value:
    typ: str = ""
    string: str = ""
    num: int = 0
    bulk: str = ""
    array: List["Value"] = field(default_factory=list) # Default factory gives a new empty list for each instance

