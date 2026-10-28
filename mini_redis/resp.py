from typing import List, BinaryIO
from dataclasses import dataclass, field
from enum import Enum

# Define constants that represent each type of RESP data.
class RESPType(str, Enum):
    """RESP data types."""

    SIMPLE_STRING = "+"
    ERROR = "-"
    INTEGER = ":"
    BULK_STRING = "$"
    ARRAY = "*"
    NULL = "null"  # Not a real prefix: RESP2 null is written as a null bulk string "$-1\r\n"

CRLF = b"\r\n"
# Limits that stop a bad or hostile client from making us buffer forever.
MAX_LINE = 64*1024  # longest header line, e.g. "*3" or "$5"
MAX_BULK = 512*1024*1024  # largest bulk string, the same 512 MB limit as Redis


class ProtocolError(ValueError):
    """Bytes that break the rules for a client command.

    The message matches Redis, and the server sends it to the client as
    "-ERR Protocol error: <message>".
    """
    
# Define a dataclass to represent a value in the RESP protocol.
@dataclass
class Value:
    """One RESP value. `typ` decides which of the other fields is used:

    SIMPLE_STRING and ERROR use `string`, INTEGER uses `num`,
    BULK_STRING uses `bulk`, ARRAY uses `array`, NULL uses none.
    """

    typ: str = ""
    string: str = ""
    num: int = 0
    bulk: bytes = b""
    array: List["Value"] = field(default_factory=list) # Default factory gives a new empty list for each instance

    def marshal(self) -> bytes:
        """Convert this Value into RESP bytes ready to send over the socket."""
        if self.typ == RESPType.ARRAY:
            return self._marshal_array()
        if self.typ == RESPType.BULK_STRING:
            return self._marshal_bulk()
        if self.typ == RESPType.SIMPLE_STRING:
            return self._marshal_string()
        if self.typ == RESPType.INTEGER:
            return self._marshal_integer()
        if self.typ == RESPType.NULL:
            return self._marshal_null()
        if self.typ == RESPType.ERROR:
            return self._marshal_error()
        return b""

    def _marshal_string(self) -> bytes:
        # +OK\r\n
        return b"+" + self.string.encode() + CRLF

    def _marshal_error(self) -> bytes:
        # -ERR something\r\n
        return b"-" + self.string.encode() + CRLF

    def _marshal_integer(self) -> bytes:
        # :42\r\n
        return b":" + str(self.num).encode() + CRLF

    def _marshal_bulk(self) -> bytes:
        # $5\r\nhello\r\n
        # The length counts bytes, not characters ("é" is 2 bytes in UTF-8).
        data = self.bulk
        return b"$" + str(len(data)).encode() + CRLF + data + CRLF

    def _marshal_array(self) -> bytes:
        # *2\r\n<item 1><item 2>
        out = bytearray(b"*" + str(len(self.array)).encode() + CRLF)
        for item in self.array:
            out += item.marshal()  # Each item marshals itself
        return bytes(out)

    def _marshal_null(self) -> bytes:
        return b"$-1\r\n"


class RESPReader(object):
    """
    A class for reading and parsing Redis RESP commands from clients
    efficiently and in real-time.
    """

    def __init__(self, reader: BinaryIO):
        self.reader = reader

    def _read_exact(self, size: int) -> bytes:
        """
        Read a specific number of bytes from the stream.

        Args:
            size (int): The number of bytes to read.
        """
        data = bytearray() # Create a mutable bytearray to accumulate the read byte
        
        while len(data) < size: # Keep reading until we have the requested number of bytes
            # request only the bytes we still need to read
            chunk = self.reader.read(size - len(data))
            if not chunk:
                raise EOFError("Unexpected end of stream")
            data.extend(chunk) # Add the newly read chunk to the accumulated data

        return bytes(data) # Convert the bytearray to an immutable bytes object before returning it

    def _read_line(self) -> tuple[bytes, int]:
        """
        Read a line from the stream, terminated by CRLF (\r\n).

        Returns:
            tuple[bytes, int]: A tuple containing the line read (without CRLF) and the number of bytes read.
        """
        line = self.reader.readline(MAX_LINE) # Read a line with a maximum length of 64KB

        if not line.endswith(b"\n"):
            if len(line) >= MAX_LINE:
                raise ValueError("Line too long")
            raise EOFError("Unexpected end of stream")
        if not line.endswith(b"\r\n"):
            raise ValueError("Line does not end with CRLF")

        return line[:-2], len(line) # Return the line without CRLF and the total number of bytes read
    def _read_int_line(self) -> int:
        """Read a CRLF-terminated integer payload from the stream.""" 
        line, _ = self._read_line()
        return int(line)

    def _read_integer(self) -> Value:
        """
        Read a RESP integer from the stream, terminated by CRLF (\r\n).

        Returns:
            Value: The parsed RESP integer value.
        """
        return Value(typ=RESPType.INTEGER, num=self._read_int_line()) # Return a Value object representing the RESP integer

    def read(self) -> Value:
        """
        Read a RESP value from the stream.

        Returns:
            Value: The parsed RESP value.
        """
        resp_type = self.reader.read(1) # Read the first byte to determine the RESP type

        if resp_type == b"*":
            return self._read_array() 
        if resp_type == b"$":
            return self._read_bulk_string() 
        if resp_type == b":":
            return self._read_integer() 
        if resp_type == b"+":
            return self._read_simple_string() 
        if resp_type == b"":
            raise EOFError("End of stream")
        
        raise ValueError(f"Unsupported RESP type: {resp_type}") # Raise an error for unsupported types

    def read_command(self) -> Value:
        """
        Read one client command: an array whose items are all bulk strings.

        read() accepts any RESP value, but a command must be only bulk
        strings. Otherwise `*2 $3 DEL :5` would reach DEL as an integer
        Value, whose .bulk is b"", and delete the key "".
        """
        resp_type = self.reader.read(1)
        if resp_type == b"":
            raise EOFError("End of stream")
        if resp_type != b"*":
            # Redis also accepts plain-text "inline" commands (PING\r\n).
            # Not supported yet.
            raise ValueError(f"Unsupported RESP type: {resp_type}")

        length = self._read_int_line()
        if length < 0:
            raise ValueError("Null array length is not supported")

        array = []
        for _ in range(length):
            item_type = self.reader.read(1)
            if item_type == b"":
                raise EOFError("Unexpected end of stream")
            if item_type != b"$":
                got = item_type.decode(errors="replace")
                raise ProtocolError(f"expected '$', got '{got}'")
            array.append(self._read_bulk_string())

        return Value(typ=RESPType.ARRAY, array=array)


    def _read_array(self) -> Value:
        """
        Read a RESP array from the stream.

        Returns:
            Value: The parsed RESP array.
        """
        length = self._read_int_line() # Read the length of the array
        if length < 0:
            raise ValueError("Null array length is not supported") # Raise an error for negative lengths

        array = [] # Initialize an empty list to hold the array elements

        for _ in range(length): # Read each element of the array
            value = self.read() # Read a RESP value
            array.append(value) # Add the value to the array

        return Value(typ=RESPType.ARRAY, array=array) # Return a Value object representing the RESP array

    def _read_simple_string(self) -> Value:
        """Read a RESP simple string from the stream."""
        line, _ = self._read_line()
        return Value(typ=RESPType.SIMPLE_STRING, string=line.decode())

    def _read_bulk_string(self) -> Value:
        """
        Read a RESP bulk string from the stream.

        Returns:
            Value: The parsed RESP bulk string.
        """
        length = self._read_int_line() # Read the length of the bulk string

        if length < -1 or length > MAX_BULK:
            raise ValueError("Invalid bulk length")
        if length == -1: # A length of -1 indicates a null bulk string
            raise ValueError("Null bulk strings are not supported")

        bulk = self._read_exact(length) # Read the exact number of bytes for the bulk string

        if self._read_exact(2) != b"\r\n": # Ensure the bulk string is terminated by CRLF
            raise ValueError("Bulk string not terminated with CRLF")

        return Value(typ=RESPType.BULK_STRING, bulk=bulk) # Return a Value object representing the RESP bulk string
