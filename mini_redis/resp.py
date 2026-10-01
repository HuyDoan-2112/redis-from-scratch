from typing import Any, List, Optional, BinaryIO
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
        line = bytearray() # Create a mutable bytearray to accumulate the read bytes

        while True:
            line.extend(self.reader.read(1)) # Read one byte at a time and add it to the line
            if line.endswith(b"\r\n"): # Check if the line ends with CRLF
                return bytes(line[:-2]), len(line) # Return the line without CRLF and the number of bytes read

    def _read_integer(self) -> tuple[int, int]:
        """
        Read an integer from the stream, terminated by CRLF (\r\n).

        Returns:
            tuple[int, int]: A tuple containing the integer read and the number of bytes read.
        """
        line, bytes_read = self._read_line() # Read a line from the stream
        return int(line), bytes_read # Convert the line to an integer and return it along with the number of bytes read

    def _read(self) -> Value:
        """
        Read a RESP value from the stream.

        Returns:
            Value: The parsed RESP value.
        """
        resp_type = self.reader.read(1) # Read the first byte to determine the RESP type

        if resp_type == b"*":
            return self._read_array() # If it's an array, call the _read_array method
        if resp_type == b"$":
            return self._read_bulk_string() # If it's a bulk string, call the _read_bulk_string method
        if resp_type == b":":
            return self._read_integer() # If it's an integer, call the _read_integer method
        if resp_type == b"+":
            return self._read_simple_string() # If it's a simple string, call the _read_simple_string method

        raise ValueError(f"Unsupported RESP type: {resp_type}") # Raise an error for unsupported types


    def _read_array(self) -> Value:
        """
        Read a RESP array from the stream.

        Returns:
            Value: The parsed RESP array.
        """
        length, bytes_read = self._read_integer() # Read the length of the array
        if length < 0:
            raise ValueError("Null array length is not supported") # Raise an error for negative lengths

        array = [] # Initialize an empty list to hold the array elements

        for _ in range(length): # Read each element of the array
            value = self._read() # Read a RESP value
            array.append(value) # Add the value to the array

        return Value(typ=RESPType.ARRAY, array=array) # Return a Value object representing the RESP array

    def _read_bulk_string(self) -> Value:
        """
        Read a RESP bulk string from the stream.

        Returns:
            Value: The parsed RESP bulk string.
        """
        length, bytes_read = self._read_integer() # Read the length of the bulk string

        if length == -1: # A length of -1 indicates a null bulk string
            raise ValueError("Null bulk strings are not supported")

        bulk = self._read_exact(length) # Read the exact number of bytes for the bulk string

        if self._read_exact(2) != b"\r\n": # Ensure the bulk string is terminated by CRLF
            raise ValueError("Bulk string not terminated with CRLF")

        return Value(typ=RESPType.BULK_STRING, bulk=bulk.decode()) # Return a Value object representing the RESP bulk string


class RESPWriter(object):
