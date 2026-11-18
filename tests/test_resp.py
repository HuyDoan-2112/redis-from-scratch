"""Tests for the two command parsers in resp.py.

RESPReader reads the AOF file. CommandParser reads client sockets, where a
command can arrive in many pieces. Both must accept and reject the same bytes,
so every input here goes through both.

Run from the project root:
    python -m unittest tests.test_resp -v
"""

import io
import unittest

from mini_redis.resp import MAX_BULK, MAX_LINE, CommandParser, ProtocolError, RESPReader
from tests.helpers import cmd_bytes


def bulk_cmd(*parts: bytes) -> bytes:
    """RESP bytes of a command whose arguments can be any bytes."""
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        out += b"$%d\r\n%s\r\n" % (len(p), p)
    return out


def parse_in_chunks(data: bytes, size: int) -> list[list[bytes]]:
    """CommandParser: feed `data` `size` bytes at a time, collect every command."""
    parser = CommandParser()
    commands = []
    for i in range(0, len(data), size):
        parser.feed(data[i:i + size])
        while (cmd := parser.next_command()) is not None:
            commands.append([item.bulk for item in cmd.array])
    return commands


def read_all(data: bytes) -> list[list[bytes]]:
    """RESPReader: read every complete command from `data`."""
    reader = RESPReader(io.BytesIO(data))
    commands = []
    while True:
        try:
            cmd = reader.read_command()
        except EOFError:
            return commands
        commands.append([item.bulk for item in cmd.array])


# Values with CRLF, a lone LF, "$" and "*" inside, an empty argument, and a
# value longer than one 16 KB recv().
PIPELINE = [
    [b"SET", b"k", b"v"],
    [b"SET", b"crlf", b"a\r\nb\nc\r"],
    [b"SET", b"", b""],
    [b"SET", b"marks", b"*2\r\n$3\r\n"],
    [b"SET", b"big", bytes(range(256)) * 100],
    [b"GET", b"big"],
]
PIPELINE_BYTES = b"".join(bulk_cmd(*c) for c in PIPELINE)


class TestSplitInput(unittest.TestCase):
    def test_every_split_gives_the_same_commands(self):
        """Feeding one byte at a time hits every place a command can be cut."""
        self.assertEqual(read_all(PIPELINE_BYTES), PIPELINE)
        for size in (1, 2, 3, 7, 1024, len(PIPELINE_BYTES)):
            with self.subTest(chunk_size=size):
                self.assertEqual(parse_in_chunks(PIPELINE_BYTES, size), PIPELINE)

    def test_incomplete_command_waits(self):
        data = cmd_bytes("SET", "a", "1")
        for cut in range(len(data)):
            with self.subTest(cut=cut):
                self.assertEqual(read_all(data[:cut]), [])
                parser = CommandParser()
                parser.feed(data[:cut])
                self.assertIsNone(parser.next_command())
                parser.feed(data[cut:])
                self.assertEqual([i.bulk for i in parser.next_command().array], [b"SET", b"a", b"1"])

    def test_parsed_bytes_are_dropped(self):
        """Only the unparsed part of a big value stays in the buffer, not the
        whole command, so memory does not double while it arrives."""
        parser = CommandParser()
        parser.feed(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$10\r\nabc")
        self.assertIsNone(parser.next_command())
        self.assertEqual(bytes(parser.buf), b"abc")

    def test_large_value_in_small_chunks(self):
        value = b"x" * 5_000_000
        self.assertEqual(
            parse_in_chunks(bulk_cmd(b"SET", b"k", value), 1024),
            [[b"SET", b"k", value]],
        )

    def test_empty_array(self):
        self.assertEqual(read_all(b"*0\r\n"), [[]])
        self.assertEqual(parse_in_chunks(b"*0\r\n", 1), [[]])


class TestBadInput(unittest.TestCase):
    def assert_rejected(self, data: bytes, message: str, error=ValueError):
        """Both parsers raise the same error with the same message."""
        with self.assertRaisesRegex(error, message) as from_reader:
            read_all(data)
        with self.assertRaisesRegex(error, message) as from_parser:
            parse_in_chunks(data, len(data))
        self.assertIs(type(from_parser.exception), type(from_reader.exception))
        self.assertEqual(str(from_parser.exception), str(from_reader.exception))

    def test_item_that_is_not_a_bulk_string(self):
        """Rejected as soon as the type byte arrives, as in Redis."""
        for prefix in (b":", b"+", b"*", b"-"):
            with self.subTest(prefix=prefix):
                self.assert_rejected(
                    b"*2\r\n$3\r\nDEL\r\n" + prefix,
                    f"expected '\\$', got '\\{prefix.decode()}'",
                    ProtocolError,
                )

    def test_command_that_is_not_an_array(self):
        self.assert_rejected(b"PING", "Unsupported RESP type")

    def test_bad_lengths(self):
        cases = {
            b"*-1\r\n": "Null array length",
            b"*x\r\n": "invalid literal",
            b"*1\r\n$-1\r\n": "Null bulk strings",
            b"*1\r\n$-2\r\n": "Invalid bulk length",
            b"*1\r\n$%d\r\n" % (MAX_BULK + 1): "Invalid bulk length",
        }
        for data, message in cases.items():
            with self.subTest(data=data):
                self.assert_rejected(data, message)

    def test_line_endings(self):
        self.assert_rejected(b"*1\n", "does not end with CRLF")
        self.assert_rejected(b"*1\r\n$1\r\nab\r\n", "not terminated with CRLF")

    def test_line_too_long(self):
        self.assert_rejected(b"*" + b"1" * MAX_LINE, "Line too long")
        # One byte shorter can still be a valid line: wait for its CRLF.
        almost = b"*" + b"1" * (MAX_LINE - 1)
        self.assertEqual(read_all(almost), [])
        self.assertEqual(parse_in_chunks(almost, len(almost)), [])


class TestBytesUsed(unittest.TestCase):
    """bytes_used is the replica's offset: the bytes of every complete command
    so far, never part of a command that has not fully arrived."""

    SET = bulk_cmd(b"SET", b"a", b"1")  # 27 bytes
    DEL = bulk_cmd(b"DEL", b"b")        # 20 bytes

    def test_new_parser(self):
        self.assertEqual(CommandParser().bytes_used, 0)

    def test_one_command(self):
        parser = CommandParser()
        parser.feed(self.SET)
        parser.next_command()
        self.assertEqual(parser.bytes_used, 27)

    def test_half_command_is_not_counted(self):
        """If the link drops here, the replica must ask for byte 28, the start
        of DEL, not byte 38 in the middle of it."""
        parser = CommandParser()
        parser.feed(self.SET + self.DEL[:10])
        parser.next_command()
        self.assertIsNone(parser.next_command())
        self.assertEqual(parser.bytes_used, 27)
        parser.feed(self.DEL[10:])
        parser.next_command()
        self.assertEqual(parser.bytes_used, 47)

    def test_every_split(self):
        """After each recv-sized chunk, bytes_used is the end of the last
        complete command, however the stream is cut."""
        ends, total = [], 0
        for command in PIPELINE:
            total += len(bulk_cmd(*command))
            ends.append(total)
        for size in (1, 2, 3, 7, 1024, len(PIPELINE_BYTES)):
            with self.subTest(chunk_size=size):
                parser = CommandParser()
                for i in range(0, len(PIPELINE_BYTES), size):
                    parser.feed(PIPELINE_BYTES[i:i + size])
                    while parser.next_command() is not None:
                        pass
                    received = min(i + size, len(PIPELINE_BYTES))
                    complete = [end for end in ends if end <= received]
                    self.assertEqual(parser.bytes_used, complete[-1] if complete else 0)

    def test_counts_the_bytes_received_not_a_re_encoding(self):
        """$03 is accepted for a 3-byte argument. The offset must count the 4
        bytes that arrived, not the 3 that marshal() would write."""
        data = b"*1\r\n$03\r\nDEL\r\n"
        parser = CommandParser()
        parser.feed(data)
        command = parser.next_command()
        self.assertEqual(parser.bytes_used, len(data))
        self.assertEqual(len(command.marshal()), len(data) - 1)

    def test_stream_recorded_from_redis(self):
        """A replication stream from Redis 8.10.3: a ping, then SELECT 0,
        SET b 2 EX 100 and EXPIRE a 100 as Redis propagates them, then two
        pings. INFO replication reported master_repl_offset:168 at the end."""
        stream = (
            b"*1\r\n$4\r\nping\r\n"
            b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"
            b"*5\r\n$3\r\nset\r\n$1\r\nb\r\n$1\r\n2\r\n$4\r\nPXAT\r\n$13\r\n1794960536880\r\n"
            b"*3\r\n$9\r\nPEXPIREAT\r\n$1\r\na\r\n$13\r\n1794960536882\r\n"
            b"*1\r\n$4\r\nping\r\n"
            b"*1\r\n$4\r\nping\r\n"
        )
        parser = CommandParser()
        parser.feed(stream)
        while parser.next_command() is not None:
            pass
        self.assertEqual(parser.bytes_used, 168)


if __name__ == "__main__":
    unittest.main(verbosity=2)
