"""Tests for the replication backlog: a ring buffer of the last `size` bytes of
the replication stream, read back from a byte number for partial resyncs.

The examples use an 8-byte backlog, the same ones worked through by hand when
the class was designed. The random tests compare every read with the whole
stream kept in full.

Run from the project root:
    python3 -m unittest tests.test_backlog -v
"""

import random
import unittest

from mini_redis.replication.backlog import Backlog


def fed(*chunks: bytes, size: int = 8, offset: int = 0) -> Backlog:
    backlog = Backlog(size, offset)
    for chunk in chunks:
        backlog.feed(chunk)
    return backlog


class TestBacklogState(unittest.TestCase):
    def assertState(self, backlog: Backlog, buf: bytes, idx: int, offset: int, histlen: int) -> None:
        self.assertEqual(
            (bytes(backlog.buf), backlog.idx, backlog.offset, backlog.histlen),
            (buf, idx, offset, histlen),
        )

    def test_new_backlog_is_empty(self):
        backlog = Backlog(8)
        self.assertState(backlog, bytes(8), 0, 0, 0)
        self.assertEqual(backlog.first_byte_offset, 1)

    def test_feed_that_fits(self):
        backlog = fed(b"ABCDE")
        self.assertState(backlog, b"ABCDE\0\0\0", 5, 5, 5)
        self.assertEqual(backlog.first_byte_offset, 1)

    def test_feed_that_wraps_overwrites_the_oldest_bytes(self):
        backlog = fed(b"ABCDE", b"FGHIJ")
        # F G H fill seats 5 to 7, then I J wrap to seats 0 and 1 over A B.
        self.assertState(backlog, b"IJCDEFGH", 2, 10, 8)
        self.assertEqual(backlog.first_byte_offset, 3)

    def test_feed_of_exactly_size_bytes(self):
        backlog = fed(b"ABC", b"12345678")
        self.assertEqual(backlog.read_from(4), b"12345678")
        self.assertIsNone(backlog.read_from(3))

    def test_feed_larger_than_the_backlog_keeps_the_last_size_bytes(self):
        backlog = fed(b"ABCDE", b"FGHIJ", b"KLMNOPQRSTUVWXYZ12")
        # The offset grows by all 18 bytes, even the 10 that were not kept.
        self.assertState(backlog, b"UVWXYZ12", 0, 28, 8)
        self.assertEqual(backlog.first_byte_offset, 21)
        self.assertEqual(backlog.read_from(21), b"UVWXYZ12")
        self.assertIsNone(backlog.read_from(20))

    def test_feed_after_a_large_feed_continues_from_seat_0(self):
        backlog = fed(b"ABCDE", b"FGHIJ", b"KLMNOPQRSTUVWXYZ12", b"!?")
        self.assertState(backlog, b"!?WXYZ12", 2, 30, 8)
        self.assertEqual(backlog.read_from(27), b"12!?")

    def test_empty_feed_changes_nothing(self):
        backlog = fed(b"ABCDE", b"")
        self.assertState(backlog, b"ABCDE\0\0\0", 5, 5, 5)

    def test_feed_accepts_bytearray_and_memoryview(self):
        backlog = fed(bytearray(b"ABC"), memoryview(b"DEF"))
        self.assertEqual(backlog.read_from(1), b"ABCDEF")

    def test_size_must_be_at_least_1(self):
        for size in (0, -1):
            with self.subTest(size=size):
                with self.assertRaisesRegex(ValueError, "at least 1"):
                    Backlog(size)


class TestReadFrom(unittest.TestCase):
    def setUp(self):
        # Holds bytes 3 to 10: C D E F G H I J. Bytes 1 and 2 were overwritten.
        self.backlog = fed(b"ABCDE", b"FGHIJ")

    def test_every_offset(self):
        cases = {
            2: None,             # overwritten by J
            3: b"CDEFGHIJ",      # oldest byte held
            6: b"FGHIJ",         # wraps: two slices
            9: b"IJ",            # one slice at the front
            10: b"J",            # newest byte only
            11: b"",             # the replica already has everything
            12: None,            # never fed
        }
        for offset, want in cases.items():
            with self.subTest(offset=offset):
                self.assertEqual(self.backlog.read_from(offset), want)

    def test_empty_backlog_accepts_only_the_next_byte(self):
        backlog = Backlog(8)
        self.assertEqual(backlog.read_from(1), b"")
        self.assertIsNone(backlog.read_from(0))
        self.assertIsNone(backlog.read_from(2))

    def test_starting_offset(self):
        """A backlog created when the stream is already at byte 1000, as after
        REPLICAOF NO ONE, numbers its first byte 1001."""
        backlog = Backlog(8, offset=1000)
        self.assertEqual(backlog.first_byte_offset, 1001)
        self.assertEqual(backlog.read_from(1001), b"")
        self.assertIsNone(backlog.read_from(1000))
        backlog.feed(b"abc")
        self.assertEqual(backlog.read_from(1001), b"abc")
        self.assertEqual(backlog.read_from(1003), b"c")

    def test_result_is_a_copy(self):
        """Later feeds overwrite seats, but bytes already read must not change.
        Checked for both the one-slice and the two-slice return."""
        one_slice = self.backlog.read_from(9)
        two_slices = self.backlog.read_from(6)
        self.backlog.feed(b"XYZXYZXYZ")
        self.assertEqual((one_slice, two_slices), (b"IJ", b"FGHIJ"))
        for got in (one_slice, two_slices):
            self.assertIs(type(got), bytes)


class TestAgainstWholeStream(unittest.TestCase):
    def test_random_feeds(self):
        """Feed random chunks and keep the whole stream next to the backlog.
        After every feed, each offset held must read back the tail of the
        stream, and the offsets just outside must read None."""
        rng = random.Random(1)
        for trial in range(500):
            size = rng.randint(1, 40)
            start = rng.choice([0, rng.randint(1, 10**12)])
            backlog = Backlog(size, start)
            whole = bytearray()
            for _ in range(rng.randint(1, 20)):
                n = rng.choice([0, 1, size - 1, size, size + 1, rng.randint(1, 3 * size)])
                chunk = rng.randbytes(n)
                backlog.feed(chunk)
                whole += chunk

                end = start + len(whole)
                held = min(size, len(whole))
                self.assertEqual(backlog.offset, end)
                self.assertEqual(backlog.first_byte_offset, end - held + 1)
                for offset in range(end - held - 1, end + 3):
                    if end - held + 1 <= offset <= end + 1:
                        want = bytes(whole[offset - start - 1:])
                    else:
                        want = None
                    self.assertEqual(
                        backlog.read_from(offset), want,
                        f"trial {trial}: size {size}, start {start}, offset {offset}",
                    )


if __name__ == "__main__":
    unittest.main()
