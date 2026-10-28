"""Tests for argument parsing helpers.

Expected results come from real Redis 8 (EXPIRE / INCR with each value).

Run from the project root:
    python -m unittest tests.test_args -v
"""

import unittest

from mini_redis.commands.args import parse_int
from mini_redis.commands.errors import CommandError


class TestParseInt(unittest.TestCase):
    def test_valid(self):
        cases = {
            b"10": 10,
            b"0": 0,
            b"-1": -1,
            b"9223372036854775807": 2**63 - 1,
            b"-9223372036854775808": -(2**63),
        }
        for raw, want in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(parse_int(raw), want)

    def test_invalid(self):
        cases = [
            b"",
            b"-",
            b"+10",
            b" 10",
            b"10 ",
            b"010",
            b"-0",
            b"1_0",
            b"1.5",
            b"abc",
            b"\xd9\xa1",  # Arabic-Indic digit one: not ASCII
            b"9223372036854775808",  # 2^63, one too big
            b"-9223372036854775809",  # one too small
            b"9" * 5000,  # longer than int() allows
        ]
        for raw in cases:
            with self.subTest(raw=raw[:30]):
                with self.assertRaises(CommandError) as ctx:
                    parse_int(raw)
                self.assertEqual(
                    str(ctx.exception), "ERR value is not an integer or out of range"
                )


if __name__ == "__main__":
    unittest.main()
