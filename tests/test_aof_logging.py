"""What each command writes to the AOF.

A change is logged as a command that gives the same result whenever it is
replayed: relative times become absolute deadlines, and commands that change
nothing log nothing. These run commands through execute(), as the server does
for a client, then read the AOF file back.

Run from the project root:
    python -m unittest tests.test_aof_logging -v
"""

import tempfile
import time
import unittest
from pathlib import Path

from mini_redis.config import AOFConfig
from mini_redis.persistence.aof import AOF
from mini_redis.server import execute
from tests.helpers import make_cmd, replay, reset_keyspace

FAR_FUTURE_MS = 4102444800000  # 2100-01-01, a deadline that never passes during a test


def now_ms() -> int:
    return int(time.time() * 1000)


class TestAOFLogging(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / "appendonly.aof"
        reset_keyspace()
        self.addCleanup(reset_keyspace)
        self.aof = AOF(AOFConfig(path=self.path, enabled=True, appendfsync="no"))
        self.addCleanup(self.aof.close)

    def do(self, *parts: str):
        """Run one command and return its reply, as RESP bytes."""
        return execute(make_cmd(*parts), self.aof).marshal()

    def logged(self) -> list[list[str]]:
        """Every command in the AOF so far."""
        self.aof.flush()
        return replay(self.path)

    def logged_by(self, *parts: str) -> list[list[str]]:
        """Only the commands that this one command added to the AOF."""
        before = len(self.logged())
        self.do(*parts)
        return self.logged()[before:]

    # SET ------------------------------------------------------------------

    def test_set(self):
        self.assertEqual(self.logged_by("SET", "k", "v"), [["SET", "k", "v"]])

    def test_relative_expire_is_logged_as_an_absolute_deadline(self):
        for unit, amount, ms in (("EX", "100", 100_000), ("PX", "100000", 100_000)):
            with self.subTest(option=unit):
                before = now_ms()
                log = self.logged_by("SET", "k", "v", unit, amount)
                after = now_ms()

                self.assertEqual(len(log), 1)
                self.assertEqual(log[0][:4], ["SET", "k", "v", "PXAT"])
                self.assertTrue(before + ms <= int(log[0][4]) <= after + ms, log[0])

    def test_exat_is_logged_as_pxat(self):
        self.assertEqual(
            self.logged_by("SET", "k", "v", "EXAT", "4102444800"),
            [["SET", "k", "v", "PXAT", "4102444800000"]],
        )

    def test_keepttl(self):
        self.do("SET", "k", "v", "PXAT", str(FAR_FUTURE_MS))
        self.assertEqual(self.logged_by("SET", "k", "v2", "KEEPTTL"), [["SET", "k", "v2", "KEEPTTL"]])

    def test_set_with_a_deadline_in_the_past_logs_del(self):
        self.do("SET", "k", "old")
        self.assertEqual(self.logged_by("SET", "k", "v", "PXAT", "1"), [["DEL", "k"]])
        self.assertEqual(self.do("EXISTS", "k"), b":0\r\n")

    def test_set_that_does_not_write_logs_nothing(self):
        self.do("SET", "exists", "v")
        self.assertEqual(self.logged_by("SET", "exists", "v2", "NX"), [])
        self.assertEqual(self.logged_by("SET", "missing", "v", "XX"), [])

    # Expiry ---------------------------------------------------------------

    def test_expire_is_logged_as_pexpireat(self):
        self.do("SET", "k", "v")
        before = now_ms()
        log = self.logged_by("EXPIRE", "k", "100")
        after = now_ms()

        self.assertEqual(len(log), 1)
        self.assertEqual(log[0][:2], ["PEXPIREAT", "k"])
        self.assertTrue(before + 100_000 <= int(log[0][2]) <= after + 100_000, log[0])

    def test_pexpireat_is_logged_as_sent(self):
        self.do("SET", "k", "v")
        self.assertEqual(
            self.logged_by("PEXPIREAT", "k", str(FAR_FUTURE_MS)),
            [["PEXPIREAT", "k", str(FAR_FUTURE_MS)]],
        )

    def test_expire_in_the_past_logs_del(self):
        self.do("SET", "k", "v")
        self.assertEqual(self.logged_by("EXPIRE", "k", "-1"), [["DEL", "k"]])
        self.assertEqual(self.do("EXISTS", "k"), b":0\r\n")

    def test_expire_that_does_not_apply_logs_nothing(self):
        self.do("SET", "k", "v", "PXAT", str(FAR_FUTURE_MS))
        self.assertEqual(self.logged_by("EXPIRE", "missing", "100"), [])
        self.assertEqual(self.logged_by("EXPIRE", "k", "100", "NX"), [])  # k already has a TTL
        self.assertEqual(self.logged_by("EXPIRE", "k", "100", "GT"), [])  # 100 s is sooner

    def test_reading_an_expired_key_logs_del(self):
        self.do("SET", "k", "v", "PX", "20")
        time.sleep(0.05)
        self.assertEqual(self.logged_by("GET", "k"), [["DEL", "k"]])

    # Other writes ---------------------------------------------------------

    def test_del_logs_only_the_keys_that_existed(self):
        self.do("SET", "a", "1")
        self.do("SET", "b", "2")
        self.assertEqual(self.logged_by("DEL", "a", "missing", "b"), [["DEL", "a", "b"]])
        self.assertEqual(self.logged_by("DEL", "missing"), [])

    def test_incr(self):
        self.assertEqual(self.logged_by("INCR", "n"), [["INCR", "n"]])

    def test_hset(self):
        self.assertEqual(self.logged_by("HSET", "h", "f", "v"), [["HSET", "h", "f", "v"]])

    def test_failed_writes_log_nothing(self):
        self.do("HSET", "h", "f", "v")
        self.do("SET", "s", "not a number")
        self.assertEqual(self.logged_by("INCR", "h"), [])  # WRONGTYPE
        self.assertEqual(self.logged_by("INCR", "s"), [])  # not an integer
        self.assertEqual(self.logged_by("HSET", "s", "f", "v"), [])  # WRONGTYPE
        self.assertEqual(self.logged_by("SET", "k", "v", "EX", "0"), [])  # invalid expire time

    def test_reads_log_nothing(self):
        self.do("SET", "k", "v", "PXAT", str(FAR_FUTURE_MS))
        self.do("HSET", "h", "f", "v")
        before = self.logged()
        for command in (("GET", "k"), ("HGET", "h", "f"), ("EXISTS", "k"), ("TTL", "k"),
                        ("DBSIZE",), ("PING",), ("ECHO", "hi")):
            self.do(*command)
        self.assertEqual(self.logged(), before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
