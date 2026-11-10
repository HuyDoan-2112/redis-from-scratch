"""Tests for RDB snapshots: the file format (dump and load), BGSAVE and
LASTSAVE run in this process, and loading a snapshot into the keyspace.

Run from the project root:
    python3 -m unittest tests.test_rdb -v
"""

import contextlib
import copy
import io
import os
import tempfile
import time
import unittest
import zlib
from pathlib import Path

from mini_redis import propagation
from mini_redis.config import AOFConfig, RDBConfig
from mini_redis.persistence import aof as aof_module, child, rdb as rdb_module
from mini_redis.persistence.aof import AOF
from mini_redis.persistence.rdb import RDB, RDBError, dump, load
from mini_redis.resp import RESPType, Value
from mini_redis.server import cron, execute
from mini_redis.storage import keyspace
from mini_redis.storage.expire import active_expire_cycle
from tests.helpers import capture_fd2, cmd_bytes, make_cmd, reset_keyspace, wait_for_child

FAR_FUTURE_MS = 4102444800000  # 2100-01-01, a deadline that never passes during a test


def to_bytes(store: dict, expires: dict, now_ms: int = 1000) -> bytes:
    out = io.BytesIO()
    dump(out, store, expires, now_ms)
    return out.getvalue()


def from_bytes(data: bytes, now_ms: int = 1000) -> tuple[dict, dict]:
    return load(io.BytesIO(data), now_ms)


def with_crc(body: bytes) -> bytes:
    """body followed by its CRC32, so a test can build a file whose only fault is elsewhere."""
    return body + zlib.crc32(body).to_bytes(4, "little")


# A small file that has every part: a string with a TTL, a hash, and an empty value.
SMALL_STORE = {b"k": ("string", b"v"), b"h": ("hash", {b"f": b"x"}), b"e": ("string", b"")}
SMALL_EXPIRES = {b"k": 9999}


# ---------------------------------------------------------------------------
# File format: dump() and load() only (fast, no fork, no disk)
# ---------------------------------------------------------------------------

class TestExactBytes(unittest.TestCase):
    """These pin the format, so it cannot change by accident."""

    def test_empty_database(self):
        self.assertEqual(to_bytes({}, {}), with_crc(b"MINIRDB\x01\xff"))

    def test_one_string_key(self):
        body = bytes.fromhex(
            "4d494e4952444201"     # "MINIRDB", version 1
            "00"                   # type: string
            "ffffffffffffffff"     # expire: -1, no TTL
            "01000000" "6b"        # key: len 1, "k"
            "01000000" "76"        # value: len 1, "v"
            "ff"                   # EOF
        )
        self.assertEqual(to_bytes({b"k": ("string", b"v")}, {}), with_crc(body))

    def test_hash_with_a_ttl(self):
        body = bytes.fromhex(
            "4d494e4952444201"
            "01"                   # type: hash
            "2823000000000000"     # expire: 9000 ms
            "03000000" "753a31"    # key: len 3, "u:1"
            "01000000"             # 1 field
            "03000000" "616765"    # field: len 3, "age"
            "02000000" "3330"      # value: len 2, "30"
            "ff"
        )
        self.assertEqual(to_bytes({b"u:1": ("hash", {b"age": b"30"})}, {b"u:1": 9000}), with_crc(body))


class TestRoundTrip(unittest.TestCase):
    def assert_round_trip(self, store: dict, expires: dict) -> None:
        self.assertEqual(from_bytes(to_bytes(store, expires)), (store, expires))

    def test_strings_and_hashes_with_and_without_ttl(self):
        self.assert_round_trip(
            {b"s": ("string", b"1"), b"st": ("string", b"2"),
             b"h": ("hash", {b"a": b"1", b"b": b"2"}), b"ht": ("hash", {b"c": b"3"})},
            {b"st": FAR_FUTURE_MS, b"ht": FAR_FUTURE_MS},
        )

    def test_binary_safe(self):
        odd = b"\x00\r\n\xff"
        self.assert_round_trip(
            {odd: ("string", odd), b"": ("string", b""), b"h": ("hash", {odd: odd, b"": b""})},
            {odd: FAR_FUTURE_MS},
        )

    def test_values_larger_than_one_read_chunk(self):
        self.assert_round_trip(
            {b"big": ("string", b"x" * 1_000_000),
             b"wide": ("hash", {b"f%d" % i: b"v" * (i % 50) for i in range(10_000)})},
            {},
        )

    def test_dump_does_not_change_its_inputs(self):
        store = copy.deepcopy(SMALL_STORE)
        expires = {b"k": 9999, b"gone": 1}
        store[b"gone"] = ("string", b"old")
        before = copy.deepcopy((store, expires))

        to_bytes(store, expires)

        self.assertEqual((store, expires), before)

    def test_dump_rejects_a_type_it_does_not_know(self):
        with self.assertRaisesRegex(ValueError, "does not know type 'list'"):
            to_bytes({b"l": ("list", [b"a"])}, {})


class TestExpiry(unittest.TestCase):
    """A key counts as expired when its deadline is at or before now, the same
    rule as keyspace.lookup_key()."""

    def test_dump_leaves_out_expired_keys(self):
        data = to_bytes({b"old": ("string", b"x"), b"new": ("string", b"y")},
                        {b"old": 999, b"new": 1001}, now_ms=1000)

        self.assertEqual(from_bytes(data, now_ms=0), ({b"new": ("string", b"y")}, {b"new": 1001}))

    def test_load_skips_keys_that_expired_after_the_save(self):
        data = to_bytes({b"k": ("string", b"v"), b"keep": ("string", b"w")}, {b"k": 5000}, now_ms=1000)

        self.assertEqual(from_bytes(data, now_ms=6000), ({b"keep": ("string", b"w")}, {}))

    def test_deadline_equal_to_now_counts_as_expired(self):
        store = {b"k": ("string", b"v")}

        self.assertEqual(from_bytes(to_bytes(store, {b"k": 1000}, now_ms=1000), now_ms=0), ({}, {}))
        self.assertEqual(from_bytes(to_bytes(store, {b"k": 2000}, now_ms=1000), now_ms=2000), ({}, {}))


class TestBadFiles(unittest.TestCase):
    """Every bad file raises RDBError. Never a struct.error or IndexError, and
    never half the data."""

    def setUp(self):
        self.good = to_bytes(SMALL_STORE, SMALL_EXPIRES)

    def assert_rejected(self, data: bytes, message: str = "") -> None:
        with self.assertRaisesRegex(RDBError, message):
            from_bytes(data)

    def test_wrong_magic(self):
        self.assert_rejected(b"REDIS" + self.good[5:], "bad magic")

    def test_unsupported_version(self):
        self.assert_rejected(with_crc(b"MINIRDB\x02\xff"), "unsupported RDB version 2")

    def test_file_cut_short_anywhere(self):
        for size in range(len(self.good)):
            with self.subTest(size=size):
                self.assert_rejected(self.good[:size])

    def test_any_flipped_bit(self):
        for pos in range(len(self.good)):
            for bit in range(8):
                with self.subTest(pos=pos, bit=bit):
                    bad = bytearray(self.good)
                    bad[pos] ^= 1 << bit
                    self.assert_rejected(bytes(bad))

    def test_flipped_bit_in_a_value_fails_the_checksum(self):
        pos = self.good.index(b"v")
        bad = self.good[:pos] + b"w" + self.good[pos + 1:]

        self.assert_rejected(bad, "checksum mismatch")

    def test_unknown_type_is_named_even_when_the_checksum_matches(self):
        body = self.good[:-4]
        bad = with_crc(body[:8] + b"\x07" + body[9:])  # byte 8 is the first record's type

        self.assert_rejected(bad, "unknown record type 7")

    def test_extra_bytes_after_the_checksum(self):
        self.assert_rejected(self.good + b"\x00", "extra bytes after checksum")


# ---------------------------------------------------------------------------
# BGSAVE and LASTSAVE in this process: the command forks, cron reaps
# ---------------------------------------------------------------------------

class BGSaveTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.path = self.dir / "dump.rdb"
        reset_keyspace()
        self.addCleanup(reset_keyspace)
        # The child slot is module state: free it even if a test fails mid-save.
        self.addCleanup(child.kill)
        self.rdb = RDB(RDBConfig(path=self.path))
        rdb_module.current = self.rdb
        self.addCleanup(setattr, rdb_module, "current", None)

    def do(self, *commands: str) -> list[Value]:
        return [execute(make_cmd(*command.split()), aof_module.current) for command in commands]

    def bgsave(self) -> Value:
        return self.do("BGSAVE")[0]

    def finish(self) -> str:
        """Wait for the child the way cron does. Returns what the parent printed."""
        with contextlib.redirect_stdout(io.StringIO()) as out:
            wait_for_child()
        return out.getvalue()

    def saved(self) -> tuple[dict, dict]:
        with open(self.path, "rb") as f:
            return load(f, keyspace.now_ms())

    def temp_files(self) -> list[Path]:
        return list(self.dir.glob("temp-*"))


class TestBGSave(BGSaveTest):
    def test_saves_a_snapshot_that_load_reads_back(self):
        self.do("SET a 1", "HSET h f v", f"SET t v PXAT {FAR_FUTURE_MS}")
        self.rdb.lastsave = 0
        started = int(time.time())

        reply = self.bgsave()

        self.assertEqual((reply.typ, reply.string), (RESPType.SIMPLE_STRING, "Background saving started"))
        self.assertEqual(child.kind, "rdb")
        self.assertEqual(self.finish(), "Background saving terminated with success\n")
        self.assertEqual(self.saved(), (
            {b"a": ("string", b"1"), b"h": ("hash", {b"f": b"v"}), b"t": ("string", b"v")},
            {b"t": FAR_FUTURE_MS},
        ))
        self.assertTrue(started <= self.rdb.lastsave <= int(time.time()))
        self.assertTrue(self.rdb.last_save_ok)
        self.assertEqual(self.temp_files(), [])

    def test_writes_after_the_fork_are_not_in_the_snapshot(self):
        self.do("SET a 1")

        self.bgsave()
        self.do("SET b 2", "DEL a")  # the child does not see these
        self.finish()

        self.assertEqual(self.saved(), ({b"a": ("string", b"1")}, {}))

    def test_expired_keys_are_left_out(self):
        self.do("SET short v PX 30", "SET keep v")
        time.sleep(0.05)  # short has expired, but nothing has deleted it yet

        self.bgsave()
        self.finish()

        self.assertEqual(self.saved(), ({b"keep": ("string", b"v")}, {}))

    def test_save_replaces_the_previous_file(self):
        self.do("SET a 1")
        self.bgsave()
        self.finish()
        self.do("SET a 2")

        self.bgsave()
        self.finish()

        self.assertEqual(self.saved(), ({b"a": ("string", b"2")}, {}))

    def test_creates_the_directory_of_the_file(self):
        self.path = self.dir / "sub" / "dir" / "dump.rdb"
        rdb_module.current = RDB(RDBConfig(path=self.path))
        self.do("SET a 1")

        self.bgsave()
        self.finish()

        self.assertEqual(self.saved(), ({b"a": ("string", b"1")}, {}))

    def test_second_bgsave_while_one_runs_is_refused(self):
        self.bgsave()

        reply = self.bgsave()

        self.assertEqual((reply.typ, reply.string), (RESPType.ERROR, "ERR Background save already in progress"))
        self.finish()


class TestBGSaveFailure(BGSaveTest):
    def make_the_child_fail(self) -> None:
        """A type dump() does not know makes the child raise after it has
        started writing the temp file."""
        keyspace.store[b"zz"] = ("list", [b"a"])

    def test_failed_save_keeps_the_old_file_and_lastsave(self):
        self.do("SET a 1")
        self.bgsave()
        self.finish()
        lastsave = self.rdb.lastsave
        self.do("SET a 2")
        self.make_the_child_fail()

        with capture_fd2() as stderr:
            self.bgsave()
            printed = self.finish()

        self.assertIn(b"does not know type 'list'", bytes(stderr))  # the child's traceback
        self.assertEqual(printed, "Background rdb child failed (exit code 1)\n")
        self.assertFalse(self.rdb.last_save_ok)
        self.assertEqual(self.rdb.lastsave, lastsave)
        self.assertEqual(self.saved(), ({b"a": ("string", b"1")}, {}))
        self.assertEqual(self.temp_files(), [])

        # Once the problem is gone, the next save works.
        del keyspace.store[b"zz"]
        self.bgsave()
        self.finish()
        self.assertTrue(self.rdb.last_save_ok)
        self.assertEqual(self.saved(), ({b"a": ("string", b"2")}, {}))

    def test_kill_during_a_save_leaves_no_temp_file_and_keeps_the_old_file(self):
        self.do("SET a 1")
        self.bgsave()
        self.finish()
        for i in range(100_000):  # big enough that the next save is still running when killed
            keyspace.store[b"key:%d" % i] = ("string", b"v")

        self.bgsave()
        pid = child._pid
        child.kill()

        self.assertIsNone(child.kind)
        self.assertEqual(self.temp_files(), [])
        self.assertEqual(self.saved(), ({b"a": ("string", b"1")}, {}))
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)  # already reaped: no zombie left


class TestLastSave(BGSaveTest):
    def test_starts_at_the_time_the_server_started(self):
        before = int(time.time())
        rdb = RDB(RDBConfig(path=self.path))

        self.assertTrue(before <= rdb.lastsave <= int(time.time()))

    def test_replies_with_lastsave_as_an_integer(self):
        self.rdb.lastsave = 1762729200

        reply = self.do("LASTSAVE")[0]

        self.assertEqual((reply.typ, reply.num), (RESPType.INTEGER, 1762729200))


class TestBGSaveWithAOF(BGSaveTest):
    """BGSAVE and the AOF rewrite share one child slot."""

    def setUp(self):
        super().setUp()
        self.aof_path = self.dir / "appendonly.aof"
        aof = AOF(AOFConfig(path=self.aof_path, enabled=True, appendfsync="no"))
        self.addCleanup(aof.close)
        aof_module.current = aof
        self.addCleanup(setattr, aof_module, "current", None)
        self.do("SET a 1", "INCR n", "INCR n")
        aof.flush()

    def test_bgsave_during_an_aof_rewrite_is_refused(self):
        self.do("BGREWRITEAOF")

        reply = self.bgsave()

        self.assertEqual((reply.typ, reply.string), (
            RESPType.ERROR,
            "ERR Another child process is active (AOF?): can't BGSAVE right now. "
            "Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenever possible.",
        ))
        self.assertEqual(child.kind, "aof")
        self.finish()

    def test_bgrewriteaof_during_bgsave_runs_once_the_save_is_done(self):
        self.bgsave()

        reply = self.do("BGREWRITEAOF")[0]
        self.assertEqual(reply.string, "Background append only file rewriting scheduled")

        with contextlib.redirect_stdout(io.StringIO()):
            deadline = time.time() + 5
            while child.kind != "aof":  # cron reaps the save, then starts the rewrite
                self.assertLess(time.time(), deadline, "scheduled rewrite never started")
                cron(aof_module.current, hz=10)
                time.sleep(0.01)
        self.finish()

        self.assertEqual(self.saved(), ({b"a": ("string", b"1"), b"n": ("string", b"2")}, {}))
        self.assertEqual(self.aof_path.read_bytes(), cmd_bytes("SET", "a", "1") + cmd_bytes("SET", "n", "2"))


# ---------------------------------------------------------------------------
# Loading at startup: keyspace.replace_all() and RDB.load_file()
# ---------------------------------------------------------------------------

class TestReplaceAll(unittest.TestCase):
    def setUp(self):
        reset_keyspace()
        self.addCleanup(reset_keyspace)
        propagation.take()

    def test_replaces_the_data_and_rebuilds_the_ttl_index(self):
        execute(make_cmd("SET", "old", "x", "PXAT", str(FAR_FUTURE_MS)), None)

        keyspace.replace_all(
            {b"a": ("string", b"1"), b"t": ("string", b"2"), b"u": ("hash", {b"f": b"v"})},
            {b"t": FAR_FUTURE_MS, b"u": FAR_FUTURE_MS + 1},
        )

        self.assertEqual(keyspace.store, {b"a": ("string", b"1"), b"t": ("string", b"2"),
                                          b"u": ("hash", {b"f": b"v"})})
        self.assertEqual(keyspace.expires, {b"t": FAR_FUTURE_MS, b"u": FAR_FUTURE_MS + 1})
        # Active expiry samples from ttl_keys: the loaded keys must be in it, the old one not.
        self.assertEqual(sorted(keyspace.sample_ttl_keys(10)), [b"t", b"u"])
        self.assertEqual({k: keyspace.ttl_keys[i] for k, i in keyspace.ttl_pos.items()},
                         {b"t": b"t", b"u": b"u"})

    def test_active_expiry_finds_loaded_keys(self):
        keyspace.replace_all({b"t": ("string", b"v")}, {b"t": keyspace.now_ms() + 20})
        time.sleep(0.05)

        active_expire_cycle(keyspace.now_ms(), 0.025)

        self.assertEqual(keyspace.store, {})

    def test_keeps_the_same_dict_objects(self):
        store, expires = keyspace.store, keyspace.expires

        keyspace.replace_all({b"a": ("string", b"1")}, {})

        self.assertIs(keyspace.store, store)
        self.assertIs(keyspace.expires, expires)

    def test_logs_nothing_to_the_aof(self):
        execute(make_cmd("SET", "old", "x"), None)
        propagation.take()

        keyspace.replace_all({b"a": ("string", b"1")}, {b"a": FAR_FUTURE_MS})

        self.assertEqual(propagation.take(), [])


class TestLoadFile(BGSaveTest):
    def write_snapshot(self, store: dict, expires: dict) -> None:
        self.path.write_bytes(to_bytes(store, expires, now_ms=0))

    def load_file(self) -> tuple[int | None, str]:
        with contextlib.redirect_stdout(io.StringIO()) as out:
            loaded = self.rdb.load_file()
        return loaded, out.getvalue()

    def test_no_file_loads_nothing(self):
        self.assertEqual(self.load_file(), (None, ""))
        self.assertEqual(keyspace.store, {})

    def test_loads_the_snapshot_into_the_keyspace(self):
        self.write_snapshot({b"a": ("string", b"1"), b"h": ("hash", {b"f": b"v"}), b"t": ("string", b"x")},
                            {b"t": FAR_FUTURE_MS})

        loaded, printed = self.load_file()

        self.assertEqual(loaded, 3)
        self.assertRegex(printed, r"^DB loaded from disk: 3 keys in \d+\.\d+ seconds\n$")
        self.assertEqual(keyspace.store, {b"a": ("string", b"1"), b"h": ("hash", {b"f": b"v"}),
                                          b"t": ("string", b"x")})
        self.assertEqual(keyspace.expires, {b"t": FAR_FUTURE_MS})
        self.assertEqual(keyspace.sample_ttl_keys(10), [b"t"])

    def test_keys_that_expired_while_stopped_are_not_loaded(self):
        self.write_snapshot({b"gone": ("string", b"x"), b"keep": ("string", b"y")},
                            {b"gone": keyspace.now_ms() - 1})

        loaded, _ = self.load_file()

        self.assertEqual(loaded, 1)
        self.assertEqual(keyspace.store, {b"keep": ("string", b"y")})

    def test_corrupt_file_raises_and_loads_nothing(self):
        data = bytearray(to_bytes({b"a": ("string", b"1")}, {}))
        data[-1] ^= 0xFF  # break the checksum
        self.path.write_bytes(bytes(data))

        with self.assertRaisesRegex(RDBError, "checksum mismatch"):
            self.rdb.load_file()
        self.assertEqual(keyspace.store, {})

    def test_file_that_cannot_be_read_raises_rdberror(self):
        self.path.mkdir()  # a directory where the file should be

        with self.assertRaisesRegex(RDBError, "Is a directory"):
            self.rdb.load_file()


# ---------------------------------------------------------------------------
# Save points: the dirty counter, cron's automatic BGSAVE, the foreground save
# ---------------------------------------------------------------------------

class SavePointTest(BGSaveTest):
    SAVE_POINTS: tuple[tuple[int, int], ...] = ((60, 3), (300, 1))

    def setUp(self):
        super().setUp()
        self.rdb = RDB(RDBConfig(path=self.path, save_points=self.SAVE_POINTS))
        rdb_module.current = self.rdb
        propagation.take()


class TestDirtyCounter(SavePointTest):
    def test_counts_each_logged_change(self):
        self.do("SET a 1", "SET b 2", "INCR n", "HSET h f v", "DEL a b")

        self.assertEqual(self.rdb.dirty, 5)

    def test_reads_and_failed_writes_do_not_count(self):
        self.do("SET a 1")
        self.do("GET a", "EXISTS a", "HSET a f v", "SET a 2 NX", "DEL missing")

        self.assertEqual(self.rdb.dirty, 1)

    def test_expiry_counts(self):
        self.do("SET k v PX 20")
        time.sleep(0.05)

        self.do("GET k")  # lazy expiry deletes it

        self.assertEqual(self.rdb.dirty, 2)

    def test_a_save_resets_the_counter(self):
        self.do("SET a 1", "SET b 2")

        self.bgsave()
        self.finish()

        self.assertEqual(self.rdb.dirty, 0)

    def test_writes_made_during_a_save_stay_counted(self):
        self.do("SET a 1", "SET b 2", "SET c 3")

        self.bgsave()
        self.do("SET d 4", "SET e 5")  # not in the snapshot
        self.finish()

        self.assertEqual(self.rdb.dirty, 2)

    def test_a_failed_save_keeps_the_count(self):
        self.do("SET a 1", "SET b 2")
        keyspace.store[b"zz"] = ("list", [b"a"])  # dump() raises in the child

        with capture_fd2():
            self.bgsave()
            self.finish()

        self.assertEqual(self.rdb.dirty, 2)


class TestSavePointDue(SavePointTest):
    """Save points (60, 3) and (300, 1): due when a point has at least that
    many changes and more than that many seconds since the last save."""

    def setUp(self):
        super().setUp()
        self.rdb.lastsave = 1_000_000

    def due(self, dirty: int, seconds_since_save: float):
        self.rdb.dirty = dirty
        return self.rdb.save_point_due(self.rdb.lastsave + seconds_since_save)

    def test_not_due_without_enough_changes(self):
        self.assertIsNone(self.due(dirty=2, seconds_since_save=61))

    def test_not_due_without_enough_time(self):
        self.assertIsNone(self.due(dirty=1000, seconds_since_save=59))

    def test_seconds_must_be_more_than_the_save_point(self):
        self.assertIsNone(self.due(dirty=3, seconds_since_save=60))

    def test_due_when_both_pass(self):
        self.assertEqual(self.due(dirty=3, seconds_since_save=61), (60, 3))

    def test_any_save_point_is_enough(self):
        self.assertEqual(self.due(dirty=1, seconds_since_save=301), (300, 1))

    def test_never_due_without_save_points(self):
        rdb = RDB(RDBConfig(path=self.path))
        rdb.dirty = 10**6

        self.assertIsNone(rdb.save_point_due(rdb.lastsave + 10**6))

    def test_waits_5_seconds_after_a_failed_save(self):
        keyspace.store[b"zz"] = ("list", [b"a"])  # dump() raises in the child
        with capture_fd2():
            self.bgsave()
            self.finish()
        self.assertFalse(self.rdb.last_save_ok)
        tried = self.rdb._last_try
        self.rdb.lastsave = int(tried) - 10_000
        self.rdb.dirty = 10

        self.assertIsNone(self.rdb.save_point_due(tried + 4.9))
        self.assertEqual(self.rdb.save_point_due(tried + 5), (60, 3))


class TestCronSave(SavePointTest):
    SAVE_POINTS = ((1, 1),)

    def cron(self) -> str:
        with contextlib.redirect_stdout(io.StringIO()) as out:
            cron(None, hz=10)
        return out.getvalue()

    def test_cron_starts_a_save_when_a_save_point_is_due(self):
        self.do("SET a 1")
        self.rdb.lastsave = int(time.time()) - 2

        printed = self.cron()

        self.assertEqual(printed, "1 changes in 1 seconds. Saving...\n")
        self.assertEqual(child.kind, "rdb")
        self.finish()
        self.assertEqual(self.saved(), ({b"a": ("string", b"1")}, {}))
        self.assertEqual(self.rdb.dirty, 0)

    def test_cron_does_not_save_without_changes(self):
        self.rdb.lastsave = int(time.time()) - 2

        self.assertEqual(self.cron(), "")
        self.assertIsNone(child.kind)

    def test_cron_does_not_start_a_save_while_another_child_runs(self):
        self.do("SET a 1")
        self.rdb.lastsave = int(time.time()) - 2
        child.start("aof", lambda: time.sleep(30), lambda ok: None)

        self.cron()

        self.assertEqual(child.kind, "aof")


class TestForegroundSave(SavePointTest):
    """save() writes the file in this process. The server uses it at shutdown."""

    def test_save_writes_the_file_without_a_child(self):
        self.do("SET a 1", f"SET t v PXAT {FAR_FUTURE_MS}")
        self.rdb.lastsave = 0

        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.rdb.save()

        self.assertEqual(out.getvalue(), "DB saved on disk\n")
        self.assertIsNone(child.kind)
        self.assertEqual(self.saved(), ({b"a": ("string", b"1"), b"t": ("string", b"v")}, {b"t": FAR_FUTURE_MS}))
        self.assertEqual(self.rdb.dirty, 0)
        self.assertTrue(self.rdb.last_save_ok)
        self.assertGreater(self.rdb.lastsave, 0)
        self.assertEqual(self.temp_files(), [])

    def test_failed_save_raises_and_keeps_the_old_file(self):
        self.do("SET a 1")
        with contextlib.redirect_stdout(io.StringIO()):
            self.rdb.save()
        self.do("SET a 2")
        lastsave = self.rdb.lastsave
        (self.dir / "temp-bgsave.rdb").mkdir()  # the temp file cannot be created

        with self.assertRaises(OSError):
            self.rdb.save()

        self.assertFalse(self.rdb.last_save_ok)
        self.assertEqual(self.rdb.dirty, 1)
        self.assertEqual(self.rdb.lastsave, lastsave)
        self.assertEqual(self.saved(), ({b"a": ("string", b"1")}, {}))


if __name__ == "__main__":
    unittest.main(verbosity=2)
