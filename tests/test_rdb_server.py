"""RDB tests against the real server process, the way a user runs it.

Each test starts `python -m mini_redis --rdb-path <tmp>/dump.rdb`, talks to it
over TCP, and checks the snapshot file it writes and what a restart loads.

Run from the project root:
    python -m unittest tests.test_rdb_server -v
"""

import io
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from mini_redis.persistence.rdb import dump, load
from mini_redis.storage.keyspace import now_ms
from tests.helpers import (
    PROJECT_ROOT,
    Server,
    child_pids,
    cmd_bytes,
    free_port,
    pid_exists,
    replay,
    wait_until,
)

FAR_FUTURE_MS = 4102444800000  # 2100-01-01, a deadline that never passes during a test


class RDBServerTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.rdb_path = self.dir / "dump.rdb"
        self.aof_path = self.dir / "appendonly.aof"

    def start(self, aof: bool = False):
        flags = ["--rdb-path", str(self.rdb_path)]
        if aof:
            flags += ["--aof", "--aof-path", str(self.aof_path)]
        server = Server(free_port(), self.dir / "server.log", *flags)
        self.addCleanup(server.kill)
        server.start()
        c = server.client()
        self.addCleanup(c.close)
        return server, c

    def saved(self) -> tuple[dict, dict]:
        with open(self.rdb_path, "rb") as f:
            return load(f, now_ms())

    def fill(self, c, keys: int) -> None:
        """Enough keys that a save is still running a moment after it starts."""
        for start in range(0, keys, 10_000):
            c.pipeline([("SET", f"key:{i}", "v") for i in range(start, start + 10_000)])


class TestBGSave(RDBServerTest):
    def test_bgsave_writes_a_file_and_lastsave_moves(self):
        server, c = self.start()
        c.call("SET", "a", "1")
        c.call("HSET", "h", "f", "v")
        c.call("SET", "t", "v", "PXAT", str(FAR_FUTURE_MS))
        first = c.call("LASTSAVE")
        self.assertLessEqual(abs(first - time.time()), 5, "LASTSAVE starts at the server's start time")
        # LASTSAVE has 1 s resolution: let a second pass so the save must move it.
        self.assertTrue(wait_until(lambda: int(time.time()) > first, timeout=2))

        self.assertEqual(c.call("BGSAVE"), "Background saving started")

        self.assertTrue(wait_until(lambda: c.call("LASTSAVE") > first), "LASTSAVE did not move")
        self.assertEqual(self.saved(), (
            {b"a": ("string", b"1"), b"h": ("hash", {b"f": b"v"}), b"t": ("string", b"v")},
            {b"t": FAR_FUTURE_MS},
        ))
        self.assertEqual(child_pids(server.proc.pid), [], "save child was not reaped")
        self.assertEqual(list(self.dir.glob("temp-*")), [])
        self.assertEqual(server.stop(), 0)  # the log is buffered until the server exits
        self.assertIn("Background saving terminated with success", server.log())

    def test_second_bgsave_while_one_runs_is_an_error(self):
        server, c = self.start()
        c.call("SET", "a", "1")

        # One send: both run in the same loop turn, before cron can reap the child.
        c.sock.sendall(cmd_bytes("BGSAVE") * 2)

        self.assertEqual(c._read_reply(), "Background saving started")
        with self.assertRaisesRegex(RuntimeError, "^ERR Background save already in progress$"):
            c._read_reply()
        self.assertTrue(wait_until(lambda: child_pids(server.proc.pid) == []))

    def test_sigterm_during_bgsave(self):
        """Stopping mid-save exits cleanly and leaves no child or temp file."""
        server, c = self.start()
        self.fill(c, 200_000)

        c.call("BGSAVE")
        children = child_pids(server.proc.pid)
        self.assertEqual(len(children), 1, "expected one save child")

        self.assertEqual(server.stop(), 0)

        self.assertTrue(wait_until(lambda: not pid_exists(children[0]), timeout=2),
                        "save child outlived the server")
        self.assertEqual(list(self.dir.glob("temp-*")), [])


class TestBGSaveWithAOF(RDBServerTest):
    def test_bgsave_during_an_aof_rewrite_is_an_error(self):
        server, c = self.start(aof=True)
        c.call("SET", "a", "1")

        c.sock.sendall(cmd_bytes("BGREWRITEAOF") + cmd_bytes("BGSAVE"))

        self.assertEqual(c._read_reply(), "Background append only file rewriting started")
        with self.assertRaisesRegex(RuntimeError, r"^ERR Another child process is active \(AOF\?\)"):
            c._read_reply()
        self.assertTrue(wait_until(lambda: child_pids(server.proc.pid) == []))

    def test_bgrewriteaof_during_bgsave_runs_after_it(self):
        server, c = self.start(aof=True)
        self.fill(c, 100_000)
        c.pipeline([("INCR", "counter")] * 1_000)

        c.sock.sendall(cmd_bytes("BGSAVE") + cmd_bytes("BGREWRITEAOF"))

        self.assertEqual(c._read_reply(), "Background saving started")
        self.assertEqual(c._read_reply(), "Background append only file rewriting scheduled")
        # The rewrite turns 1,000 INCRs into one SET.
        self.assertTrue(
            wait_until(lambda: b"INCR" not in self.aof_path.read_bytes(), timeout=10),
            "scheduled rewrite did not run",
        )
        self.assertTrue(wait_until(lambda: child_pids(server.proc.pid) == []))
        store, _ = self.saved()
        self.assertEqual(len(store), 100_001)
        self.assertEqual(store[b"counter"], ("string", b"1000"))


class TestLoadAtStartup(RDBServerTest):
    """Without AOF, the server loads dump.rdb before it accepts clients."""

    def save(self, c) -> None:
        """BGSAVE, then wait until dump.rdb is in place (os.replace makes it appear complete)."""
        self.rdb_path.unlink(missing_ok=True)
        self.assertEqual(c.call("BGSAVE"), "Background saving started")
        self.assertTrue(wait_until(self.rdb_path.exists), "BGSAVE did not write dump.rdb")

    def test_data_survives_a_restart(self):
        server, c = self.start()
        c.call("SET", "a", "1")
        c.call("HSET", "h", "f", "v")
        c.call("SET", "t", "v", "PXAT", str(FAR_FUTURE_MS))
        self.save(c)
        self.assertEqual(server.stop(), 0)

        server.start()
        c = server.client()
        self.addCleanup(c.close)

        self.assertEqual(c.call("GET", "a"), "1")
        self.assertEqual(c.call("HGET", "h", "f"), "v")
        self.assertGreater(c.call("TTL", "t"), 0)
        self.assertEqual(c.call("DBSIZE"), 3)
        self.assertEqual(server.stop(), 0)
        self.assertIn("DB loaded from disk: 3 keys in", server.log())

    def test_key_that_expired_while_stopped_stays_gone(self):
        server, c = self.start()
        c.call("SET", "short", "v", "PX", "300")
        c.call("SET", "keep", "v")
        self.save(c)
        server.stop()
        time.sleep(0.4)

        server.start()
        c = server.client()
        self.addCleanup(c.close)

        self.assertIsNone(c.call("GET", "short"))
        self.assertEqual(c.call("DBSIZE"), 1)

    def test_no_file_starts_empty(self):
        server, c = self.start()

        self.assertEqual(c.call("DBSIZE"), 0)
        self.assertFalse(self.rdb_path.exists(), "starting must not create dump.rdb")

    def test_many_keys_survive_a_restart(self):
        server, c = self.start()
        self.fill(c, 100_000)
        self.save(c)
        server.stop()

        server.start()
        c = server.client()
        self.addCleanup(c.close)

        self.assertEqual(c.call("DBSIZE"), 100_000)
        self.assertEqual(c.call("GET", "key:99999"), "v")


class TestBadFileAtStartup(RDBServerTest):
    """A dump.rdb that exists but cannot be used stops the server before it
    opens its port: starting empty would hide the problem, and the next save
    would overwrite the file."""

    def run_server(self, *flags: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "mini_redis", "--port", str(free_port()),
             "--rdb-path", str(self.rdb_path), *flags],
            cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=10,
        )

    def corrupt_snapshot(self) -> None:
        out = io.BytesIO()
        dump(out, {b"a": ("string", b"1")}, {}, 0)
        data = bytearray(out.getvalue())
        data[-1] ^= 0xFF  # break the checksum
        self.rdb_path.write_bytes(bytes(data))

    def test_corrupt_file_stops_the_server(self):
        self.corrupt_snapshot()

        result = self.run_server()

        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stderr, f"Can't load the RDB file {self.rdb_path}: checksum mismatch\n")
        self.assertNotIn("Server listening", result.stdout)

    def test_file_that_cannot_be_read_stops_the_server(self):
        self.rdb_path.mkdir()

        result = self.run_server()

        self.assertEqual(result.returncode, 1)
        self.assertIn(f"Can't load the RDB file {self.rdb_path}: ", result.stderr)
        self.assertIn("Is a directory", result.stderr)

    def test_corrupt_file_with_aof_turned_on_for_the_first_time_creates_no_aof(self):
        """Otherwise the next start would find an empty AOF, and ignore the
        fixed dump.rdb."""
        self.corrupt_snapshot()

        result = self.run_server("--aof", "--aof-path", str(self.aof_path))

        self.assertEqual(result.returncode, 1)
        self.assertFalse(self.aof_path.exists())


class TestLoadWithAOF(RDBServerTest):
    def test_an_existing_aof_wins_over_dump_rdb(self):
        server, c = self.start(aof=True)
        c.call("SET", "a", "1")
        c.call("BGSAVE")
        self.assertTrue(wait_until(self.rdb_path.exists))
        c.call("SET", "a", "2")  # only in the AOF
        server.stop()

        server.start()
        c = server.client()
        self.addCleanup(c.close)

        self.assertEqual(c.call("GET", "a"), "2")

    def test_turning_aof_on_for_the_first_time_keeps_the_snapshot(self):
        server, c = self.start()
        c.call("SET", "a", "1")
        c.call("HSET", "h", "f", "v")
        c.call("SET", "t", "v", "PXAT", str(FAR_FUTURE_MS))
        c.call("BGSAVE")
        self.assertTrue(wait_until(self.rdb_path.exists))
        server.stop()

        server, c = self.start(aof=True)  # the AOF file does not exist yet

        self.assertEqual(c.call("DBSIZE"), 3)
        # The new AOF starts with the snapshot's data...
        self.assertEqual(replay(self.aof_path), [
            ["SET", "a", "1"],
            ["HSET", "h", "f", "v"],
            ["SET", "t", "v"],
            ["PEXPIREAT", "t", str(FAR_FUTURE_MS)],
        ])
        c.call("SET", "b", "2")
        server.stop()

        # ...so the data survives the next restart without dump.rdb.
        self.rdb_path.unlink()
        server.start()
        c = server.client()
        self.addCleanup(c.close)
        self.assertEqual(c.call("GET", "a"), "1")
        self.assertEqual(c.call("GET", "b"), "2")
        self.assertEqual(c.call("DBSIZE"), 4)

    def test_an_empty_aof_that_exists_is_not_filled_from_an_old_dump_rdb(self):
        """An empty AOF is a real state (a rewrite after every key was
        deleted), so an old dump.rdb must not bring deleted keys back."""
        self.rdb_path.write_bytes(self.snapshot_with_one_key())
        self.aof_path.write_bytes(b"")

        server, c = self.start(aof=True)

        self.assertEqual(c.call("DBSIZE"), 0)

    def snapshot_with_one_key(self) -> bytes:
        out = io.BytesIO()
        dump(out, {b"old": ("string", b"x")}, {}, 0)
        return out.getvalue()


class TestSavePoints(RDBServerTest):
    def start_with(self, *flags: str):
        server = Server(free_port(), self.dir / "server.log", "--rdb-path", str(self.rdb_path), *flags)
        self.addCleanup(server.kill)
        server.start()
        c = server.client()
        self.addCleanup(c.close)
        return server, c

    def restart(self, server: Server):
        server.start()
        c = server.client()
        self.addCleanup(c.close)
        return c

    def test_a_due_save_point_saves_on_its_own(self):
        server, c = self.start_with("--save", "1 1")
        first = c.call("LASTSAVE")
        c.call("SET", "a", "1")

        self.assertTrue(wait_until(self.rdb_path.exists, timeout=5), "no automatic save")
        self.assertTrue(wait_until(lambda: c.call("LASTSAVE") > first), "LASTSAVE did not move")
        self.assertEqual(self.saved(), ({b"a": ("string", b"1")}, {}))
        self.assertEqual(server.stop(), 0)
        self.assertIn("1 changes in 1 seconds. Saving...", server.log())

    def test_no_changes_no_automatic_save(self):
        server, c = self.start_with("--save", "1 1")

        time.sleep(1.5)

        self.assertFalse(self.rdb_path.exists())

    def test_sigterm_saves_the_latest_data(self):
        server, c = self.start_with("--save", "3600 1")  # never due during the test
        c.call("SET", "a", "1")
        c.call("SET", "t", "v", "PXAT", str(FAR_FUTURE_MS))

        self.assertEqual(server.stop(), 0)

        self.assertEqual(self.saved(), ({b"a": ("string", b"1"), b"t": ("string", b"v")}, {b"t": FAR_FUTURE_MS}))
        self.assertIn("DB saved on disk", server.log())
        c = self.restart(server)
        self.assertEqual(c.call("GET", "a"), "1")
        self.assertGreater(c.call("TTL", "t"), 0)

    def test_sigterm_during_a_bgsave_still_saves_everything(self):
        server, c = self.start_with("--save", "3600 1")
        self.fill(c, 200_000)
        c.call("BGSAVE")
        c.call("SET", "last", "write")  # after the fork: only the final save has it

        self.assertEqual(server.stop(), 0)

        store, _ = self.saved()
        self.assertEqual(len(store), 200_001)
        self.assertEqual(store[b"last"], ("string", b"write"))
        self.assertEqual(list(self.dir.glob("temp-*")), [])

    def test_no_save_points_no_save_at_shutdown(self):
        server, c = self.start_with()
        c.call("SET", "a", "1")

        self.assertEqual(server.stop(), 0)

        self.assertFalse(self.rdb_path.exists())

    def test_failed_final_save_keeps_the_server_running(self):
        server, c = self.start_with("--save", "3600 1")
        c.call("SET", "a", "1")
        blocker = self.dir / "temp-bgsave.rdb"
        blocker.mkdir()  # the final save cannot create its temp file

        server.proc.send_signal(signal.SIGTERM)
        time.sleep(0.5)  # the loop wakes at once on a signal; this is plenty

        self.assertIsNone(server.proc.poll(), "the server exited without saving")
        self.assertEqual(c.call("GET", "a"), "1")
        c.call("SET", "b", "2")

        # Once the problem is gone, the next SIGTERM saves and exits.
        blocker.rmdir()
        self.assertEqual(server.stop(), 0)
        self.assertEqual(self.saved(), ({b"a": ("string", b"1"), b"b": ("string", b"2")}, {}))
        self.assertIn("Error trying to save the DB, can't exit", server.log())


if __name__ == "__main__":
    unittest.main(verbosity=2)
