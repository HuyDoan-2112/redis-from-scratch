"""Tests for the AOF class: crash recovery, buffering, fsync policies and
the background rewrite.

Run from the project root:
    python3 -m unittest tests.test_aof -v
"""

import contextlib
import io
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from mini_redis.persistence.aof import AOF
from mini_redis.config import AOFConfig, FSYNC_POLICIES
from mini_redis.server import execute
from tests.helpers import cmd_bytes, make_cmd, replay, reset_keyspace

FAR_FUTURE_MS = 4102444800000  # 2100-01-01, a deadline that never passes during a test


# ---------------------------------------------------------------------------
# Unit tests: AOF class only (fast, no network)
# ---------------------------------------------------------------------------

class TestAOFRecovery(unittest.TestCase):
    """The server crashed in the middle of a write, so the last command in
    the file is only half written. Like Redis with aof-load-truncated yes:
    every complete command is replayed, the broken tail is skipped and cut
    off the file, and the server can restart again and again after that."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "appendonly.aof"

    def tearDown(self):
        self.tmp.cleanup()

    def test_replay_complete_file(self):
        """A healthy file replays every command in order."""
        self.path.write_bytes(cmd_bytes("SET", "a", "1") + cmd_bytes("SET", "b", "2"))

        self.assertEqual(replay(self.path), [["SET", "a", "1"], ["SET", "b", "2"]])

    def test_broken_last_command_is_skipped(self):
        """A half-written last command is skipped; earlier ones still load."""
        good = cmd_bytes("SET", "a", "1")
        broken = cmd_bytes("SET", "b", "2")[:10]  # crash after 10 bytes
        self.path.write_bytes(good + broken)

        self.assertEqual(replay(self.path), [["SET", "a", "1"]])

    def test_broken_bytes_are_removed_from_file(self):
        """After replay, the file must contain only the complete commands."""
        good = cmd_bytes("SET", "a", "1")
        broken = cmd_bytes("SET", "b", "2")[:10]
        self.path.write_bytes(good + broken)

        replay(self.path)

        self.assertEqual(
            self.path.read_bytes(),
            good,
            "broken tail was not truncated from the AOF file",
        )

    def test_new_write_after_crash_survives_next_restart(self):
        """The full bug: crash -> restart -> write -> restart must still work."""
        good = cmd_bytes("SET", "a", "1")
        broken = cmd_bytes("SET", "b", "2")[:10]
        self.path.write_bytes(good + broken)

        # Restart #1: replay, then a client sends SET c 3
        aof = AOF(AOFConfig(path=self.path, enabled=True))
        with contextlib.redirect_stdout(io.StringIO()):
            aof.read(lambda v: None)
        aof.write(make_cmd("SET", "c", "3"))
        aof.close()

        # Restart #2: must not crash, and must see a and c
        try:
            commands = replay(self.path)
        except Exception as err:
            self.fail(f"server could not replay AOF on second restart: {err!r}")

        self.assertEqual(commands, [["SET", "a", "1"], ["SET", "c", "3"]])

    def test_cut_inside_length_line(self):
        """Same bug when the cut lands inside '$1\\r\\n' (like truncate -s -5)."""
        good = cmd_bytes("SET", "a", "1")
        full_b = cmd_bytes("SET", "b", "2")
        self.path.write_bytes(good + full_b[:-5])  # file ends with "...$1"

        aof = AOF(AOFConfig(path=self.path, enabled=True))
        with contextlib.redirect_stdout(io.StringIO()):
            aof.read(lambda v: None)
        aof.write(make_cmd("SET", "c", "3"))
        aof.close()

        try:
            commands = replay(self.path)
        except Exception as err:
            self.fail(f"server could not replay AOF on second restart: {err!r}")

        self.assertEqual(commands, [["SET", "a", "1"], ["SET", "c", "3"]])


def wait_for_rewrite(aof: AOF, timeout: float = 5.0) -> None:
    """Run check_rewrite() the way the server's cron does, until the child is done."""
    deadline = time.time() + timeout
    while aof.rewrite_pid is not None:
        if time.time() > deadline:
            raise AssertionError("rewrite child did not finish")
        aof.check_rewrite()
        time.sleep(0.01)


class AOFTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.path = self.dir / "appendonly.aof"
        self.closed: list[AOF] = []
        reset_keyspace()
        self.addCleanup(reset_keyspace)

    def open_aof(self, appendfsync: str = "no") -> AOF:
        aof = AOF(AOFConfig(path=self.path, enabled=True, appendfsync=appendfsync))
        self.addCleanup(lambda: aof in self.closed or aof.close())
        return aof

    def close(self, aof: AOF) -> None:
        """AOF.close() may run only once, so cleanup skips AOFs closed here."""
        aof.close()
        self.closed.append(aof)

    def record_fsyncs(self) -> mock.Mock:
        """Watch os.fsync (a system call) and still let it run."""
        patcher = mock.patch.object(os, "fsync", wraps=os.fsync)
        fsync = patcher.start()
        self.addCleanup(patcher.stop)
        return fsync


class TestBuffer(AOFTest):
    def test_write_reaches_the_file_only_on_flush(self):
        aof = self.open_aof()
        aof.write(make_cmd("SET", "a", "1"))
        aof.write(make_cmd("SET", "b", "2"))
        self.assertEqual(self.path.read_bytes(), b"")

        aof.flush()

        self.assertEqual(self.path.read_bytes(), cmd_bytes("SET", "a", "1") + cmd_bytes("SET", "b", "2"))

    def test_close_flushes_and_fsyncs_with_every_policy(self):
        for policy in FSYNC_POLICIES:
            with self.subTest(appendfsync=policy):
                self.path.unlink(missing_ok=True)
                with mock.patch.object(os, "fsync", wraps=os.fsync) as fsync:
                    aof = self.open_aof(policy)
                    fd = aof.fd
                    aof.write(make_cmd("SET", "a", "1"))

                    self.close(aof)

                self.assertEqual(self.path.read_bytes(), cmd_bytes("SET", "a", "1"))
                fsync.assert_any_call(fd)


class TestFsyncPolicy(AOFTest):
    def test_always_fsyncs_on_every_flush(self):
        fsync = self.record_fsyncs()
        aof = self.open_aof("always")

        aof.write(make_cmd("SET", "a", "1"))
        aof.flush()
        self.assertEqual(fsync.call_count, 1)
        fsync.assert_called_with(aof.fd)

        aof.flush()  # nothing buffered: no write, no fsync
        self.assertEqual(fsync.call_count, 1)

        aof.write(make_cmd("SET", "b", "2"))
        aof.flush()
        self.assertEqual(fsync.call_count, 2)

    def test_everysec_fsyncs_in_the_background(self):
        fsync = self.record_fsyncs()
        aof = self.open_aof("everysec")

        aof.write(make_cmd("SET", "a", "1"))
        aof.flush()
        self.assertEqual(fsync.call_count, 0, "flush() itself must not fsync")

        deadline = time.time() + 2
        while fsync.call_count == 0 and time.time() < deadline:
            time.sleep(0.05)
        fsync.assert_called_with(aof.fd)

    def test_no_never_fsyncs_before_close(self):
        fsync = self.record_fsyncs()
        aof = self.open_aof("no")

        aof.write(make_cmd("SET", "a", "1"))
        aof.flush()
        time.sleep(1.2)  # longer than the everysec interval

        self.assertEqual(fsync.call_count, 0)
        self.assertEqual(self.path.read_bytes(), cmd_bytes("SET", "a", "1"))


class TestBackgroundRewrite(AOFTest):
    """BGREWRITEAOF forks a child that writes the data to a temp file. The
    server keeps logging writes, and cron swaps the temp file in once the
    child is done."""

    def do(self, *commands: str) -> None:
        for command in commands:
            execute(make_cmd(*command.split()), self.aof)

    def setUp(self):
        super().setUp()
        self.aof = self.open_aof()

    def test_rewrite_keeps_only_the_commands_that_rebuild_the_data(self):
        self.do("INCR n", "INCR n", "INCR n", "SET a 1", "SET a 2", "HSET h f v",
                 f"SET t v PXAT {FAR_FUTURE_MS}", "SET gone x", "DEL gone")
        self.aof.flush()

        self.aof.start_rewrite()
        wait_for_rewrite(self.aof)

        self.assertEqual(
            self.path.read_bytes(),
            cmd_bytes("SET", "n", "3")
            + cmd_bytes("SET", "a", "2")
            + cmd_bytes("HSET", "h", "f", "v")
            + cmd_bytes("SET", "t", "v")
            + cmd_bytes("PEXPIREAT", "t", str(FAR_FUTURE_MS)),
        )
        self.assertEqual(list(self.dir.glob("temp-*")), [])

    def test_writes_made_during_the_rewrite_are_kept(self):
        self.do("SET a 1")
        self.aof.flush()

        self.aof.start_rewrite()
        self.do("SET b 2", "DEL a")  # the child does not see these
        self.aof.flush()
        wait_for_rewrite(self.aof)

        self.do("SET c 3")  # goes to the new file
        self.close(self.aof)

        self.assertEqual(replay(self.path), [
            ["SET", "a", "1"],
            ["SET", "b", "2"],
            ["DEL", "a"],
            ["SET", "c", "3"],
        ])

    def test_expired_keys_are_left_out(self):
        self.do("SET short v PX 30", "SET keep v")
        time.sleep(0.05)  # short has expired, but nothing has deleted it yet

        self.aof.start_rewrite()
        wait_for_rewrite(self.aof)

        self.assertEqual(self.path.read_bytes(), cmd_bytes("SET", "keep", "v"))

    def test_second_rewrite_while_one_runs_is_refused(self):
        self.aof.start_rewrite()

        with self.assertRaisesRegex(RuntimeError, "already in progress"):
            self.aof.start_rewrite()
        wait_for_rewrite(self.aof)

    def test_failed_rewrite_keeps_the_old_file(self):
        self.do("SET a 1")
        self.aof.flush()
        # A directory where the temp file should go makes the child fail.
        blocker = self.dir / "temp-rewrite.aof"
        blocker.mkdir()

        self.aof.start_rewrite()
        with contextlib.redirect_stdout(io.StringIO()):
            wait_for_rewrite(self.aof)

        self.do("SET b 2")
        self.aof.flush()
        self.assertEqual(self.path.read_bytes(), cmd_bytes("SET", "a", "1") + cmd_bytes("SET", "b", "2"))

        # Once the problem is gone, a new rewrite works.
        blocker.rmdir()
        self.aof.start_rewrite()
        wait_for_rewrite(self.aof)
        self.assertEqual(self.path.read_bytes(), cmd_bytes("SET", "a", "1") + cmd_bytes("SET", "b", "2"))

    def test_close_during_rewrite_stops_the_child(self):
        self.do("SET a 1")
        self.aof.start_rewrite()
        pid = self.aof.rewrite_pid

        self.close(self.aof)

        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)  # already reaped: no zombie left
        self.assertEqual(list(self.dir.glob("temp-*")), [])
        self.assertEqual(self.path.read_bytes(), cmd_bytes("SET", "a", "1"))


class TestOldFileClose(AOFTest):
    """After a rewrite, the old AOF has no name and this process holds its last
    fd. Closing it makes the kernel free the file's blocks, which takes time
    for a big file, so it happens off the event loop."""

    def setUp(self):
        super().setUp()
        self.aof = self.open_aof()
        execute(make_cmd("SET", "a", "1"), self.aof)
        self.aof.flush()

    def slow_close_of_old_file(self, seconds: float) -> None:
        """Make os.close() of the current AOF fd take `seconds`, like freeing a big file."""
        real_close = os.close
        old_fd = self.aof.fd

        def close(fd: int) -> None:
            if fd == old_fd:
                time.sleep(seconds)
            real_close(fd)

        patcher = mock.patch.object(os, "close", side_effect=close)
        patcher.start()
        self.addCleanup(patcher.stop)

    def finish_rewrite(self) -> float:
        """Wait for the rewrite child, and return the longest check_rewrite() call."""
        self.aof.start_rewrite()
        longest = 0.0
        while self.aof.rewrite_pid is not None:
            started = time.perf_counter()
            self.aof.check_rewrite()
            longest = max(longest, time.perf_counter() - started)
            time.sleep(0.01)
        return longest

    def deleted_files_held_open(self) -> list[str]:
        held = []
        for fd in os.listdir("/proc/self/fd"):
            try:
                target = os.readlink(f"/proc/self/fd/{fd}")
            except OSError:
                continue  # closed while we looked
            if target.startswith(str(self.dir)) and target.endswith(" (deleted)"):
                held.append(target)
        return held

    def test_finishing_a_rewrite_does_not_wait_for_the_old_file_to_close(self):
        self.slow_close_of_old_file(0.5)

        self.assertLess(self.finish_rewrite(), 0.25)

        execute(make_cmd("SET", "b", "2"), self.aof)
        self.aof.flush()
        self.assertEqual(self.path.read_bytes(), cmd_bytes("SET", "a", "1") + cmd_bytes("SET", "b", "2"))

    def test_close_waits_until_the_old_file_is_closed(self):
        self.slow_close_of_old_file(0.3)
        self.finish_rewrite()

        self.close(self.aof)

        self.assertEqual(self.deleted_files_held_open(), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
