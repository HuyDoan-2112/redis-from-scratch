"""Tests for the background child slot: one forked child at a time, reaped
from cron, killed at shutdown.

Run from the project root:
    python3 -m unittest tests.test_child -v
"""

import contextlib
import io
import os
import signal
import tempfile
import time
import unittest
from pathlib import Path

from mini_redis.persistence import child
from tests.helpers import capture_fd2, wait_for_child


class TestChild(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.done: list[bool] = []
        self.addCleanup(child.kill)

    def test_work_runs_in_another_process_and_reports_success(self):
        pid_file = self.dir / "pid"

        child.start("rdb", lambda: pid_file.write_text(str(os.getpid())), self.done.append)
        self.assertEqual(child.kind, "rdb")
        wait_for_child()

        self.assertEqual(self.done, [True])
        self.assertIsNone(child.kind)
        self.assertNotEqual(int(pid_file.read_text()), os.getpid())

    def test_check_leaves_a_running_child_alone(self):
        child.start("aof", lambda: time.sleep(30), self.done.append)

        child.check()

        self.assertEqual(child.kind, "aof")
        self.assertEqual(self.done, [])

    def test_work_that_raises_reports_failure_and_prints_the_traceback(self):
        def work():
            raise ValueError("disk on fire")

        with capture_fd2() as stderr, contextlib.redirect_stdout(io.StringIO()) as stdout:
            child.start("rdb", work, self.done.append)
            wait_for_child()

        self.assertEqual(self.done, [False])
        self.assertIn(b"ValueError: disk on fire", bytes(stderr))  # printed by the child
        self.assertEqual(stdout.getvalue(), "Background rdb child failed (exit code 1)\n")  # by the parent

    def test_child_killed_by_a_signal_reports_a_negative_exit_code(self):
        def work():
            os.kill(os.getpid(), signal.SIGKILL)  # like the kernel's out-of-memory killer

        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            child.start("rdb", work, self.done.append)
            wait_for_child()

        self.assertEqual(self.done, [False])
        self.assertEqual(stdout.getvalue(), "Background rdb child failed (exit code -9)\n")

    def test_kill_stops_the_child_quietly_and_leaves_no_zombie(self):
        child.start("aof", lambda: time.sleep(30), self.done.append)
        pid = child._pid

        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            child.kill()

        self.assertEqual(stdout.getvalue(), "", "a kill at shutdown is not a failure")
        self.assertEqual(self.done, [False])
        self.assertIsNone(child.kind)
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)  # already reaped

    def test_kill_without_a_child_does_nothing(self):
        child.kill()

        self.assertEqual(self.done, [])

    def test_only_one_child_at_a_time(self):
        child.start("aof", lambda: time.sleep(30), self.done.append)

        with self.assertRaises(AssertionError):
            child.start("rdb", lambda: None, self.done.append)
        self.assertEqual(child.kind, "aof")

    def test_slot_is_free_even_if_on_done_raises(self):
        def on_done(ok: bool) -> None:
            raise OSError("rename failed")

        child.start("rdb", lambda: None, on_done)
        with self.assertRaisesRegex(OSError, "rename failed"):
            wait_for_child()

        self.assertIsNone(child.kind)


if __name__ == "__main__":
    unittest.main(verbosity=2)
