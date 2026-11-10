"""AOF tests against the real server process, the way a user runs it.

Each test starts `python -m mini_redis --aof`, talks to it over TCP, and
stops it with SIGTERM (clean) or SIGKILL (crash), then starts it again on the
same AOF to check what survived.

Run from the project root:
    python -m unittest tests.test_aof_server -v
"""

import os
import tempfile
import time
import unittest
from pathlib import Path

from mini_redis.config import FSYNC_POLICIES
from tests.helpers import Server, child_pids, cmd_bytes, free_port, pid_exists, replay, wait_until

FAR_FUTURE_MS = 4102444800000  # 2100-01-01, a deadline that never passes during a test


class AOFServerTest(unittest.TestCase):
    """Starts no server itself: each test calls self.server(...) with its flags."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.aof_path = self.dir / "appendonly.aof"

    def server(self, *args: str, aof: bool = True) -> Server:
        flags = ("--aof", "--aof-path", str(self.aof_path)) if aof else ()
        server = Server(free_port(), self.dir / "server.log", *flags, *args)
        self.addCleanup(server.kill)
        return server

    def client(self, server: Server):
        c = server.client()
        self.addCleanup(c.close)
        return c


class TestRestart(AOFServerTest):
    def test_crash_recovery_like_redis_cli(self):
        server = self.server()

        # Start, write two keys, stop.
        server.start()
        c = self.client(server)
        self.assertEqual(c.call("SET", "a", "1"), "OK")
        self.assertEqual(c.call("SET", "b", "2"), "OK")
        server.stop()

        # A crash in the middle of a write (same as: truncate -s -5 appendonly.aof).
        size = self.aof_path.stat().st_size
        os.truncate(self.aof_path, size - 5)

        # Restart: b is lost, a is still there. Then write c.
        server.start()
        c = self.client(server)
        self.assertEqual(c.call("GET", "a"), "1")
        self.assertIsNone(c.call("GET", "b"))
        self.assertEqual(c.call("SET", "c", "3"), "OK")
        server.stop()

        # Restart again: the broken tail was cut off, so c was not glued onto it.
        server.start()
        c = self.client(server)
        self.assertEqual(c.call("GET", "a"), "1")
        self.assertIsNone(c.call("GET", "b"))
        self.assertEqual(c.call("GET", "c"), "3")

    def test_every_write_command_survives_restart(self):
        server = self.server()
        server.start()
        c = self.client(server)
        c.call("SET", "s", "v")
        c.call("HSET", "h", "f1", "a")
        c.call("HSET", "h", "f2", "b")
        c.call("HSET", "h", "f1", "c")  # overwrite a field
        for _ in range(3):
            c.call("INCR", "n")
        c.call("SET", "gone", "x")
        c.call("DEL", "gone")
        server.stop()

        server.start()
        c = self.client(server)
        self.assertEqual(c.call("GET", "s"), "v")
        self.assertEqual(c.call("HGET", "h", "f1"), "c")
        self.assertEqual(c.call("HGET", "h", "f2"), "b")
        self.assertEqual(c.call("GET", "n"), "3")
        self.assertEqual(c.call("EXISTS", "gone"), 0)
        self.assertEqual(c.call("DBSIZE"), 3)

    def test_ttl_survives_restart(self):
        server = self.server()
        server.start()
        c = self.client(server)
        c.call("SET", "ex", "v", "EX", "100")
        c.call("SET", "exp", "v")
        c.call("EXPIRE", "exp", "200")
        c.call("SET", "keep", "v", "EX", "300")
        c.call("SET", "keep", "v2", "KEEPTTL")
        c.call("SET", "plain", "v", "EX", "100")
        c.call("SET", "plain", "v2")  # a plain SET drops the TTL
        server.stop()

        server.start()
        c = self.client(server)
        self.assertIn(c.call("TTL", "ex"), (99, 100))
        self.assertIn(c.call("TTL", "exp"), (199, 200))
        self.assertIn(c.call("TTL", "keep"), (299, 300))
        self.assertEqual(c.call("GET", "keep"), "v2")
        self.assertEqual(c.call("TTL", "plain"), -1)

    def test_key_that_expired_while_stopped_stays_gone(self):
        server = self.server()
        server.start()
        c = self.client(server)
        c.call("SET", "px", "v", "PX", "200")
        c.call("SET", "at", "v")
        c.call("PEXPIREAT", "at", str(int(time.time() * 1000) + 200))
        c.call("SET", "stays", "v")
        server.stop()

        time.sleep(0.4)

        server.start()
        c = self.client(server)
        self.assertIsNone(c.call("GET", "px"))
        self.assertIsNone(c.call("GET", "at"))
        self.assertEqual(c.call("DBSIZE"), 1)


class TestCrash(AOFServerTest):
    def test_acknowledged_writes_survive_sigkill_with_every_policy(self):
        """Replies go out only after the write reached the kernel, so a process
        crash loses nothing that a client saw "OK" for, whatever the policy."""
        for policy in FSYNC_POLICIES:
            with self.subTest(appendfsync=policy):
                self.aof_path.unlink(missing_ok=True)
                server = self.server("--appendfsync", policy)
                server.start()
                c = self.client(server)
                replies = c.pipeline([("SET", f"key:{i}", str(i)) for i in range(1000)])
                self.assertEqual(replies, ["OK"] * 1000)

                server.kill()

                server.start()
                c = self.client(server)
                self.assertEqual(c.call("DBSIZE"), 1000)
                values = c.pipeline([("GET", f"key:{i}") for i in range(1000)])
                self.assertEqual(values, [str(i) for i in range(1000)])
                server.stop()


class TestExpiryIsLogged(AOFServerTest):
    def test_active_expiry_logs_del(self):
        """A key nobody reads still expires, and the AOF records the delete."""
        server = self.server()
        server.start()
        c = self.client(server)
        c.call("SET", "k", "v", "PX", "100")
        self.assertTrue(wait_until(lambda: c.call("DBSIZE") == 0), "key was not expired")
        server.stop()

        self.assertEqual(replay(self.aof_path)[-1], ["DEL", "k"])


class TestBGRewriteAOF(AOFServerTest):
    def test_rewrite_shrinks_file_and_keeps_data(self):
        server = self.server()
        server.start()
        c = self.client(server)
        c.pipeline([("INCR", "counter")] * 10_000)
        c.call("SET", "a", "1")
        c.call("HSET", "h", "f", "v")
        c.call("SET", "t", "v", "PXAT", str(FAR_FUTURE_MS))
        c.call("SET", "gone", "x")
        c.call("DEL", "gone")

        self.assertEqual(c.call("BGREWRITEAOF"), "Background append only file rewriting started")

        rewritten = (
            cmd_bytes("SET", "counter", "10000")
            + cmd_bytes("SET", "a", "1")
            + cmd_bytes("HSET", "h", "f", "v")
            + cmd_bytes("SET", "t", "v")
            + cmd_bytes("PEXPIREAT", "t", str(FAR_FUTURE_MS))
        )
        self.assertTrue(
            wait_until(lambda: self.aof_path.read_bytes() == rewritten),
            f"AOF was not rewritten, it holds {self.aof_path.stat().st_size} bytes",
        )
        self.assertEqual(list(self.dir.glob("temp-*")), [])

        # New writes go to the new file.
        c.call("SET", "after", "1")
        server.stop()

        server.start()
        c = self.client(server)
        self.assertEqual(c.call("GET", "counter"), "10000")
        self.assertEqual(c.call("GET", "a"), "1")
        self.assertEqual(c.call("HGET", "h", "f"), "v")
        self.assertGreater(c.call("TTL", "t"), 0)
        self.assertEqual(c.call("GET", "after"), "1")
        self.assertEqual(c.call("DBSIZE"), 5)

    def test_rewrite_without_aof_is_an_error(self):
        server = self.server(aof=False)
        server.start()
        c = self.client(server)
        with self.assertRaisesRegex(RuntimeError, "^ERR AOF is not enabled$"):
            c.call("BGREWRITEAOF")

    def test_second_rewrite_while_one_runs_is_an_error(self):
        server = self.server()
        server.start()
        c = self.client(server)
        c.call("SET", "a", "1")

        # One send: the server runs both in the same loop turn, before cron can
        # reap the first child, so the second always sees it running.
        c.sock.sendall(cmd_bytes("BGREWRITEAOF") * 2)

        self.assertEqual(c._read_reply(), "Background append only file rewriting started")
        with self.assertRaisesRegex(RuntimeError, "^ERR Background append only file rewriting already in progress$"):
            c._read_reply()
        self.assertTrue(wait_until(lambda: self.aof_path.read_bytes() == cmd_bytes("SET", "a", "1")))
        self.assertTrue(wait_until(lambda: child_pids(server.proc.pid) == []), "rewrite child was not reaped")

    def test_sigterm_during_rewrite(self):
        """Stopping mid-rewrite exits cleanly, leaves no child or temp file,
        and keeps all the data."""
        keys = 100_000
        server = self.server()
        server.start()
        c = self.client(server)
        for start in range(0, keys, 10_000):
            c.pipeline([("SET", f"key:{i}", "v") for i in range(start, start + 10_000)])

        c.call("BGREWRITEAOF")
        children = child_pids(server.proc.pid)
        self.assertEqual(len(children), 1, "expected one rewrite child")

        self.assertEqual(server.stop(), 0)

        self.assertTrue(wait_until(lambda: not pid_exists(children[0]), timeout=2),
                        "rewrite child outlived the server")
        self.assertEqual(list(self.dir.glob("temp-*")), [])

        server.start()
        c = self.client(server)
        self.assertEqual(c.call("DBSIZE"), keys)


if __name__ == "__main__":
    unittest.main(verbosity=2)
